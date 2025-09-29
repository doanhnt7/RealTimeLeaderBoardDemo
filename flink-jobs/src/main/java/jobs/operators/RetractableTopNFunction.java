package jobs.operators;

import jobs.models.Score;
import jobs.models.ScoreChangeEvent;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.state.StateTtlConfig;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.flink.table.runtime.typeutils.SortedMapTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import java.util.Collections;
/**
 * A retractable TopN function similar to Flink's table runtime version but adapted for Score objects.
 * 
 * This function processes Score objects and maintains top N scores with proper retraction logic.
 * It handles updates to existing scores and maintains consistent ranking.
 * 
 * Features:
 * - Maintains top N scores using sorted map structure
 * - Handles score updates and retractions properly
 * - TTL-based cleanup to prevent memory leaks
 * - Emits complete top N list when changes occur
 */
public class RetractableTopNFunction extends KeyedProcessFunction<String, Score, ScoreChangeEvent> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RetractableTopNFunction.class);
    
    private final int topN;
    private final long cleanupIntervalMs;
    private final long ttlMinutes;
    
    // State to store mapping from score value to list of users with that score
    private transient MapState<Double, List<Score>> dataState;
    
    // State to store sorted map of score -> count for efficient ranking  
    private transient ValueState<SortedMap<Double, Long>> treeMap;
    
    // State to track if cleanup timer is registered
    private transient ValueState<Boolean> timerRegistered;
   
    class RetractContext { Score insertFromRetract; Score deleteFromRetract; }

    // Comparator for scores (descending order - higher scores first)
    private static final Comparator<Double> SCORE_COMPARATOR = Collections.reverseOrder();
    
    // Helper methods for rank checking
    private boolean isInRankEnd(long rank) {
        return rank <= topN;
    }
    
    
    public RetractableTopNFunction(int topN, long ttlMinutes, long cleanupIntervalMinutes) {
        this.topN = topN;
        this.ttlMinutes = ttlMinutes;
        this.cleanupIntervalMs = cleanupIntervalMinutes * 60 * 1000L;
    }
    
    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        // TTL config for state
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(java.time.Duration.ofMinutes(ttlMinutes))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();

        // Initialize data state (score -> list of Score objects with that score) with TTL
        ListTypeInfo<Score> scoreListTypeInfo = new ListTypeInfo<>(Score.class);
        MapStateDescriptor<Double, List<Score>> dataDescriptor = 
            new MapStateDescriptor<>("data-state", Types.DOUBLE, scoreListTypeInfo);
        dataDescriptor.enableTimeToLive(ttlConfig);
        dataState = getRuntimeContext().getMapState(dataDescriptor);

        // Initialize sorted map state (score -> count) with TTL
        ValueStateDescriptor<SortedMap<Double, Long>> treeMapDescriptor = 
            new ValueStateDescriptor<>(
                "tree-map",
            new SortedMapTypeInfo<>(
                BasicTypeInfo.DOUBLE_TYPE_INFO,    // Key type
                BasicTypeInfo.LONG_TYPE_INFO,      // Value type
                SCORE_COMPARATOR));                // Comparator
        treeMapDescriptor.enableTimeToLive(ttlConfig);
        treeMap = getRuntimeContext().getState(treeMapDescriptor);

        // Initialize timer registration state with TTL
        ValueStateDescriptor<Boolean> timerDescriptor = 
            new ValueStateDescriptor<>("timer-registered", Types.BOOLEAN);
        timerDescriptor.enableTimeToLive(ttlConfig);
        timerRegistered = getRuntimeContext().getState(timerDescriptor);

    }
    
    @Override
    public void processElement(
            Score input,
            KeyedProcessFunction<String, Score, ScoreChangeEvent>.Context ctx,
            Collector<ScoreChangeEvent> out) throws Exception {
        
        // Register cleanup timer if not already registered
        registerCleanupTimer(ctx);

        SortedMap<Double, Long> currentTreeMap = treeMap.value();
        if (currentTreeMap == null) {
            currentTreeMap = new TreeMap<>(SCORE_COMPARATOR);
        }
        RetractContext retractContext = new RetractContext();
        
        Double sortKey = input.getScore();
        Double prevSortKey = input.getPreviousScore();
        // Process retraction first if previousScore exists
        if (prevSortKey != 0.0) {
            if(sortKey.equals(prevSortKey)){
                // Update dataState for accumulation
                List<Score> inputs = dataState.get(sortKey);
                if (inputs == null) {
                    inputs = new ArrayList<>();
                }
                inputs.add(input);
                dataState.put(sortKey, inputs);
                return;
            }
            // Emit retraction (this will handle finding and removing from dataState internally)
            boolean stateRemoved = retractRecordWithoutRowNumber(currentTreeMap, prevSortKey, input.getId(), out, retractContext);
            
           
            // If retractRecord didn't remove from dataState, do manual removal
            if (!stateRemoved) {
                stateRemoved = removeUserFromDataState(input.getId(), prevSortKey);
            }
            if (stateRemoved) { // if stateRemoved is true, update sortedMap
                 // Update sortedMap after emitting
                if (currentTreeMap.containsKey(prevSortKey)) {
                    long count = currentTreeMap.get(prevSortKey) - 1;
                    if (count == 0) {
                        currentTreeMap.remove(prevSortKey);
                    } else {
                        currentTreeMap.put(prevSortKey, count);
                    }
                } else {
                    // Handle stale state - log warning but continue
                    LOG.warn("Previous score {} not found in treeMap for user {}", prevSortKey, input.getId());
                }
            
            }
        }

        // Update sortedMap for accumulation
        if (currentTreeMap.containsKey(sortKey)) {
            currentTreeMap.put(sortKey, currentTreeMap.get(sortKey) + 1);
        } else {
            currentTreeMap.put(sortKey, 1L);
        }

        // Emit accumulation records without row number
        emitRecordsWithoutRowNumber(currentTreeMap, sortKey, input, out, retractContext);
        
        // Update dataState for accumulation
        List<Score> inputs = dataState.get(sortKey);
        if (inputs == null) {
            inputs = new ArrayList<>();
        }
        inputs.add(input);
        dataState.put(sortKey, inputs);

        // Update the tree map state
        treeMap.update(currentTreeMap);
    }
    
    
    /**
     * Remove user from dataState only (called after emit)
     */
    private boolean removeUserFromDataState(String userId, Double scoreValue) throws Exception {
        boolean stateRemoved = false;
        List<Score> scoresAtThisValue = dataState.get(scoreValue);
        if (scoresAtThisValue != null) {
            Iterator<Score> iter = scoresAtThisValue.iterator();
            while (iter.hasNext()) {
                Score score = iter.next();
                if (score.getId().equals(userId)) {
                    iter.remove();
                    stateRemoved = true;
                    break;
                }
            }
            
            // Update data state
            if (scoresAtThisValue.isEmpty()) {
                dataState.remove(scoreValue);
            } else {
                dataState.put(scoreValue, scoresAtThisValue);
            }
        }
        return stateRemoved;
    }
    
    /**
     * Emit records without row number - adapted from Flink's emitRecordsWithoutRowNumber
     */
    private void emitRecordsWithoutRowNumber(
            SortedMap<Double, Long> sortedMap,
            Double sortKey,
            Score inputRow,
            Collector<ScoreChangeEvent> out,
            RetractContext retractContext) throws Exception {
        
        Iterator<Map.Entry<Double, Long>> iterator = sortedMap.entrySet().iterator();
        long curRank = 0L;
        boolean findsSortKey = false;
        Score toInsert = null;
        Score toDelete = null;
        
        while (iterator.hasNext() && isInRankEnd(curRank)) {
            Map.Entry<Double, Long> entry = iterator.next();
            Double key = entry.getKey();
            
            if (!findsSortKey && key.equals(sortKey)) {
                curRank += entry.getValue();
                if (isInRankEnd(curRank)) {
                    toInsert = inputRow;
                }
                findsSortKey = true;
            } else if (findsSortKey) {
                List<Score> inputs = dataState.get(key);
                if (inputs != null) {
                    long count = entry.getValue();
                    // gets the rank of last record with same sortKey
                    long rankOfLastRecord = curRank + count;
                    // deletes the record if there is a record recently downgrades to Top-(N+1)
                    if (isInRankEnd(rankOfLastRecord)) {
                        curRank = rankOfLastRecord;
                    } else {
                        int index = (int)(topN - curRank);
                        if (index < inputs.size()) {
                            toDelete = inputs.get(index);
                        }
                        break;
                    }
                } else {
                    // Stale state: treeMap has key but dataState doesn't
                    processStateStaled(sortedMap, key);
                }
            } else {
                curRank += entry.getValue();
            }
        }

        if(toInsert == null) {
            out.collect(new ScoreChangeEvent(ScoreChangeEvent.ChangeType.DELETE, retractContext.deleteFromRetract));
            out.collect(new ScoreChangeEvent(ScoreChangeEvent.ChangeType.INSERT, retractContext.insertFromRetract));
           
        }
        else {
            if(retractContext.deleteFromRetract == null) {
                out.collect(new ScoreChangeEvent(ScoreChangeEvent.ChangeType.DELETE, toDelete));
            }
            out.collect(new ScoreChangeEvent(ScoreChangeEvent.ChangeType.INSERT, inputRow));
        }
    }
    
    /**
     * Retract record without row number - adapted from Flink's retractRecordWithoutRowNumber
     * @return true if the record was found and removed from dataState
     */
    private boolean retractRecordWithoutRowNumber(
            SortedMap<Double, Long> sortedMap,
            Double sortKey,
            String id,
            Collector<ScoreChangeEvent> out,
            RetractContext retractContext) throws Exception {
        
        Iterator<Map.Entry<Double, Long>> iterator = sortedMap.entrySet().iterator();
        long nextRank = 1L; // the next rank number, should be in the rank range
        boolean findsSortKey = false;
        
        while (iterator.hasNext() && isInRankEnd(nextRank)) {
            Map.Entry<Double, Long> entry = iterator.next();
            Double key = entry.getKey();
            
            if (!findsSortKey && key.equals(sortKey)) {
                List<Score> inputs = dataState.get(key);
                if (inputs != null) {
                    Iterator<Score> inputIter = inputs.iterator();
                    while (inputIter.hasNext() && isInRankEnd(nextRank)) {
                        Score prevRow = inputIter.next();
                        if (!findsSortKey && prevRow.getId().equals(id)) {
                            // Found the record to retract
                            if (isInRankEnd(nextRank)) {
                                retractContext.deleteFromRetract = prevRow;
                            }
                            nextRank -= 1;
                            findsSortKey = true;
                            // Remove from dataState
                            inputIter.remove();
                           
                        } else if (findsSortKey) {
                            if (nextRank == topN) {
                                retractContext.insertFromRetract = prevRow;
                            }
                        }
                        nextRank += 1;
                    }
                    
                    // Update dataState after removal
                    if (findsSortKey) {
                        if (inputs.isEmpty()) {
                            dataState.remove(key);
                        } else {
                            dataState.put(key, inputs);
                        }
                    }
                } else {
                    // Stale state: treeMap has key but dataState doesn't
                    processStateStaled(sortedMap, key);
                }
            } else if (findsSortKey) {
                long count = entry.getValue();
                // gets the rank of last record with same sortKey
                long rankOfLastRecord = nextRank + count - 1;
                if (rankOfLastRecord < topN) {
                    nextRank = rankOfLastRecord + 1;
                } else {
                    // sends the record if there is a record recently upgrades to Top-N
                    int index = (int)(topN - nextRank);
                    List<Score> inputs = dataState.get(key);
                    if (inputs != null && index < inputs.size()) {
                        Score toAdd = inputs.get(index);
                        retractContext.insertFromRetract = toAdd;
                        break;
                    } else if (inputs == null) {
                        // Stale state: treeMap has key but dataState doesn't
                        processStateStaled(sortedMap, key);
        
                    }
                }
            } else {
                nextRank += entry.getValue();
            }
        }
        
        return findsSortKey;
    }
    

    
    /**
     * Register cleanup timer if not already registered
     */
    private void registerCleanupTimer(KeyedProcessFunction<String, Score, ScoreChangeEvent>.Context ctx) throws Exception {
        Boolean registered = timerRegistered.value();
        
        if (registered == null || !registered) {
            // First time - register timer for next cleanup
            long currentTime = ctx.timestamp();
            long nextCleanupTime = currentTime + cleanupIntervalMs;
            ctx.timerService().registerEventTimeTimer(nextCleanupTime);
            timerRegistered.update(true);
        }
    }
    
    /**
     * Process stale state when dataState key is expired but treeMap key still exists
     * This can happen when TTL expires dataState entries but treeMap hasn't been cleaned up yet
     */
    private void processStateStaled(SortedMap<Double, Long> currentTreeMap, Double scoreKey) throws Exception {
        LOG.warn("Detected stale state: scoreKey {} exists in treeMap but not in dataState, cleaning up", scoreKey);
        
        // Remove the stale key from treeMap since corresponding dataState is gone
        currentTreeMap.remove(scoreKey);
        
        LOG.debug("Removed stale scoreKey {} from treeMap", scoreKey);
    }
    
    
    
    /**
     * Removes entries that are older than TTL and emits appropriate change events
     */
    private void cleanupExpiredEntries(long currentTime, Collector<ScoreChangeEvent> out) throws Exception {
        long expirationTime = currentTime - (ttlMinutes * 60 * 1000L);
        SortedMap<Double, Long> currentTreeMap = treeMap.value();
        if (currentTreeMap == null) {
            currentTreeMap = new TreeMap<>(SCORE_COMPARATOR);
        }
        
        // Collect expired records to retract
        List<Score> expiredRecords = new ArrayList<>();
       

        // Find expired entries
        for (Double scoreValue : dataState.keys()) {
            List<Score> scoresAtThisValue = dataState.get(scoreValue);
            if (scoresAtThisValue != null) {
                // Collect expired records before removing
                for (Score score : scoresAtThisValue) {
                    if (score.getLastUpdateTime() < expirationTime) {
                        expiredRecords.add(score);
                    }
                }
            }
        }
        
        // Process each expired record using same logic as processElement
        for (Score expiredRecord : expiredRecords) {
            Double scoreValue = expiredRecord.getScore();
            String userId = expiredRecord.getId();
            RetractContext retractContext = new RetractContext();
            // Try retract from Top-N (emit events if needed)
            boolean stateRemoved = retractRecordWithoutRowNumber(currentTreeMap, scoreValue, userId, out, retractContext);
            // If retractRecord didn't remove from dataState, do manual removal
            if (!stateRemoved) {
                stateRemoved = removeUserFromDataState(userId, scoreValue);
            }else if(retractContext.insertFromRetract != null){
                out.collect(new ScoreChangeEvent(ScoreChangeEvent.ChangeType.DELETE, retractContext.deleteFromRetract));
                out.collect(new ScoreChangeEvent(ScoreChangeEvent.ChangeType.INSERT, retractContext.insertFromRetract));
            }
            
            // If stateRemoved is true, update treeMap
            if (stateRemoved) {
                if (currentTreeMap.containsKey(scoreValue)) {
                    long count = currentTreeMap.get(scoreValue) - 1;
                    if (count == 0) {
                        currentTreeMap.remove(scoreValue);
                    } else {
                        currentTreeMap.put(scoreValue, count);
                    }
                } else {
                    // Handle stale state - log warning but continue
                    LOG.warn("Expired score {} not found in treeMap for user {}", scoreValue, userId);
                }
            }
        }
        
        // Update treeMap state
        treeMap.update(currentTreeMap);
        
        if (!expiredRecords.isEmpty()) {
            LOG.info("Cleaned up {} expired records, processed retractions", expiredRecords.size());
        }
    }
    
    @Override
    public void onTimer(long timestamp, 
                       KeyedProcessFunction<String, Score, ScoreChangeEvent>.OnTimerContext ctx, 
                       Collector<ScoreChangeEvent> out) throws Exception {
        
        // Perform cleanup and emit change events for expired records
        cleanupExpiredEntries(timestamp, out);
        
        // Register next cleanup timer
        long nextCleanupTime = timestamp + cleanupIntervalMs;
        ctx.timerService().registerEventTimeTimer(nextCleanupTime);
    
        
        LOG.debug("Cleanup timer fired at timestamp: {}, next cleanup at: {}", timestamp, nextCleanupTime);
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        LOG.info("RetractableTopNFunction closed for topN: {}, ttl: {} minutes", topN, ttlMinutes);
    }
}
