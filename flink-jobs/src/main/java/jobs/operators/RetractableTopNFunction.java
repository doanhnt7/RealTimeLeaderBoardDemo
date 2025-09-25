package jobs.operators;

import jobs.models.Score;
import jobs.models.ScoreChangeEvent;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import java.time.Duration;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

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
    private final long ttlMinutes;
    
    // State to store mapping from score value to list of users with that score
    private transient MapState<Double, List<Score>> dataState;
    
    // State to store sorted map of score -> count for efficient ranking  
    private transient ValueState<SortedMap<Double, Long>> treeMap;
    
    // State to track last cleanup time
    private transient ValueState<Long> lastCleanupTime;
    
    // TTL configuration
    private static final long CLEANUP_INTERVAL_MS = 5 * 60 * 1000L; // 5 minutes
    
    // Comparator for scores (descending order - higher scores first)
    private static final Comparator<Double> SCORE_COMPARATOR = (s1, s2) -> Double.compare(s2, s1);
    
    // Helper methods for rank checking
    private boolean isInRankEnd(long rank) {
        return rank <= topN;
    }
    
    private boolean isInRankRange(long rank) {
        return rank >= 1 && rank <= topN;
    }
    
    public RetractableTopNFunction(int topN, long ttlMinutes) {
        this.topN = topN;
        this.ttlMinutes = ttlMinutes;
    }
    
    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        
        // Configure TTL
        StateTtlConfig ttlConfig = StateTtlConfig
            .newBuilder(Duration.ofMinutes(ttlMinutes))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build();
        
        // Initialize data state (score -> list of Score objects with that score)
        ListTypeInfo<Score> scoreListTypeInfo = new ListTypeInfo<>(Score.class);
        MapStateDescriptor<Double, List<Score>> dataDescriptor = 
            new MapStateDescriptor<>("data-state", Types.DOUBLE, scoreListTypeInfo);
        dataDescriptor.enableTimeToLive(ttlConfig);
        dataState = getRuntimeContext().getMapState(dataDescriptor);
        
        // Initialize sorted map state (score -> count)
        ValueStateDescriptor<SortedMap<Double, Long>> treeMapDescriptor = 
            new ValueStateDescriptor<>("tree-map", 
                TypeInformation.of(new TypeHint<SortedMap<Double, Long>>() {}));
        treeMapDescriptor.enableTimeToLive(ttlConfig);
        treeMap = getRuntimeContext().getState(treeMapDescriptor);
        
        // Initialize cleanup time state
        ValueStateDescriptor<Long> cleanupDescriptor = 
            new ValueStateDescriptor<>("last-cleanup", Types.LONG);
        lastCleanupTime = getRuntimeContext().getState(cleanupDescriptor);
    }
    
    @Override
    public void processElement(
            Score input,
            KeyedProcessFunction<String, Score, ScoreChangeEvent>.Context ctx,
            Collector<ScoreChangeEvent> out) throws Exception {
        long currentTime = ctx.timestamp();
        performPeriodicCleanup(currentTime);

        SortedMap<Double, Long> currentTreeMap = treeMap.value();
        if (currentTreeMap == null) {
            currentTreeMap = new TreeMap<>(SCORE_COMPARATOR);
        }

        // Process retraction first if previousScore exists
        if (input.getPreviousScore() != 0.0) {
            Double prevSortKey = input.getPreviousScore();
            
            // Find the actual previous record by userId in the previousScore bucket
            Score actualPrevRecord = findAndRemoveUserFromBucket(input.getId(), prevSortKey, currentTreeMap);
            
            // Emit retraction records without row number using the actual previous record
            if (actualPrevRecord != null) {
                retractRecordWithoutRowNumber(currentTreeMap, prevSortKey, actualPrevRecord, out);
            }
        }

        // Process accumulation for current score
        Double sortKey = input.getScore();
        
        // Update sortedMap for accumulation
        if (currentTreeMap.containsKey(sortKey)) {
            currentTreeMap.put(sortKey, currentTreeMap.get(sortKey) + 1);
        } else {
            currentTreeMap.put(sortKey, 1L);
        }

        // Emit accumulation records without row number
        emitRecordsWithoutRowNumber(currentTreeMap, sortKey, input, out);
        
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
     * Find user in specific score bucket and remove, return the actual record that was removed
     */
    private Score findAndRemoveUserFromBucket(String userId, Double scoreValue, SortedMap<Double, Long> currentTreeMap) throws Exception {
        List<Score> scoresAtThisValue = dataState.get(scoreValue);
        if (scoresAtThisValue != null) {
            Iterator<Score> iter = scoresAtThisValue.iterator();
            while (iter.hasNext()) {
                Score score = iter.next();
                if (score.getId().equals(userId)) {
                    // Found the actual previous record, remove it
                    iter.remove();
                    
                    // Update tree map count
                    Long count = currentTreeMap.get(scoreValue);
                    if (count != null) {
                        if (count <= 1) {
                            currentTreeMap.remove(scoreValue);
                        } else {
                            currentTreeMap.put(scoreValue, count - 1);
                        }
                    }
                    
                    // Update data state
                    if (scoresAtThisValue.isEmpty()) {
                        dataState.remove(scoreValue);
                    } else {
                        dataState.put(scoreValue, scoresAtThisValue);
                    }
                    
                    return score; // Return the actual record with correct timestamp
                }
            }
        }
        return null; // User not found in this bucket
    }
    
    /**
     * Emit records without row number - adapted from Flink's emitRecordsWithoutRowNumber
     */
    private void emitRecordsWithoutRowNumber(
            SortedMap<Double, Long> sortedMap,
            Double sortKey,
            Score inputRow,
            Collector<ScoreChangeEvent> out) throws Exception {
        
        Iterator<Map.Entry<Double, Long>> iterator = sortedMap.entrySet().iterator();
        long curRank = 0L;
        boolean findsSortKey = false;
        Score toCollect = null;
        Score toDelete = null;
        
        while (iterator.hasNext() && isInRankEnd(curRank)) {
            Map.Entry<Double, Long> entry = iterator.next();
            Double key = entry.getKey();
            
            if (!findsSortKey && key.equals(sortKey)) {
                curRank += entry.getValue();
                if (isInRankRange(curRank)) {
                    toCollect = inputRow;
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
                }
            } else {
                curRank += entry.getValue();
            }
        }
        
        if (toDelete != null) {
            out.collect(new ScoreChangeEvent(ScoreChangeEvent.ChangeType.DELETE, toDelete, -1));
        }
        if (toCollect != null) {
            out.collect(new ScoreChangeEvent(ScoreChangeEvent.ChangeType.INSERT, inputRow, -1));
        }
    }
    
    /**
     * Retract record without row number - adapted from Flink's retractRecordWithoutRowNumber
     */
    private void retractRecordWithoutRowNumber(
            SortedMap<Double, Long> sortedMap,
            Double sortKey,
            Score inputRow,
            Collector<ScoreChangeEvent> out) throws Exception {
        
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
                        if (!findsSortKey && prevRow.getId().equals(inputRow.getId())) {
                            if (isInRankRange(nextRank)) {
                                out.collect(new ScoreChangeEvent(ScoreChangeEvent.ChangeType.DELETE, prevRow, -1));
                            }
                            findsSortKey = true;
                        } else if (findsSortKey) {
                            if (nextRank == topN) {
                                out.collect(new ScoreChangeEvent(ScoreChangeEvent.ChangeType.INSERT, prevRow, -1));
                            }
                        }
                        nextRank += 1;
                    }
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
                        out.collect(new ScoreChangeEvent(ScoreChangeEvent.ChangeType.INSERT, toAdd, -1));
                        break;
                    }
                }
            } else {
                nextRank += entry.getValue();
            }
        }
    }
    

    
    /**
     * Performs periodic cleanup of expired entries
     */
    private void performPeriodicCleanup(long currentTime) throws Exception {
        Long lastCleanup = lastCleanupTime.value();
        
        if (lastCleanup == null || (currentTime - lastCleanup) > CLEANUP_INTERVAL_MS) {
            cleanupExpiredEntries(currentTime);
            lastCleanupTime.update(currentTime);
        }
    }
    
    /**
     * Removes entries that are older than TTL
     */
    private void cleanupExpiredEntries(long currentTime) throws Exception {
        long expirationTime = currentTime - (ttlMinutes * 60 * 1000L);
        List<Double> scoresToRemove = new ArrayList<>();
        
        // Find expired entries
        for (Double scoreValue : dataState.keys()) {
            List<Score> scoresAtThisValue = dataState.get(scoreValue);
            if (scoresAtThisValue != null) {
                scoresAtThisValue.removeIf(score -> score.getLastUpdateTime() < expirationTime);
                
                if (scoresAtThisValue.isEmpty()) {
                    scoresToRemove.add(scoreValue);
                } else {
                    dataState.put(scoreValue, scoresAtThisValue);
                }
            }
        }
        
        // Remove empty score buckets and update tree map
        SortedMap<Double, Long> currentTreeMap = treeMap.value();
        if (currentTreeMap != null) {
            for (Double scoreValue : scoresToRemove) {
                dataState.remove(scoreValue);
                currentTreeMap.remove(scoreValue);
            }
            treeMap.update(currentTreeMap);
        }
        
        if (!scoresToRemove.isEmpty()) {
            LOG.debug("Cleaned up {} expired score buckets", scoresToRemove.size());
        }
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        LOG.info("RetractableTopNFunction closed for topN: {}, ttl: {} minutes", topN, ttlMinutes);
    }
}
