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
import java.util.HashSet;
import java.util.Set;

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

        // Build membership sets before mutation (only ids, no rank sensitivity)
        Set<String> beforeTopNIds = buildTopNIdSet(currentTreeMap);

        // Retract using previousScore carried by upstream if present
        Double prev = input.getPreviousScore();
        if (prev != null) {
            removeUserFromBucket(input.getId(), prev, currentTreeMap);
        }

        // Accumulate new score
        addScore(input, currentTreeMap);

        // Build membership sets after mutation
        Set<String> afterTopNIds = buildTopNIdSet(currentTreeMap);

        // Emit minimal changes without row number: set difference on membership
        emitMembershipChanges(beforeTopNIds, afterTopNIds, out, currentTreeMap);

        // Persist updated map
        treeMap.update(currentTreeMap);
    }
    
    /**
     * Find and remove previous score for a user, updating the sorted map accordingly
     */
    private Score findAndRemovePreviousScore(String userId, SortedMap<Double, Long> currentTreeMap) throws Exception {
        // Search through all score buckets to find the user's previous score
        for (Double scoreValue : dataState.keys()) {
            List<Score> scoresAtThisValue = dataState.get(scoreValue);
            if (scoresAtThisValue != null) {
                Iterator<Score> iter = scoresAtThisValue.iterator();
                while (iter.hasNext()) {
                    Score score = iter.next();
                    if (score.getId().equals(userId)) {
                        // Found the user's previous score, remove it
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
                        
                        return score;
                    }
                }
            }
        }
        return null; // User didn't have a previous score
    }
    
    /**
     * Add a new score, updating both data state and sorted map
     */
    private void addScore(Score input, SortedMap<Double, Long> currentTreeMap) throws Exception {
        Double scoreValue = input.getScore();
        
        // Update tree map count
        Long count = currentTreeMap.get(scoreValue);
        currentTreeMap.put(scoreValue, (count == null) ? 1L : count + 1);
        
        // Update data state
        List<Score> scoresAtThisValue = dataState.get(scoreValue);
        if (scoresAtThisValue == null) {
            scoresAtThisValue = new ArrayList<>();
        }
        scoresAtThisValue.add(input);
        dataState.put(scoreValue, scoresAtThisValue);
    }
    
    /**
     * Emit change events by comparing previous and new top N lists
     */
    private void emitMembershipChanges(Set<String> beforeIds,
                                       Set<String> afterIds,
                                       Collector<ScoreChangeEvent> out,
                                       SortedMap<Double, Long> currentTreeMap) throws Exception {
        // Deletions: in before but not in after
        for (String id : beforeIds) {
            if (!afterIds.contains(id)) {
                Score s = findScoreByIdInTopN(id, currentTreeMap);
                // If not present anymore, we can emit with a minimal placeholder (id, score 0)
                if (s == null) {
                    s = new Score(id, 0.0, (Double) null, 0L);
                }
                out.collect(new ScoreChangeEvent(ScoreChangeEvent.ChangeType.DELETE, s, -1));
            }
        }
        // Insertions: in after but not in before
        for (String id : afterIds) {
            if (!beforeIds.contains(id)) {
                Score s = findScoreByIdInTopN(id, currentTreeMap);
                if (s != null) {
                    out.collect(new ScoreChangeEvent(ScoreChangeEvent.ChangeType.INSERT, s, -1));
                }
            }
        }
    }
    
    /**
     * Build top N list from the sorted map structure
     */
    private List<Score> buildTopNFromTreeMap(SortedMap<Double, Long> currentTreeMap) throws Exception {
        List<Score> topNList = new ArrayList<>();
        int currentRank = 0;
        
        for (Map.Entry<Double, Long> entry : currentTreeMap.entrySet()) {
            if (currentRank >= topN) {
                break;
            }
            
            Double scoreValue = entry.getKey();
            List<Score> scoresAtThisValue = dataState.get(scoreValue);
            
            if (scoresAtThisValue != null) {
                // Sort by timestamp descending for tie-breaking
                scoresAtThisValue.sort((s1, s2) -> 
                    Long.compare(s2.getLastUpdateTime(), s1.getLastUpdateTime()));
                
                for (Score score : scoresAtThisValue) {
                    if (currentRank >= topN) {
                        break;
                    }
                    topNList.add(score);
                    currentRank++;
                }
            }
        }
        
        return topNList;
    }

    private Set<String> buildTopNIdSet(SortedMap<Double, Long> currentTreeMap) throws Exception {
        List<Score> list = buildTopNFromTreeMap(currentTreeMap);
        Set<String> ids = new HashSet<>();
        for (Score s : list) ids.add(s.getId());
        return ids;
    }

    private void removeUserFromBucket(String userId, Double prevScoreValue, SortedMap<Double, Long> currentTreeMap) throws Exception {
        List<Score> scoresAtThisValue = dataState.get(prevScoreValue);
        if (scoresAtThisValue != null) {
            Iterator<Score> iter = scoresAtThisValue.iterator();
            boolean removed = false;
            while (iter.hasNext()) {
                Score sc = iter.next();
                if (userId.equals(sc.getId())) {
                    iter.remove();
                    removed = true;
                    break;
                }
            }
            if (removed) {
                Long count = currentTreeMap.get(prevScoreValue);
                if (count != null) {
                    if (count <= 1) currentTreeMap.remove(prevScoreValue);
                    else currentTreeMap.put(prevScoreValue, count - 1);
                }
                if (scoresAtThisValue.isEmpty()) dataState.remove(prevScoreValue);
                else dataState.put(prevScoreValue, scoresAtThisValue);
            }
        }
    }

    private Score findScoreByIdInTopN(String userId, SortedMap<Double, Long> currentTreeMap) throws Exception {
        int currentRank = 0;
        for (Map.Entry<Double, Long> entry : currentTreeMap.entrySet()) {
            if (currentRank >= topN) break;
            Double scoreValue = entry.getKey();
            List<Score> scoresAtThisValue = dataState.get(scoreValue);
            if (scoresAtThisValue != null) {
                scoresAtThisValue.sort((s1, s2) -> Long.compare(s2.getLastUpdateTime(), s1.getLastUpdateTime()));
                for (Score s : scoresAtThisValue) {
                    if (currentRank >= topN) break;
                    if (userId.equals(s.getId())) return s;
                    currentRank++;
                }
            }
        }
        return null;
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
