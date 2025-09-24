package jobs.operators;

import jobs.models.Score;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.functions.OpenContext;
import jobs.models.TopNDelta;
import org.apache.flink.util.Collector;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

public class TopNScoresInLastXMinutesFunction extends KeyedProcessFunction<String, Score, TopNDelta> {
    
    private final int topN;
    private final long ttlMinutes;
    
    private transient ValueState<Map<String, Score>> scores;
    private transient ValueState<byte[]> lastTopNHash;
    // Keep previous Top-N snapshot for delta updates to Redis
    private transient ValueState<Map<String, Double>> previousTopN;
    
    public TopNScoresInLastXMinutesFunction(int topN, long ttlMinutes) {
        this.topN = topN;
        this.ttlMinutes = ttlMinutes;
    }
    
    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        
        // Initialize ValueState for scores set-like container (by id)
        ValueStateDescriptor<Map<String, Score>> scoresDescriptor = new ValueStateDescriptor<>(
            "scores",
            TypeInformation.of(new TypeHint<Map<String, Score>>() {})
        );
        scores = getRuntimeContext().getState(scoresDescriptor);
        
        // Initialize ValueState for tracking top N changes
        ValueStateDescriptor<byte[]> lastTopNHashDescriptor = new ValueStateDescriptor<>(
            "last-topn-hash",
            TypeInformation.of(byte[].class)
        );
        lastTopNHash = getRuntimeContext().getState(lastTopNHashDescriptor);
        
        // Initialize ValueState for previous Top-N snapshot (Id -> score)
        ValueStateDescriptor<Map<String, Double>> prevTopDesc = new ValueStateDescriptor<>(
            "prev-topn",
            TypeInformation.of(new TypeHint<Map<String, Double>>() {})
        );
        previousTopN = getRuntimeContext().getState(prevTopDesc);
        
        // No external resources in processor
    }
    
    @Override
    public void processElement(Score event, Context ctx, Collector<TopNDelta> out) throws Exception {
        long eventTime = event.getLastUpdateTime();
        String id = event.getId();
                
        // Update score
        Map<String, Score> map = scores.value();
        if (map == null) map = new HashMap<>();
        Score currentScore = map.get(id);
        if (currentScore == null) {
            currentScore = new Score(id, event.getScore(), eventTime);
        } else {
            currentScore.updateScore(event.getScore(), eventTime);
        }
        map.put(id, currentScore);
        scores.update(map);
        
        // Get current top N (TTL cleanup handled by Flink)
        List<Score> topNList = getTopN();

        // Fast hash check to skip work when unchanged
        byte[] currentHash = generateTopNHash(topNList);
        byte[] prevHash = lastTopNHash.value();
        if (prevHash != null && Arrays.equals(currentHash, prevHash)) {
            return;
        }

        // Build current Top-N map (Id -> score)
        Map<String, Double> currentTopMap = new HashMap<>();
        for (Score s : topNList) {
            currentTopMap.put(s.getId(), s.getScore());
        }

        // Load previous Top-N map from state
        Map<String, Double> prevTopMap = previousTopN.value();
        if (prevTopMap == null) prevTopMap = Collections.emptyMap();

        // Compute delta: removals and upserts (new or score-changed)
        List<String> toRemove = new ArrayList<>();
        for (String prevId : prevTopMap.keySet()) {
            if (!currentTopMap.containsKey(prevId)) {
                toRemove.add(prevId);
            }
        }

        Map<String, Double> toUpsert = new HashMap<>();
        for (Map.Entry<String, Double> e : currentTopMap.entrySet()) {
            Double prevScore = prevTopMap.get(e.getKey());
            if (prevScore == null || !prevScore.equals(e.getValue())) {
                toUpsert.put(e.getKey(), e.getValue());
            }
        }

        if (!toRemove.isEmpty() || !toUpsert.isEmpty()) {
            // Emit delta for external Redis sink
            out.collect(new TopNDelta( toRemove, toUpsert));

            // Persist current Top-N as previous snapshot and hash
            previousTopN.update(currentTopMap);
            lastTopNHash.update(currentHash);
        }
    }
    
    private byte[] generateTopNHash(List<Score> topNList) {
        StringBuilder sb = new StringBuilder();
        for (Score score : topNList) {
            sb.append(score.getId()).append(":").append(score.getScore()).append("|");
        }
        String rawString = sb.toString();
    
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(rawString.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not supported", e);
        }
    }
    
    private List<Score> getTopN() throws Exception {
        List<Score> allScores = new ArrayList<>();
        long now = System.currentTimeMillis();
        long minAllowedTime = now - ttlMinutes;
        Map<String, Score> map = scores.value();
        if (map == null) map = new HashMap<>();
        // Evict expired scores from the set and build list
        Iterator<Map.Entry<String, Score>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Score> e = it.next();
            Score sc = e.getValue();
            if (sc.getLastUpdateTime() < minAllowedTime) {
                it.remove();
            } else {
                allScores.add(sc);
            }
        }
        // Persist the cleaned set
        scores.update(map);
        
        // Sort by score descending, then by last update time descending
        allScores.sort((a, b) -> {
            int scoreCompare = Double.compare(b.getScore(), a.getScore());
            if (scoreCompare != 0) return scoreCompare;
            return Long.compare(b.getLastUpdateTime(), a.getLastUpdateTime());
        });
        
        return allScores.subList(0, Math.min(topN, allScores.size()));
    }
    
    
    @Override
    public void close() throws Exception { super.close(); }
    
}
