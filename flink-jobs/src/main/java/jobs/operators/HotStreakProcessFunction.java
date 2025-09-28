package jobs.operators;

import jobs.models.Score;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Process Function for Hot Streak calculation with time-bounded windows.
 * 
 * This function calculates hot streak ratio by comparing short-term (10s) and long-term (60s) 
 * average scores. It maintains state for incremental calculation and handles late data appropriately.
 * 
 * E.g.: For each Score event, calculate the hot streak ratio based on 10s vs 60s average scores.
 */
public class HotStreakProcessFunction extends KeyedProcessFunction<String, Score, Score> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(HotStreakProcessFunction.class);

    private final long shortWindowMs; // 10 seconds
    private final long longWindowMs;  // 60 seconds

    // the state which keeps the last triggering timestamp
    private transient ValueState<Long> lastTriggeringTsState;

    // the state which keeps the total score and count for short window (10s)
    private transient ValueState<Double> shortTotalScoreState;
    private transient ValueState<Integer> shortCountState;

    // the state which keeps the total score and count for long window (60s)
    private transient ValueState<Double> longTotalScoreState;
    private transient ValueState<Integer> longCountState;

    // the state which keeps the safe timestamp to cleanup states
    private transient ValueState<Long> cleanupTsState;

    // the state which keeps all the score data that are not expired for short window
    private transient MapState<Long, List<Score>> shortInputState;

    // the state which keeps all the score data that are not expired for long window
    private transient MapState<Long, List<Score>> longInputState;

    // the state which keeps the previous ratio
    private transient ValueState<Double> previousRatioState;

    // ------------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------------
    private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";
    private transient Counter numLateRecordsDropped;

    public HotStreakProcessFunction(long shortWindowMs, long longWindowMs) {
        this.shortWindowMs = shortWindowMs;
        this.longWindowMs = longWindowMs;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        ValueStateDescriptor<Long> lastTriggeringTsDescriptor =
                new ValueStateDescriptor<>("lastTriggeringTsState", Types.LONG);
        lastTriggeringTsState = getRuntimeContext().getState(lastTriggeringTsDescriptor);

        // Short window states
        ValueStateDescriptor<Double> shortTotalScoreDescriptor =
                new ValueStateDescriptor<>("shortTotalScoreState", Types.DOUBLE);
        shortTotalScoreState = getRuntimeContext().getState(shortTotalScoreDescriptor);

        ValueStateDescriptor<Integer> shortCountDescriptor =
                new ValueStateDescriptor<>("shortCountState", Types.INT);
        shortCountState = getRuntimeContext().getState(shortCountDescriptor);

        // Long window states
        ValueStateDescriptor<Double> longTotalScoreDescriptor =
                new ValueStateDescriptor<>("longTotalScoreState", Types.DOUBLE);
        longTotalScoreState = getRuntimeContext().getState(longTotalScoreDescriptor);

        ValueStateDescriptor<Integer> longCountDescriptor =
                new ValueStateDescriptor<>("longCountState", Types.INT);
        longCountState = getRuntimeContext().getState(longCountDescriptor);

        ValueStateDescriptor<Long> cleanupTsStateDescriptor =
                new ValueStateDescriptor<>("cleanupTsState", Types.LONG);
        this.cleanupTsState = getRuntimeContext().getState(cleanupTsStateDescriptor);

        // Short window input state
        ListTypeInfo<Score> scoreListTypeInfo = new ListTypeInfo<>(Score.class);
        MapStateDescriptor<Long, List<Score>> shortInputStateDesc =
                new MapStateDescriptor<>(
                        "shortInputState", Types.LONG, scoreListTypeInfo);
        shortInputState = getRuntimeContext().getMapState(shortInputStateDesc);

        // Long window input state
        MapStateDescriptor<Long, List<Score>> longInputStateDesc =
                new MapStateDescriptor<>(
                        "longInputState", Types.LONG, scoreListTypeInfo);
        longInputState = getRuntimeContext().getMapState(longInputStateDesc);

        // Previous ratio state
        ValueStateDescriptor<Double> previousRatioDescriptor =
                new ValueStateDescriptor<>("previousRatioState", Types.DOUBLE);
        previousRatioState = getRuntimeContext().getState(previousRatioDescriptor);

        // metrics
        this.numLateRecordsDropped =
                getRuntimeContext().getMetricGroup().counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
    }

    @Override
    public void processElement(
            Score input,
            KeyedProcessFunction<String, Score, Score>.Context ctx,
            Collector<Score> out)
            throws Exception {
        // triggering timestamp for trigger calculation
        long triggeringTs = input.getLastUpdateTime();

        Long lastTriggeringTs = lastTriggeringTsState.value();
        if (lastTriggeringTs == null) {
            lastTriggeringTs = 0L;
        }

        // check if the data is expired, if not, save the data and register event time timer
        if (triggeringTs > lastTriggeringTs) {
            // Add to short window state
            List<Score> shortData = shortInputState.get(triggeringTs);
            if (null != shortData) {
                shortData.add(input);
                shortInputState.put(triggeringTs, shortData);
            } else {
                shortData = new ArrayList<>();
                shortData.add(input);
                shortInputState.put(triggeringTs, shortData);
                // register event time timer
                ctx.timerService().registerEventTimeTimer(triggeringTs);
            }

            // Add to long window state
            List<Score> longData = longInputState.get(triggeringTs);
            if (null != longData) {
                longData.add(input);
                longInputState.put(triggeringTs, longData);
            } else {
                longData = new ArrayList<>();
                longData.add(input);
                longInputState.put(triggeringTs, longData);
            }

            registerCleanupTimer(ctx, triggeringTs);
        } else {
            numLateRecordsDropped.inc();
            LOG.debug("Late record dropped for user: {}, timestamp: {}", input.getId(), triggeringTs);
        }
    }

    private void registerCleanupTimer(
            KeyedProcessFunction<String, Score, Score>.Context ctx, long timestamp)
            throws Exception {
        // calculate safe timestamp to cleanup states (use longer window for cleanup)
        long minCleanupTimestamp = timestamp + longWindowMs + 1;
        long maxCleanupTimestamp = timestamp + (long) (longWindowMs * 1.5) + 1;
        // update timestamp and register timer if needed
        Long curCleanupTimestamp = cleanupTsState.value();
        if (curCleanupTimestamp == null || curCleanupTimestamp < minCleanupTimestamp) {
            ctx.timerService().registerEventTimeTimer(maxCleanupTimestamp);
            cleanupTsState.update(maxCleanupTimestamp);
        }
    }

    @Override
    public void onTimer(
            long timestamp,
            KeyedProcessFunction<String, Score, Score>.OnTimerContext ctx,
            Collector<Score> out)
            throws Exception {
        Long cleanupTimestamp = cleanupTsState.value();
        // if cleanupTsState has not been updated then it is safe to cleanup states
        if (cleanupTimestamp != null && cleanupTimestamp <= timestamp) {
            shortInputState.clear();
            longInputState.clear();
            shortTotalScoreState.clear();
            shortCountState.clear();
            longTotalScoreState.clear();
            longCountState.clear();
            lastTriggeringTsState.clear();
            cleanupTsState.clear();
            return;
        }

        // gets all window data from state for the calculation
        List<Score> shortInputs = shortInputState.get(timestamp);
        List<Score> longInputs = longInputState.get(timestamp);

        if (null != shortInputs || null != longInputs) {
            // Process short window (10s)
            Double shortTotalScore = shortTotalScoreState.value();
            Integer shortCount = shortCountState.value();
            if (null == shortTotalScore) shortTotalScore = 0.0;
            if (null == shortCount) shortCount = 0;

            // Process long window (60s)
            Double longTotalScore = longTotalScoreState.value();
            Integer longCount = longCountState.value();
            if (null == longTotalScore) longTotalScore = 0.0;
            if (null == longCount) longCount = 0;

            // Process short window retraction and accumulation
            processWindow(shortInputState, shortTotalScore, shortCount, timestamp, shortWindowMs, 
                         shortTotalScoreState, shortCountState);

            // Process long window retraction and accumulation
            processWindow(longInputState, longTotalScore, longCount, timestamp, longWindowMs,
                         longTotalScoreState, longCountState);

            // Calculate hot streak ratio
            Double currentShortTotal = shortTotalScoreState.value();
            Integer currentShortCount = shortCountState.value();
            Double currentLongTotal = longTotalScoreState.value();
            Integer currentLongCount = longCountState.value();

            double shortAvg = (currentShortCount == 0) ? 0.0 : currentShortTotal / currentShortCount;
            double longAvg = (currentLongCount == 0) ? 0.0 : currentLongTotal / currentLongCount;
            double currentRatio = (longAvg == 0.0) ? 0.0 : shortAvg / longAvg;

            // Get previous ratio
            Double previousRatio = previousRatioState.value();
            if (previousRatio == null) {
                previousRatio = 0.0;
            }

            // create output score with current ratio and previous ratio (similar to TotalScoreTimeRangeBoundedPrecedingFunction format)
            Score outputScore = new Score(ctx.getCurrentKey(), currentRatio, previousRatio, timestamp);
            out.collect(outputScore);
            
            // update previousRatioState
            previousRatioState.update(currentRatio);
        }
        lastTriggeringTsState.update(timestamp);
    }

    private void processWindow(MapState<Long, List<Score>> inputState, 
                              Double totalScore, Integer count, 
                              long timestamp, long windowMs,
                              ValueState<Double> totalScoreState, 
                              ValueState<Integer> countState) throws Exception {
        // keep up timestamps of retract data
        List<Long> retractTsList = new ArrayList<>();

        // do retraction - remove scores that are outside the window
        Iterator<Map.Entry<Long, List<Score>>> iter = inputState.entries().iterator();
        while (iter.hasNext()) {
            Map.Entry<Long, List<Score>> data = iter.next();
            Long dataTs = data.getKey();
            Long offset = timestamp - dataTs;
            if (offset > windowMs) {
                List<Score> retractDataList = data.getValue();
                if (retractDataList != null) {
                    for (Score retractScore : retractDataList) {
                        totalScore -= retractScore.getScore();
                        count--;
                    }
                    retractTsList.add(dataTs);
                } else {
                    LOG.warn(
                            "The state is cleared because of state ttl. "
                                    + "This will result in incorrect result. "
                                    + "You can increase the state ttl to avoid this.");
                }
            }
        }

        // do accumulation - add current scores
        List<Score> currentInputs = inputState.get(timestamp);
        if (currentInputs != null) {
            for (Score curScore : currentInputs) {
                totalScore += curScore.getScore();
                count++;
            }
        }

        // update the states
        totalScoreState.update(totalScore);
        countState.update(count);

        // remove the data that has been retracted
        for (Long retractTs : retractTsList) {
            inputState.remove(retractTs);
        }
    }

    @Override
    public void close() throws Exception {
        // No cleanup needed for this function
    }
}
