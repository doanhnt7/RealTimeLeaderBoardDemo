package jobs.operators;

import jobs.models.Score;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import java.time.Duration;
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
 * Process Function for RANGE clause event-time bounded OVER window for Score aggregation.
 * 
 * This function calculates the total score for each user within a time-bounded window.
 * It maintains state for incremental calculation and handles late data appropriately.
 * 
 * E.g.: For each Score event, calculate the total score gained in the last X minutes.
 */
public class TotalScoreTimeRangeBoundedPrecedingFunction
        extends KeyedProcessFunction<String, Score, Score> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(TotalScoreTimeRangeBoundedPrecedingFunction.class);

    private final long precedingOffset; 

    // the state which keeps the last triggering timestamp
    private transient ValueState<Long> lastTriggeringTsState;

    // the state which keeps the total score accumulator
    private transient ValueState<Double> totalScoreState;

    // the state which keeps the safe timestamp to cleanup states
    private transient ValueState<Long> cleanupTsState;

    // the state which keeps all the score data that are not expired.
    // The first element (as the mapState key) of the tuple is the time stamp. Per each time stamp,
    // the second element of tuple is a list that contains all the Score objects belonging
    // to this time stamp.
    private transient MapState<Long, List<Score>> inputState;

    //the state which keeps the previous score
    private transient ValueState<Double> previousScoreState;
    private final long previousScoreTtlMinutes;

    // ------------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------------
    private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";
    private transient Counter numLateRecordsDropped;

    public TotalScoreTimeRangeBoundedPrecedingFunction(int windowSizeMinutes, long previousScoreTtlMinutes) {
        this.precedingOffset = windowSizeMinutes * 60 * 1000L; // convert minutes to milliseconds
        this.previousScoreTtlMinutes = previousScoreTtlMinutes;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        // TTL config: 1 hour
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Duration.ofMinutes(previousScoreTtlMinutes))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();

        ValueStateDescriptor<Long> lastTriggeringTsDescriptor =
                new ValueStateDescriptor<>("lastTriggeringTsState", Types.LONG);
        lastTriggeringTsState = getRuntimeContext().getState(lastTriggeringTsDescriptor);

        ValueStateDescriptor<Double> totalScoreDescriptor =
                new ValueStateDescriptor<>("totalScoreState", Types.DOUBLE);
        totalScoreState = getRuntimeContext().getState(totalScoreDescriptor);

        ValueStateDescriptor<Long> cleanupTsStateDescriptor =
                new ValueStateDescriptor<>("cleanupTsState", Types.LONG);
        this.cleanupTsState = getRuntimeContext().getState(cleanupTsStateDescriptor);

        // input element are all Score objects
        ListTypeInfo<Score> scoreListTypeInfo = new ListTypeInfo<>(Score.class);
        MapStateDescriptor<Long, List<Score>> inputStateDesc =
                new MapStateDescriptor<>(
                        "inputState", Types.LONG, scoreListTypeInfo);
        inputState = getRuntimeContext().getMapState(inputStateDesc);

        // NEW: previousScoreState with TTL
        ValueStateDescriptor<Double> previousScoreDescriptor =
                new ValueStateDescriptor<>("previousScoreState", Types.DOUBLE);
        previousScoreDescriptor.enableTimeToLive(ttlConfig);
        previousScoreState = getRuntimeContext().getState(previousScoreDescriptor);

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
            List<Score> data = inputState.get(triggeringTs);
            if (null != data) {
                data.add(input);
                inputState.put(triggeringTs, data);
            } else {
                data = new ArrayList<>();
                data.add(input);
                inputState.put(triggeringTs, data);
                // register event time timer
                ctx.timerService().registerEventTimeTimer(triggeringTs);
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
        // calculate safe timestamp to cleanup states
        long minCleanupTimestamp = timestamp + precedingOffset + 1;
        long maxCleanupTimestamp = timestamp + (long) (precedingOffset * 1.5) + 1;
        // update timestamp and register timer if needed
        Long curCleanupTimestamp = cleanupTsState.value();
        if (curCleanupTimestamp == null || curCleanupTimestamp < minCleanupTimestamp) {
            // we don't delete existing timer since it may delete timer for data processing, can imp
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
            inputState.clear();
            totalScoreState.clear();
            lastTriggeringTsState.clear();
            cleanupTsState.clear();
            return;
        }

        // gets all window data from state for the calculation
        List<Score> inputs = inputState.get(timestamp);

        if (null != inputs) {
            Double totalScore = totalScoreState.value();
            Double previousTotalScore = previousScoreState.value();
            // initialize when first run or failover recovery per key
            if (null == totalScore) {
                totalScore = 0.0;
            }
            if (null == previousTotalScore) {
                previousTotalScore = 0.0;
            }
            // keep up timestamps of retract data
            List<Long> retractTsList = new ArrayList<>();

            // do retraction - remove scores that are outside the window
            Iterator<Map.Entry<Long, List<Score>>> iter = inputState.entries().iterator();
            while (iter.hasNext()) {
                Map.Entry<Long, List<Score>> data = iter.next();
                Long dataTs = data.getKey();
                Long offset = timestamp - dataTs;
                if (offset > precedingOffset) {
                    List<Score> retractDataList = data.getValue();
                    if (retractDataList != null) {
                        for (Score retractScore : retractDataList) {
                            totalScore -= retractScore.getScore();
                        }
                        retractTsList.add(dataTs);
                    } else {
                        // Does not retract values which are outside of window if the state is
                        // cleared already.
                        LOG.warn(
                                "The state is cleared because of state ttl. "
                                        + "This will result in incorrect result. "
                                        + "You can increase the state ttl to avoid this.");
                    }
                }
            }

            // do accumulation - add current scores
            for (Score curScore : inputs) {
                totalScore += curScore.getScore();
            }

            // update the total score state
            totalScoreState.update(totalScore);

           

            // create output score with total accumulated score
            Score outputScore = new Score(ctx.getCurrentKey(), totalScore, previousTotalScore, timestamp);
            // LOG.info("Emitting outputScore: {}", outputScore);
            out.collect(outputScore);
            
            // update previousScoreState
            previousScoreState.update(totalScore);

            // remove the data that has been retracted
            for (Long retractTs : retractTsList) {
                inputState.remove(retractTs);
            }
        }
        lastTriggeringTsState.update(timestamp);
    }

    @Override
    public void close() throws Exception {
        // No cleanup needed for this function
    }
}
