package jobs.processors;

import jobs.models.Score;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import java.util.ArrayList;
import java.util.List;

public class HotStreakProcessFunction extends KeyedProcessFunction<String, Score, Score> {
    private final long shortWindowMs;
    private final long longWindowMs;
    private transient ListState<Tuple2<Long, Double>> shortState;
    private transient ListState<Tuple2<Long, Double>> longState;
    private transient ValueState<Double> peakHotnessState;

	public HotStreakProcessFunction(long shortWindowMs, long longWindowMs) {
		this.shortWindowMs = shortWindowMs;
		this.longWindowMs = longWindowMs;
	}

	@Override
	public void open(OpenContext openContext) {

        ListStateDescriptor<Tuple2<Long, Double>> shortDesc = new ListStateDescriptor<>(
                "hot-short-state",
                TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {})
        );

        shortState = getRuntimeContext().getListState(shortDesc);

        ListStateDescriptor<Tuple2<Long, Double>> longDesc = new ListStateDescriptor<>(
                "hot-long-state",
                TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {})
        );

        longState = getRuntimeContext().getListState(longDesc);

        ValueStateDescriptor<Double> peakDesc = new ValueStateDescriptor<>(
                "hot-peak",
                Double.class
        );
        peakHotnessState = getRuntimeContext().getState(peakDesc);
	}

	@Override
    public void processElement(Score value, Context ctx, Collector<Score> out) throws Exception {
        long ts = value.getLastUpdateTime();
        Tuple2<Long, Double> entry = Tuple2.of(ts, value.getScore());
        try {
            shortState.add(entry);
            longState.add(entry);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        double shortAvg = evictAndAvg(shortState, ts, shortWindowMs);
        double longAvg = evictAndAvg(longState, ts, longWindowMs);
        double ratio = longAvg == 0.0 ? 0.0 : shortAvg / longAvg;
        Double peak = peakHotnessState.value();
        if (peak == null) peak = 0.0;
        if (ratio > peak) {
            peak = ratio;
            peakHotnessState.update(peak);
        }
        out.collect(new Score(value.getId(), peak, value.getLastUpdateTime()));
	}

    private static double evictAndAvg(ListState<Tuple2<Long, Double>> state, long now, long window) {
        long bound = now - window;
        long sum = 0;
        int cnt = 0;
        List<Tuple2<Long, Double>> kept = new ArrayList<>();
        try {
            for (Tuple2<Long, Double> t : state.get()) {
                if (t.f0 >= bound) {
                    kept.add(t);
                    sum += t.f1;
                    cnt++;
                }
            }
            state.update(kept);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return cnt == 0 ? 0.0 : (sum * 1.0) / cnt;
    }
}
