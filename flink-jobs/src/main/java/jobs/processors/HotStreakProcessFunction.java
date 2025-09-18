package jobs.processors;

import jobs.models.HotStat;
import jobs.models.User;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.OpenContext;
import java.util.ArrayDeque;
import java.util.Deque;

public class HotStreakProcessFunction extends KeyedProcessFunction<String, User, HotStat> {
	private final long shortWindowMs;
	private final long longWindowMs;
	private transient Deque<Tuple2<Long, Integer>> shortDeque;
	private transient Deque<Tuple2<Long, Integer>> longDeque;
	private double peakHotness;

	public HotStreakProcessFunction(long shortWindowMs, long longWindowMs) {
		this.shortWindowMs = shortWindowMs;
		this.longWindowMs = longWindowMs;
	}

	@Override
	public void open(OpenContext openContext) {
		shortDeque = new ArrayDeque<>();
		longDeque = new ArrayDeque<>();
		peakHotness = 0.0;
	}

	@Override
	public void processElement(User value, Context ctx, Collector<HotStat> out) {
		long ts = value.getEventTimeMillis();
		shortDeque.addLast(Tuple2.of(ts, value.getScore()));
		longDeque.addLast(Tuple2.of(ts, value.getScore()));
		evict(ts, shortDeque, shortWindowMs);
		evict(ts, longDeque, longWindowMs);
		double shortAvg = avg(shortDeque);
		double longAvg = avg(longDeque);
		double ratio = longAvg == 0.0 ? 0.0 : shortAvg / longAvg;
		if (ratio > peakHotness) peakHotness = ratio;
		out.collect(new HotStat(value.getUserId(), shortAvg, longAvg, peakHotness));
	}

	private static void evict(long now, Deque<Tuple2<Long, Integer>> dq, long window) {
		long bound = now - window;
		while (!dq.isEmpty() && dq.peekFirst().f0 < bound) dq.removeFirst();
	}
	private static double avg(Deque<Tuple2<Long, Integer>> dq) {
		if (dq.isEmpty()) return 0.0;
		long sum = 0; int cnt = 0;
		for (Tuple2<Long, Integer> t : dq) { sum += t.f1; cnt++; }
		return cnt == 0 ? 0.0 : (sum * 1.0) / cnt;
	}
}
