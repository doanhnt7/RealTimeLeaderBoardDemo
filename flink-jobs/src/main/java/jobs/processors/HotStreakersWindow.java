package jobs.processors;

import jobs.models.HotStat;
import jobs.models.HotStreaker;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class HotStreakersWindow implements AllWindowFunction<HotStat, HotStreaker, TimeWindow> {
	@Override
	public void apply(TimeWindow window, Iterable<HotStat> values, Collector<HotStreaker> out) {
		List<HotStat> list = new ArrayList<>();
		for (HotStat v : values) list.add(v);
		list.sort(Comparator.comparingDouble(HotStat::getPeakHotness).reversed().thenComparing(HotStat::getUserId));
		long rank = 1;
		for (int i = 0; i < Math.min(10, list.size()); i++) {
			HotStat h = list.get(i);
			out.collect(new HotStreaker(rank++, h.getUserId(), h.getShortAvg(), h.getLongAvg(), h.getPeakHotness()));
		}
	}
}
