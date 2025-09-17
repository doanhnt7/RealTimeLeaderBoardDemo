package jobs.processors;

import jobs.models.PlayerTotal;
import jobs.models.TopPlayer;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TopPlayersWindow implements AllWindowFunction<PlayerTotal, TopPlayer, TimeWindow> {
	@Override
	public void apply(TimeWindow window, Iterable<PlayerTotal> values, Collector<TopPlayer> out) {
		List<PlayerTotal> list = new ArrayList<>();
		for (PlayerTotal v : values) list.add(v);
		list.sort(Comparator.comparingLong(PlayerTotal::getTotalScore).reversed().thenComparing(PlayerTotal::getUserId));
		long rank = 1;
		for (int i = 0; i < Math.min(10, list.size()); i++) {
			PlayerTotal p = list.get(i);
			out.collect(new TopPlayer(rank++, p.getUserId(), p.getTeamName(), p.getTotalScore()));
		}
	}
}
