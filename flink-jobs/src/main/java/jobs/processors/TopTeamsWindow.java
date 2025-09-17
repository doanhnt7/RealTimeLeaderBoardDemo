package jobs.processors;

import jobs.models.TeamTotal;
import jobs.models.TopTeam;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TopTeamsWindow implements AllWindowFunction<TeamTotal, TopTeam, TimeWindow> {
	@Override
	public void apply(TimeWindow window, Iterable<TeamTotal> values, Collector<TopTeam> out) {
		List<TeamTotal> list = new ArrayList<>();
		for (TeamTotal v : values) list.add(v);
		list.sort(Comparator.comparingLong(TeamTotal::getTotalScore).reversed().thenComparing(TeamTotal::getTeamName));
		long rank = 1;
		for (int i = 0; i < Math.min(10, list.size()); i++) {
			TeamTotal t = list.get(i);
			out.collect(new TopTeam(rank++, t.getTeamId(), t.getTeamName(), t.getTotalScore()));
		}
	}
}
