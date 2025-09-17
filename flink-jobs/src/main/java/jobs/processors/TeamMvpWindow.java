package jobs.processors;

import jobs.models.TeamMvp;
import jobs.models.TeamMvpCandidate;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TeamMvpWindow implements AllWindowFunction<TeamMvpCandidate, TeamMvp, TimeWindow> {
	@Override
	public void apply(TimeWindow window, Iterable<TeamMvpCandidate> values, Collector<TeamMvp> out) {
		List<TeamMvpCandidate> list = new ArrayList<>();
		for (TeamMvpCandidate v : values) list.add(v);
		list.sort(Comparator.comparingDouble(TeamMvpCandidate::getContribRatio).reversed());
		int n = Math.min(10, list.size());
		for (int i = 0; i < n; i++) {
			TeamMvpCandidate c = list.get(i);
			out.collect(new TeamMvp(c.getUserId(), c.getTeamName(), c.getPlayerTotal(), c.getTeamTotal(), c.getContribRatio()));
		}
	}
}
