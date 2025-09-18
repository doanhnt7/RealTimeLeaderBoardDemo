package jobs.processors;

import jobs.models.TeamTotal;
import jobs.models.User;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.OpenContext;
public class RollingSumPerTeam extends KeyedProcessFunction<String, User, TeamTotal> {
	private transient ValueState<Tuple2<Long, String>> state; // (total, teamName)
	@Override
	public void open(OpenContext openContext) {
		ValueStateDescriptor<Tuple2<Long, String>> desc = new ValueStateDescriptor<>("team-sum", Types.TUPLE(Types.LONG, Types.STRING));
		state = getRuntimeContext().getState(desc);
	}
	@Override
	public void processElement(User value, Context ctx, Collector<TeamTotal> out) throws Exception {
		Tuple2<Long, String> cur = state.value();
		if (cur == null) cur = Tuple2.of(0L, value.getTeamName());
		long next = cur.f0 + value.getScore();
		state.update(Tuple2.of(next, value.getTeamName()));
		out.collect(new TeamTotal(value.getTeamId(), value.getTeamName(), next));
	}
}
