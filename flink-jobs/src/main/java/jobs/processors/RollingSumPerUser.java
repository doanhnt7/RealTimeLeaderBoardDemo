package jobs.processors;

import jobs.models.PlayerTotal;
import jobs.models.UserScore;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.OpenContext;
public class RollingSumPerUser extends KeyedProcessFunction<String, UserScore, PlayerTotal> {
	private transient ValueState<Tuple2<Long, String>> state; // (total, teamName)
	@Override
	public void open(OpenContext openContext) {
		ValueStateDescriptor<Tuple2<Long, String>> desc = new ValueStateDescriptor<>("user-sum", Types.TUPLE(Types.LONG, Types.STRING));
		state = getRuntimeContext().getState(desc);
	}
	@Override
	public void processElement(UserScore value, Context ctx, Collector<PlayerTotal> out) throws Exception {
		Tuple2<Long, String> cur = state.value();
		if (cur == null) cur = Tuple2.of(0L, value.getTeamName());
		long next = cur.f0 + value.getScore();
		state.update(Tuple2.of(next, value.getTeamName()));
		out.collect(new PlayerTotal(value.getUserId(), value.getTeamName(), next));
	}
}
