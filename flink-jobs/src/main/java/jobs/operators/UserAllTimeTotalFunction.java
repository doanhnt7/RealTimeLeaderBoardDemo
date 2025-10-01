package jobs.operators;

import jobs.models.User;
import jobs.models.UserScore;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Computes all-time total score per user (monotonic, based on level delta), and emits UserScore including teamId.
 */
public class UserAllTimeTotalFunction extends KeyedProcessFunction<String, User, UserScore> {
    private transient ValueState<Double> userTotalState;

    @Override
    public void open(OpenContext openContext) throws Exception {
        ValueStateDescriptor<Double> totalDesc = new ValueStateDescriptor<>("userAllTimeTotal", Types.DOUBLE);
        userTotalState = getRuntimeContext().getState(totalDesc);
    }

    @Override
    public void processElement(User value, Context ctx, Collector<UserScore> out) throws Exception {
        Double total = userTotalState.value();
        if (total == null) total = 0.0;

        int level = value.getLevel();
        int prevLevel = value.getPreviousLevel();
        int delta = Math.max(0, level - prevLevel);
        if (delta != 0) {
            total += delta;
            userTotalState.update(total);
        }

        out.collect(new UserScore(value.getUserId(), value.getTeamId(), total, delta, value.getUpdatedAt()));
    }
}


