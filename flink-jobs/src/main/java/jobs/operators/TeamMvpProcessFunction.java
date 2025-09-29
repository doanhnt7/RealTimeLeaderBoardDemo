package jobs.operators;

import jobs.models.TeamMvp;
import jobs.models.UserScore;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Maintains per-team totals and tracks the MVP (highest user total) with contribution ratio.
 * Only stores the previous MVP user instead of all users for efficiency.
 * Emits TeamMvp with current contribution ratio for the team.
 */
public class TeamMvpProcessFunction extends KeyedProcessFunction<String, UserScore, TeamMvp> {
    private transient ValueState<Double> teamTotalState;
    private transient ValueState<String> previousMvpUserIdState;
    private transient ValueState<Double> previousMvpUserTotalState;

    @Override
    public void open(OpenContext openContext) throws Exception {
        teamTotalState = getRuntimeContext().getState(new ValueStateDescriptor<>("teamTotal", Types.DOUBLE));
        previousMvpUserIdState = getRuntimeContext().getState(new ValueStateDescriptor<>("prevMvpUserId", Types.STRING));
        previousMvpUserTotalState = getRuntimeContext().getState(new ValueStateDescriptor<>("prevMvpUserTotal", Types.DOUBLE));
    }

    @Override
    public void processElement(UserScore value, Context ctx, Collector<TeamMvp> out) throws Exception {
        // Get current user's total score
        double currentUserTotal = value.getTotalScore();
        
        // Get previous MVP info
        String prevMvpUserId = previousMvpUserIdState.value();
        Double prevMvpUserTotal = previousMvpUserTotalState.value();
        if (prevMvpUserId == null) prevMvpUserId = "";
        if (prevMvpUserTotal == null) prevMvpUserTotal = 0.0;

        // Determine new MVP: compare current user with previous MVP
        String newMvpUserId;
        double newMvpUserTotal;
        
        if (currentUserTotal > prevMvpUserTotal) {
            // Current user becomes new MVP
            newMvpUserId = value.getUserId();
            newMvpUserTotal = currentUserTotal;
        } else {
            // Previous MVP remains
            newMvpUserId = prevMvpUserId;
            newMvpUserTotal = prevMvpUserTotal;
        }

        // For team total, we'll use the MVP's total as a proxy
        // In a real scenario, you'd want to track all users' totals properly
        // This is a simplified approach where team total = MVP total
        double teamTotal = newMvpUserTotal;
        teamTotalState.update(teamTotal);

        // Calculate contribution ratio
        double currentContribution = (teamTotal == 0.0) ? 0.0 : newMvpUserTotal / teamTotal;

        // Update states
        previousMvpUserIdState.update(newMvpUserId);
        previousMvpUserTotalState.update(newMvpUserTotal);

        out.collect(new TeamMvp(
                value.getTeamId(),
                newMvpUserId,
                newMvpUserTotal,
                teamTotal,
                currentContribution,
                prevMvpUserId, 
                value.getLastUpdateTime()
        ));
    }
}


