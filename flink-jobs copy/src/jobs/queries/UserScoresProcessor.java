package jobs.queries;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.DataTypes;
import static org.apache.flink.table.api.Expressions.*;

public class UserScoresProcessor {
    
    public static Table createUserScoresView(TableEnvironment tableEnv, Table usersRaw) {
        // Create user_scores view using Table API
        return usersRaw
            .select(
                $("uid").as("user_id"),
                $("teamId").cast(DataTypes.STRING()).as("team_id"),
                concat(lit("Team "), $("teamId").cast(DataTypes.STRING())).as("team_name"),
                $("level").as("score"),
                $("updated_at").cast(DataTypes.TIMESTAMP_LTZ(3)).as("event_time")
            );
    }
}
