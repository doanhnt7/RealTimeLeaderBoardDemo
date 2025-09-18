package jobs.queries;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.DataTypes;
import static org.apache.flink.table.api.Expressions.*;

public class TopPlayersQuery {
    
    public static Table createTopPlayersTable(TableEnvironment tableEnv, Table Users) {
        // Top Players Query using Table API + SQL for ranking
        Table playerAgg = Users
            .groupBy($("user_id"))
            .select(
                $("user_id"),
                $("team_name").max().as("team_name"),
                $("score").sum().cast(DataTypes.BIGINT()).as("total_score")
            );

        tableEnv.createTemporaryView("player_agg", playerAgg);
        
        return tableEnv.sqlQuery(
            "SELECT ROW_NUMBER() OVER (ORDER BY total_score DESC, user_id ASC) AS rnk, " +
            "user_id, team_name, total_score FROM player_agg WHERE ROW_NUMBER() OVER (ORDER BY total_score DESC, user_id ASC) <= 10"
        );
    }
}
