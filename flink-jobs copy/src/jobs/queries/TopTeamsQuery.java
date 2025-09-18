package jobs.queries;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.DataTypes;
import static org.apache.flink.table.api.Expressions.*;

public class TopTeamsQuery {
    
    public static Table createTopTeamsTable(TableEnvironment tableEnv, Table Users) {
        // Top Teams Query using Table API + SQL for ranking
        Table teamAgg = Users
            .groupBy($("team_id"))
            .select(
                $("team_id"),
                $("team_name").max().as("team_name"),
                $("score").sum().cast(DataTypes.BIGINT()).as("total_score")
            );

        tableEnv.createTemporaryView("team_agg", teamAgg);
        
        return tableEnv.sqlQuery(
            "SELECT ROW_NUMBER() OVER (ORDER BY total_score DESC, team_name ASC) AS rnk, " +
            "team_id, team_name, total_score FROM team_agg WHERE ROW_NUMBER() OVER (ORDER BY total_score DESC, team_name ASC) <= 10"
        );
    }
}
