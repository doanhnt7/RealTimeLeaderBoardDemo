package jobs.queries;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.DataTypes;
import static org.apache.flink.table.api.Expressions.*;

public class TeamMvpsQuery {
    
    public static Table createTeamMvpsTable(TableEnvironment tableEnv, Table Users) {
        // Team MVPs Query using Table API
        Table playerTeamAgg = Users
            .groupBy($("user_id"), $("team_id"))
            .select(
                $("user_id"),
                $("team_id"),
                $("score").sum().as("player_total")
            );

        Table teamTotalAgg = Users
            .groupBy($("team_id"))
            .select(
                $("team_id"),
                $("team_name").max().as("team_name"),
                $("score").sum().as("team_total")
            );

        Table contribAgg = playerTeamAgg
            .join(teamTotalAgg, $("team_id").isEqual($("team_id")))
            .select(
                $("user_id"),
                $("team_name"),
                $("player_total"),
                $("team_total"),
                $("player_total").cast(DataTypes.DOUBLE()).div($("team_total").cast(DataTypes.DOUBLE())).as("contrib_ratio")
            );

        tableEnv.createTemporaryView("contrib_agg", contribAgg);
        
        Table topPlayerPerTeam = tableEnv.sqlQuery(
            "SELECT user_id, team_name, player_total, team_total, contrib_ratio, " +
            "ROW_NUMBER() OVER (PARTITION BY team_name ORDER BY contrib_ratio DESC, user_id ASC) AS team_rnk " +
            "FROM contrib_agg"
        ).where($("team_rnk").isEqual(1));

        tableEnv.createTemporaryView("top_player_per_team", topPlayerPerTeam);
        
        return tableEnv.sqlQuery(
            "SELECT ROW_NUMBER() OVER (ORDER BY contrib_ratio DESC) AS rnk, " +
            "user_id, team_name, player_total, team_total, contrib_ratio " +
            "FROM top_player_per_team WHERE ROW_NUMBER() OVER (ORDER BY contrib_ratio DESC) <= 10"
        );
    }
}
