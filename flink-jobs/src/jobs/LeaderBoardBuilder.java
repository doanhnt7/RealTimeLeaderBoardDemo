package flinkfintechpoc.jobs;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import static org.apache.flink.table.api.Expressions.*;

public class LeaderBoardBuilder {

	public static void main(String[] args) throws Exception {
		EnvironmentSettings settings = EnvironmentSettings
				.newInstance()
				.inStreamingMode()
				.build();
		TableEnvironment tableEnv = TableEnvironment.create(settings);

		// Create source table using Table API
		Table usersRaw = tableEnv.from(TableDescriptor
			.forConnector("kafka")
			.schema(Schema.newBuilder()
				.column("_id", DataTypes.STRING())
				.column("uid", DataTypes.STRING())
				.column("email", DataTypes.STRING())
				.column("authProvider", DataTypes.STRING())
				.column("appId", DataTypes.STRING())
				.column("avatar", DataTypes.STRING())
				.column("geo", DataTypes.STRING())
				.column("role", DataTypes.STRING())
				.column("lastLoginAt", DataTypes.STRING())
				.column("name", DataTypes.STRING())
				.column("devices", DataTypes.ARRAY(DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())))
				.column("resources", DataTypes.ARRAY(DataTypes.STRING()))
				.column("created_at", DataTypes.STRING())
				.column("updated_at", DataTypes.STRING())
				.column("level", DataTypes.INT())
				.column("updatedAt", DataTypes.STRING())
				.column("teamId", DataTypes.INT())
				.column("ts", DataTypes.TIMESTAMP_LTZ(3))
				.watermark("ts", "ts - INTERVAL '5' SECOND")
				.build())
			.option("topic", "users")
			.option("properties.bootstrap.servers", "kafka:29092")
			.option("scan.startup.mode", "earliest-offset")
			.option("format", "json")
			.option("json.ignore-parse-errors", "true")
			.build());

		// Create user_scores view using Table API
		Table userScores = usersRaw
			.select(
				$("uid").as("user_id"),
				$("teamId").cast(DataTypes.STRING()).as("team_id"),
				concat(lit("Team "), $("teamId").cast(DataTypes.STRING())).as("team_name"),
				$("level").as("score"),
				$("updated_at").cast(DataTypes.TIMESTAMP_LTZ(3)).as("event_time")
			);

		// Create sink tables using Table API
		TableDescriptor topTeamsDescriptor = TableDescriptor
			.forConnector("upsert-kafka")
			.schema(Schema.newBuilder()
				.column("rnk", DataTypes.BIGINT())
				.column("team_id", DataTypes.STRING())
				.column("team_name", DataTypes.STRING())
				.column("total_score", DataTypes.BIGINT())
				.primaryKey("rnk")
				.build())
			.option("topic", "top-teams")
			.option("properties.bootstrap.servers", "kafka:29092")
			.option("key.format", "json")
			.option("value.format", "json")
			.build();

		TableDescriptor topPlayersDescriptor = TableDescriptor
			.forConnector("upsert-kafka")
			.schema(Schema.newBuilder()
				.column("rnk", DataTypes.BIGINT())
				.column("user_id", DataTypes.STRING())
				.column("team_name", DataTypes.STRING())
				.column("total_score", DataTypes.BIGINT())
				.primaryKey("rnk")
				.build())
			.option("topic", "top-players")
			.option("properties.bootstrap.servers", "kafka:29092")
			.option("key.format", "json")
			.option("value.format", "json")
			.build();

		TableDescriptor hotStreakersDescriptor = TableDescriptor
			.forConnector("upsert-kafka")
			.schema(Schema.newBuilder()
				.column("rnk", DataTypes.BIGINT())
				.column("user_id", DataTypes.STRING())
				.column("short_term_avg", DataTypes.DOUBLE())
				.column("long_term_avg", DataTypes.DOUBLE())
				.column("peak_hotness", DataTypes.DOUBLE())
				.primaryKey("rnk")
				.build())
			.option("topic", "hot-streakers")
			.option("properties.bootstrap.servers", "kafka:29092")
			.option("key.format", "json")
			.option("value.format", "json")
			.build();

		TableDescriptor teamMvpsDescriptor = TableDescriptor
			.forConnector("upsert-kafka")
			.schema(Schema.newBuilder()
				.column("rnk", DataTypes.BIGINT())
				.column("user_id", DataTypes.STRING())
				.column("team_name", DataTypes.STRING())
				.column("player_total", DataTypes.BIGINT())
				.column("team_total", DataTypes.BIGINT())
				.column("contrib_ratio", DataTypes.DOUBLE())
				.primaryKey("rnk")
				.build())
			.option("topic", "team-mvps")
			.option("properties.bootstrap.servers", "kafka:29092")
			.option("key.format", "json")
			.option("value.format", "json")
			.build();

		// Register sink tables
		tableEnv.createTable("top_teams", topTeamsDescriptor);
		tableEnv.createTable("top_players", topPlayersDescriptor);
		tableEnv.createTable("hot_streakers", hotStreakersDescriptor);
		tableEnv.createTable("team_mvps", teamMvpsDescriptor);

		// Register user_scores as a view for easier access
		tableEnv.createTemporaryView("user_scores", userScores);

		// Top Teams Query using Table API + SQL for ranking
		Table teamAgg = userScores
			.groupBy($("team_id"))
			.select(
				$("team_id"),
				$("team_name").max().as("team_name"),
				$("score").sum().cast(DataTypes.BIGINT()).as("total_score")
			);

		tableEnv.createTemporaryView("team_agg", teamAgg);
		
		Table topTeams = tableEnv.sqlQuery(
			"SELECT ROW_NUMBER() OVER (ORDER BY total_score DESC, team_name ASC) AS rnk, " +
			"team_id, team_name, total_score FROM team_agg WHERE ROW_NUMBER() OVER (ORDER BY total_score DESC, team_name ASC) <= 10"
		);

		// Top Players Query using Table API + SQL for ranking
		Table playerAgg = userScores
			.groupBy($("user_id"))
			.select(
				$("user_id"),
				$("team_name").max().as("team_name"),
				$("score").sum().cast(DataTypes.BIGINT()).as("total_score")
			);

		tableEnv.createTemporaryView("player_agg", playerAgg);
		
		Table topPlayers = tableEnv.sqlQuery(
			"SELECT ROW_NUMBER() OVER (ORDER BY total_score DESC, user_id ASC) AS rnk, " +
			"user_id, team_name, total_score FROM player_agg WHERE ROW_NUMBER() OVER (ORDER BY total_score DESC, user_id ASC) <= 10"
		);

		// Hot Streakers Query using Table API for window functions
		Table shortTermAgg = userScores
			.window(Over.partitionBy($("user_id"))
				.orderBy($("event_time"))
				.preceding(lit(10).second())
				.as("w"))
			.select(
				$("user_id"),
				$("event_time"),
				$("score"),
				$("score").sum().over($("w")).as("short_sum"),
				$("score").count().over($("w")).as("short_cnt")
			);

		Table longTermAgg = shortTermAgg
			.window(Over.partitionBy($("user_id"))
				.orderBy($("event_time"))
				.preceding(lit(60).second())
				.as("w"))
			.select(
				$("user_id"),
				$("event_time"),
				$("short_sum"),
				$("short_cnt"),
				$("score").sum().over($("w")).as("long_sum"),
				$("score").count().over($("w")).as("long_cnt")
			);

		// Calculate peak hotness using SQL for complex division
		tableEnv.createTemporaryView("long_term_agg", longTermAgg);
		
		Table peakHotness = tableEnv.sqlQuery(
			"SELECT user_id, " +
			"MAX(short_sum * 1.0 / NULLIF(short_cnt, 0)) AS short_term_avg, " +
			"MAX(long_sum * 1.0 / NULLIF(long_cnt, 0)) AS long_term_avg, " +
			"MAX((short_sum * 1.0 / NULLIF(short_cnt, 0)) / NULLIF((long_sum * 1.0 / NULLIF(long_cnt, 0)), 0)) AS peak_hotness " +
			"FROM long_term_agg GROUP BY user_id"
		);

		tableEnv.createTemporaryView("peak_hotness", peakHotness);
		
		Table hotStreakers = tableEnv.sqlQuery(
			"SELECT ROW_NUMBER() OVER (ORDER BY peak_hotness DESC, user_id ASC) AS rnk, " +
			"user_id, short_term_avg, long_term_avg, peak_hotness FROM peak_hotness WHERE ROW_NUMBER() OVER (ORDER BY peak_hotness DESC, user_id ASC) <= 10"
		);

		// Team MVPs Query using Table API
		Table playerTeamAgg = userScores
			.groupBy($("user_id"), $("team_id"))
			.select(
				$("user_id"),
				$("team_id"),
				$("score").sum().as("player_total")
			);

		Table teamTotalAgg = userScores
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
		
		Table teamMvps = tableEnv.sqlQuery(
			"SELECT ROW_NUMBER() OVER (ORDER BY contrib_ratio DESC) AS rnk, " +
			"user_id, team_name, player_total, team_total, contrib_ratio " +
			"FROM top_player_per_team WHERE ROW_NUMBER() OVER (ORDER BY contrib_ratio DESC) <= 10"
		);

		// Insert results into sink tables
		tableEnv.executeSql("INSERT INTO top_teams SELECT * FROM " + topTeams);
		tableEnv.executeSql("INSERT INTO top_players SELECT * FROM " + topPlayers);
		tableEnv.executeSql("INSERT INTO hot_streakers SELECT * FROM " + hotStreakers);
		tableEnv.executeSql("INSERT INTO team_mvps SELECT * FROM " + teamMvps);
	}
}