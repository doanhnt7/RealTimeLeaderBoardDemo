package flinkfintechpoc.jobs;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class LeaderBoardBuilder {

	public static void main(String[] args) throws Exception {
		EnvironmentSettings settings = EnvironmentSettings
				.newInstance()
				.inStreamingMode()
				.build();
		TableEnvironment tableEnv = TableEnvironment.create(settings);

		// Source: users topic from Python producer, mapped to user_scores semantics
		String createSource = ""
			+ "CREATE TABLE user_scores ("
			+ "  user_id STRING,"
			+ "  team_id STRING,"
			+ "  team_name STRING,"
			+ "  score INT,"
			+ "  event_time TIMESTAMP_LTZ(3),"
			+ "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND"
			+ ") WITH ("
			+ "  'connector' = 'kafka',"
			+ "  'topic' = 'users',"
			+ "  'properties.bootstrap.servers' = 'kafka:29092',"
			+ "  'scan.startup.mode' = 'earliest-offset',"
			+ "  'format' = 'json',"
			+ "  'json.ignore-parse-errors' = 'true',"
			+ "  'json.timestamp-format.standard' = 'ISO-8601'"
			+ ")";

		// Create a view to map incoming user documents to the expected fields
		String createView = ""
			+ "CREATE TEMPORARY VIEW user_scores AS "
			+ "SELECT "
			+ "  CAST(uid AS STRING) AS user_id, "
			+ "  CAST(teamId AS STRING) AS team_id, "
			+ "  CONCAT('Team ', CAST(teamId AS STRING)) AS team_name, "
			+ "  CAST(level AS INT) AS score, "
			+ "  CAST(updated_at AS TIMESTAMP_LTZ(3)) AS event_time "
			+ "FROM users_raw";

		// Instead of a physical CREATE TABLE above, register raw source then view
		String createUsersRaw = ""
			+ "CREATE TABLE users_raw ("
			+ "  _id STRING,"
			+ "  uid STRING,"
			+ "  email STRING,"
			+ "  authProvider STRING,"
			+ "  appId STRING,"
			+ "  avatar STRING,"
			+ "  geo STRING,"
			+ "  role STRING,"
			+ "  lastLoginAt STRING,"
			+ "  name STRING,"
			+ "  devices ARRAY<MAP<STRING, STRING>>,"
			+ "  resources ARRAY<STRING>,"
			+ "  created_at STRING,"
			+ "  updated_at STRING,"
			+ "  level INT,"
			+ "  updatedAt STRING,"
			+ "  teamId INT,"
			+ "  ts AS TO_TIMESTAMP_LTZ(CAST(REGEXP_REPLACE(updated_at, '[^0-9]', '') AS BIGINT), 3),"
			+ "  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND"
			+ ") WITH ("
			+ "  'connector' = 'kafka',"
			+ "  'topic' = 'users',"
			+ "  'properties.bootstrap.servers' = 'kafka:29092',"
			+ "  'scan.startup.mode' = 'earliest-offset',"
			+ "  'format' = 'json',"
			+ "  'json.ignore-parse-errors' = 'true'"
			+ ")";

		// Upsert sinks
		String createTopTeams = ""
			+ "CREATE TABLE top_teams ("
			+ "  rnk BIGINT,"
			+ "  team_id STRING,"
			+ "  team_name STRING,"
			+ "  total_score BIGINT,"
			+ "  PRIMARY KEY (rnk) NOT ENFORCED"
			+ ") WITH ("
			+ "  'connector' = 'upsert-kafka',"
			+ "  'topic' = 'top-teams',"
			+ "  'properties.bootstrap.servers' = 'kafka:29092',"
			+ "  'key.format' = 'json',"
			+ "  'value.format' = 'json'"
			+ ")";

		String createTopPlayers = ""
			+ "CREATE TABLE top_players ("
			+ "  rnk BIGINT,"
			+ "  user_id STRING,"
			+ "  team_name STRING,"
			+ "  total_score BIGINT,"
			+ "  PRIMARY KEY (rnk) NOT ENFORCED"
			+ ") WITH ("
			+ "  'connector' = 'upsert-kafka',"
			+ "  'topic' = 'top-players',"
			+ "  'properties.bootstrap.servers' = 'kafka:29092',"
			+ "  'key.format' = 'json',"
			+ "  'value.format' = 'json'"
			+ ")";

		String createHotStreakers = ""
			+ "CREATE TABLE hot_streakers ("
			+ "  rnk BIGINT,"
			+ "  user_id STRING,"
			+ "  short_term_avg DOUBLE,"
			+ "  long_term_avg DOUBLE,"
			+ "  peak_hotness DOUBLE,"
			+ "  PRIMARY KEY (rnk) NOT ENFORCED"
			+ ") WITH ("
			+ "  'connector' = 'upsert-kafka',"
			+ "  'topic' = 'hot-streakers',"
			+ "  'properties.bootstrap.servers' = 'kafka:29092',"
			+ "  'key.format' = 'json',"
			+ "  'value.format' = 'json'"
			+ ")";

		String createTeamMVPs = ""
			+ "CREATE TABLE team_mvps ("
			+ "  rnk BIGINT,"
			+ "  user_id STRING,"
			+ "  team_name STRING,"
			+ "  player_total BIGINT,"
			+ "  team_total BIGINT,"
			+ "  contrib_ratio DOUBLE,"
			+ "  PRIMARY KEY (rnk) NOT ENFORCED"
			+ ") WITH ("
			+ "  'connector' = 'upsert-kafka',"
			+ "  'topic' = 'team-mvps',"
			+ "  'properties.bootstrap.servers' = 'kafka:29092',"
			+ "  'key.format' = 'json',"
			+ "  'value.format' = 'json'"
			+ ")";

		// Execute DDL
		tableEnv.executeSql(createUsersRaw);
		// Materialize a normalized view conforming to user_scores fields
		tableEnv.executeSql("CREATE TEMPORARY VIEW user_scores AS SELECT CAST(uid AS STRING) AS user_id, CAST(teamId AS STRING) AS team_id, CONCAT('Team ', CAST(teamId AS STRING)) AS team_name, CAST(level AS INT) AS score, CAST(updated_at AS TIMESTAMP_LTZ(3)) AS event_time FROM users_raw");
		tableEnv.executeSql(createTopTeams);
		tableEnv.executeSql(createTopPlayers);
		tableEnv.executeSql(createHotStreakers);
		tableEnv.executeSql(createTeamMVPs);

		// Insert statements mirroring the provided SQL
		tableEnv.executeSql(
				"INSERT INTO top_teams " +
				"WITH team_ranks AS ( " +
				"  SELECT team_id, MAX(team_name) AS team_name, CAST(SUM(score) AS BIGINT) AS total_score, " +
				"         ROW_NUMBER() OVER (ORDER BY SUM(score) DESC, MAX(team_name) ASC) AS rnk " +
				"  FROM user_scores GROUP BY team_id ) " +
				"SELECT rnk, team_id, team_name, total_score FROM team_ranks WHERE rnk <= 10"
		);

		tableEnv.executeSql(
				"INSERT INTO top_players " +
				"WITH player_ranks AS ( " +
				"  SELECT user_id, MAX(team_name) AS team_name, CAST(SUM(score) AS BIGINT) AS total_score, " +
				"         ROW_NUMBER() OVER (ORDER BY SUM(score) DESC, user_id ASC) AS rnk " +
				"  FROM user_scores GROUP BY user_id ) " +
				"SELECT rnk, user_id, team_name, total_score FROM player_ranks WHERE rnk <= 10"
		);

		tableEnv.executeSql(
				"INSERT INTO hot_streakers " +
				"WITH short_term_agg AS ( " +
				"  SELECT user_id, event_time, score, " +
				"         SUM(score) OVER (PARTITION BY user_id ORDER BY event_time RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW) AS short_sum, " +
				"         COUNT(score) OVER (PARTITION BY user_id ORDER BY event_time RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW) AS short_cnt " +
				"  FROM user_scores ), " +
				"long_term_agg AS ( " +
				"  SELECT user_id, event_time, short_sum, short_cnt, " +
				"         SUM(score) OVER (PARTITION BY user_id ORDER BY event_time RANGE BETWEEN INTERVAL '60' SECOND PRECEDING AND CURRENT ROW) AS long_sum, " +
				"         COUNT(score) OVER (PARTITION BY user_id ORDER BY event_time RANGE BETWEEN INTERVAL '60' SECOND PRECEDING AND CURRENT ROW) AS long_cnt " +
				"  FROM short_term_agg ), " +
				"peak_hotness AS ( " +
				"  SELECT user_id, MAX(short_sum * 1.0 / NULLIF(short_cnt, 0)) AS short_term_avg, " +
				"         MAX(long_sum * 1.0 / NULLIF(long_cnt, 0)) AS long_term_avg, " +
				"         MAX( (short_sum * 1.0 / NULLIF(short_cnt, 0)) / NULLIF((long_sum * 1.0 / NULLIF(long_cnt, 0)), 0) ) AS peak_hotness " +
				"  FROM long_term_agg GROUP BY user_id ) " +
				"SELECT rnk, user_id, short_term_avg, long_term_avg, peak_hotness FROM ( " +
				"  SELECT user_id, short_term_avg, long_term_avg, peak_hotness, " +
				"         ROW_NUMBER() OVER (ORDER BY peak_hotness DESC, user_id ASC) AS rnk " +
				"  FROM peak_hotness ) WHERE rnk <= 10"
		);

		tableEnv.executeSql(
				"INSERT INTO team_mvps " +
				"WITH player_agg AS ( SELECT user_id, team_id, SUM(score) AS player_total FROM user_scores GROUP BY user_id, team_id ), " +
				"team_agg AS ( SELECT team_id, MAX(team_name) AS team_name, SUM(score) AS team_total FROM user_scores GROUP BY team_id ), " +
				"contrib_agg AS ( SELECT pt.user_id, tt.team_name, pt.player_total, tt.team_total, pt.player_total * 1.0 / tt.team_total AS contrib_ratio FROM player_agg pt JOIN team_agg tt ON pt.team_id = tt.team_id ), " +
				"top_player_per_team AS ( SELECT * FROM ( SELECT user_id, team_name, player_total, team_total, contrib_ratio, ROW_NUMBER() OVER (PARTITION BY team_name ORDER BY contrib_ratio DESC, user_id ASC) AS team_rnk FROM contrib_agg ) WHERE team_rnk = 1 ) " +
				"SELECT rnk, user_id, team_name, player_total, team_total, contrib_ratio FROM ( SELECT user_id, team_name, player_total, team_total, contrib_ratio, ROW_NUMBER() OVER (ORDER BY contrib_ratio DESC) AS rnk FROM top_player_per_team ) WHERE rnk <= 10"
		);
	}
}