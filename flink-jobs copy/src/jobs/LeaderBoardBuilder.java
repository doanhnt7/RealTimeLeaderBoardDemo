package jobs;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Table;
import jobs.queries.*;

public class LeaderBoardBuilder {

	public static void main(String[] args) throws Exception {
		EnvironmentSettings settings = EnvironmentSettings
				.newInstance()
				.inStreamingMode()
				.build();
		TableEnvironment tableEnv = TableEnvironment.create(settings);

		// Create source table using TableDescriptors
		Table usersRaw = tableEnv.from(TableDescriptors.createUsersSourceDescriptor());

		// Create user_scores view using UsersProcessor
		Table Users = UsersProcessor.createUsersView(tableEnv, usersRaw);
		tableEnv.createTemporaryView("user_scores", Users);

		// Register sink tables using TableDescriptors
		tableEnv.createTable("top_teams", TableDescriptors.createTopTeamsSinkDescriptor());
		tableEnv.createTable("top_players", TableDescriptors.createTopPlayersSinkDescriptor());
		tableEnv.createTable("hot_streakers", TableDescriptors.createHotStreakersSinkDescriptor());
		tableEnv.createTable("team_mvps", TableDescriptors.createTeamMvpsSinkDescriptor());

		// Create query tables using dedicated query classes
		Table topTeams = TopTeamsQuery.createTopTeamsTable(tableEnv, Users);
		Table topPlayers = TopPlayersQuery.createTopPlayersTable(tableEnv, Users);
		Table hotStreakers = HotStreakersQuery.createHotStreakersTable(tableEnv, Users);
		Table teamMvps = TeamMvpsQuery.createTeamMvpsTable(tableEnv, Users);

		// Insert results into sink tables
		tableEnv.executeSql("INSERT INTO top_teams SELECT * FROM " + topTeams);
		tableEnv.executeSql("INSERT INTO top_players SELECT * FROM " + topPlayers);
		tableEnv.executeSql("INSERT INTO hot_streakers SELECT * FROM " + hotStreakers);
		tableEnv.executeSql("INSERT INTO team_mvps SELECT * FROM " + teamMvps);
	}
}