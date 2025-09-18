package jobs;

import jobs.models.*;
import jobs.processors.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

public class LeaderBoardBuilder {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(10000);

		KafkaSource<User> source = KafkaSource.<User>builder()
				.setBootstrapServers("kafka:29092")
				.setTopics("leaderboard_update")
				.setGroupId("leaderboard-flink-consumer")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setDeserializer(new UserDeser())
				.build();

		DataStream<User> events = env.fromSource(
				source,
				WatermarkStrategy.<User>forBoundedOutOfOrderness(Duration.ofSeconds(5))
						.withTimestampAssigner((SerializableTimestampAssigner<User>) (e, ts) -> e.getEventTimeMillis()),
				"users-source");

		// Print each User's toString() to the console
		events.map(user -> {
			System.out.println(user.toString());
			return user;
		}).name("print-user-toString");

		// // Write each record to Redis user all-time ZSET: member=uid, score=level
		// DataStream<User> eventsWithRedis = events.map(new RedisUserAllTimeWriter("redis", 6379, null)).name("redis-user-alltime-writer");

		// KeyedStream<User, String> byTeam = eventsWithRedis.keyBy(User::getTeamId);
		// KeyedStream<User, String> byUser = eventsWithRedis.keyBy(User::getUserId);

		// DataStream<TeamTotal> teamTotals = byTeam.process(new RollingSumPerTeam()).name("team-totals");
		// DataStream<PlayerTotal> playerTotals = byUser.process(new RollingSumPerUser()).name("player-totals");

		// AllWindowedStream<TeamTotal, TimeWindow> teamTotalsWindow = teamTotals.windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)));
		// DataStream<TopTeam> topTeams = teamTotalsWindow.apply(new TopTeamsWindow()).name("compute-top-teams");

		// AllWindowedStream<PlayerTotal, TimeWindow> playerTotalsWindow = playerTotals.windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)));
		// DataStream<TopPlayer> topPlayers = playerTotalsWindow.apply(new TopPlayersWindow()).name("compute-top-players");

		// DataStream<HotStat> hotStats = eventsWithRedis.keyBy(User::getUserId).process(new HotStreakProcessFunction(10_000L, 60_000L)).name("hot-stats");
		// DataStream<HotStreaker> hotTop = hotStats.windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
		// 		.apply(new HotStreakersWindow()).name("compute-hot-streakers");

		// DataStream<PlayerTeamTotal> playerTeamTotals = eventsWithRedis
		// 		.keyBy(e -> e.getTeamId() + "|" + e.getUserId())
		// 		.process(new RollingSumPerUserTeam()).name("player-team-totals");

		// DataStream<TeamMvpCandidate> mvpCandidates = playerTeamTotals
		// 		.join(teamTotals)
		// 		.where(PlayerTeamTotal::getTeamId)
		// 		.equalTo(TeamTotal::getTeamId)
		// 		.window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
		// 		.apply((left, right) -> {
		// 			double ratio = right.getTotalScore() == 0 ? 0.0 : (left.getPlayerTotal() * 1.0) / right.getTotalScore();
		// 			return new TeamMvpCandidate(left.getUserId(), left.getTeamName(), left.getPlayerTotal(), right.getTotalScore(), ratio);
		// 		}).name("mvp-candidates");

		// DataStream<TeamMvp> teamMvps = mvpCandidates
		// 		.windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
		// 		.apply(new TeamMvpWindow()).name("team-mvps");

		// KafkaSink<String> topTeamsSink = KafkaSink.<String>builder()
		// 		.setBootstrapServers("kafka:29092")
		// 		.setRecordSerializer(org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.builder()
		// 				.setTopic("top-teams")
		// 				.setValueSerializationSchema(new org.apache.flink.api.common.serialization.SimpleStringSchema())
		// 				.build())
		// 		.setDeliveryGuarantee(org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE)
		// 		.build();

		// KafkaSink<String> topPlayersSink = KafkaSink.<String>builder()
		// 		.setBootstrapServers("kafka:29092")
		// 		.setRecordSerializer(org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.builder()
		// 				.setTopic("top-players")
		// 				.setValueSerializationSchema(new org.apache.flink.api.common.serialization.SimpleStringSchema())
		// 				.build())
		// 		.setDeliveryGuarantee(org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE)
		// 		.build();

		// KafkaSink<String> hotStreakersSink = KafkaSink.<String>builder()
		// 		.setBootstrapServers("kafka:29092")
		// 		.setRecordSerializer(org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.builder()
		// 				.setTopic("hot-streakers")
		// 				.setValueSerializationSchema(new org.apache.flink.api.common.serialization.SimpleStringSchema())
		// 				.build())
		// 		.setDeliveryGuarantee(org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE)
		// 		.build();

		// KafkaSink<String> teamMvpsSink = KafkaSink.<String>builder()
		// 		.setBootstrapServers("kafka:29092")
		// 		.setRecordSerializer(org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.builder()
		// 				.setTopic("team-mvps")
		// 				.setValueSerializationSchema(new org.apache.flink.api.common.serialization.SimpleStringSchema())
		// 				.build())
		// 		.setDeliveryGuarantee(org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE)
		// 		.build();

		// topTeams.map(TopTeam::toJson).sinkTo(topTeamsSink).name("sink-top-teams");
		// topPlayers.map(TopPlayer::toJson).sinkTo(topPlayersSink).name("sink-top-players");
		// hotTop.map(HotStreaker::toJson).sinkTo(hotStreakersSink).name("sink-hot-streakers");
		// teamMvps.map(TeamMvp::toJson).sinkTo(teamMvpsSink).name("sink-team-mvps");

        // Redis ZSET writes are done via the map side-effect above

		env.execute("LeaderBoard DataStream Job");
	}
}
