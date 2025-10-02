package jobs;

import jobs.deserializer.*;
import jobs.models.*;
import jobs.models.ScoreChangeEvent;
import jobs.operators.*;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class LeaderBoardBuilder {
	private static final String REDIS_HOST = "redis";
	private static final int REDIS_PORT = 6379;

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(10000);

		KafkaSource<User> source = KafkaSource.<User>builder()
				.setBootstrapServers("kafka:29092")
				.setTopics("leaderboard_update")
				.setGroupId("leaderboard-flink-consumer")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setDeserializer(new UserDeserV2())
				.build();

		DataStream<User> events = env.fromSource(
				source,
				WatermarkStrategy.<User>forBoundedOutOfOrderness(Duration.ofSeconds(10))
					.withTimestampAssigner((SerializableTimestampAssigner<User>) (e, ts) -> e.getUpdatedAt())
					.withIdleness(Duration.ofSeconds(30)),
				"users-source");


		// Write each record to Redis user all-time ZSET: member=uid, score=level
		events.sinkTo(new BaseSink(REDIS_HOST, REDIS_PORT)).name("base-redis-sink");

		// Pipeline: top-n-user-total-level-gained
		// Step 1) Compute per-event level delta per user (non-negative)
		DataStream<Score> userLevelGained = events
			.map(u -> new Score(u.getUserId(), Math.max(u.getLevel()-u.getPreviousLevel(), 0), u.getUpdatedAt()));
	
		// Step 2) Aggregate rolling total per user in last X minutes (event-time RANGE window)
		DataStream<Score> userTotalLevelGained = userLevelGained
			.keyBy(Score::getId)
			.process(new TotalScoreTimeRangeBoundedPrecedingFunction(1, 5))
			.name("user-total-level-gained-in-last-x-minutes");

        // Step 3) Maintain Top-N across recent Y minutes with retractable updates
        SingleOutputStreamOperator<ScoreChangeEvent> topNUserTotalLevelGained = userTotalLevelGained
            .keyBy(u -> "")
            .process(new RetractableTopNFunction(10, 120, 5))
            .name("top-n-user-total-level-gained-in-last-x-minutes-recent-y-minutes");

		// Step 4) Sink Top-N delta events to Redis using incremental updates (ZSET)
		topNUserTotalLevelGained
			.sinkTo(new ScoreChangeEventSink(REDIS_HOST, REDIS_PORT, "top_level_gainers_recent"))
			.name("redis-top-level-gainers-incremental-sink");

        // Step 4b) Sink periodic Top-N snapshots to MongoDB (side output)
        DataStream<Tuple2<Long, List<Score>>> topNSnapshots =
            topNUserTotalLevelGained.getSideOutput(RetractableTopNFunction.SNAPSHOT_OUTPUT);
        topNSnapshots
            .sinkTo(new SnapshotTopNSink("mongodb://mongo:27017", "leaderboard", "top_level_gainers_snapshots"))
            .name("mongo-top-level-gainers-snapshot-sink");



        // // MVP per team pipeline
        // // 1) Compute per-user all-time total score keyed by userId, carry teamId
		// DataStream<UserScore> userTotals = events
		// 	.keyBy(User::getUserId)
		// 	.process(new UserAllTimeTotalFunction())
		// 	.name("user-alltime-total");

        // // 2) Compute per-team MVP with contribution ratios
		// DataStream<TeamMvp> teamMvp = userTotals
		// 	.keyBy(UserScore::getTeamId)
		// 	.process(new TeamMvpProcessFunction())
		// 	.name("team-mvp");

		// // 3) Sink to Redis hash per team
		// teamMvp
		// 	.sinkTo(new TeamMvpSink(REDIS_HOST, REDIS_PORT, "team_mvp"))
		// 	.name("redis-team-mvp");

        // // Hot Streaks pipeline (similar to top-level-gainers)
        // // 1) Compute hot streak ratio per user
		// DataStream<Score> hotStreaks = events
		// 	.map(u -> new Score(u.getUserId(), Math.max(0, u.getLevel() - u.getPreviousLevel()), u.getUpdatedAt()))
		// 	.keyBy(Score::getId)
		// 	.process(new HotStreakProcessFunction(10_000L, 60_000L))
		// 	.name("hot-streaks");

        // // 2) Top N hot streaks using RetractableTopNFunction
		// DataStream<ScoreChangeEvent> topNHotStreaks = hotStreaks
		// 	.keyBy(u -> "")
		// 	.process(new RetractableTopNFunction(10, 120, 5))
		// 	.name("top-n-hotstreaks");

        // // 3) Sink to Redis using incremental updates
		// topNHotStreaks
		// 	.sinkTo(new ScoreChangeEventSink(REDIS_HOST, REDIS_PORT, "top_hotstreaks_recent"))
		// 	.name("redis-top-hotstreaks-incremental-sink");

		env.execute("LeaderBoard DataStream Job");
	}
}
