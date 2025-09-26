package jobs;

import jobs.deserializer.UserDeser;
import jobs.models.*;
import jobs.models.ScoreChangeEvent;
import jobs.operators.*;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


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
				WatermarkStrategy.<User>forBoundedOutOfOrderness(Duration.ofSeconds(1))
						.withTimestampAssigner((SerializableTimestampAssigner<User>) (e, ts) -> e.getUpdatedAt())
						.withIdleness(Duration.ofSeconds(30)),
				"users-source");


		// Write each record to Redis user all-time ZSET: member=uid, score=level
		events.sinkTo(new BaseSink("redis", 6379)).name("base-redis-sink");



        

        DataStream<Score> userLevelGained = events
		.map(u -> new Score(u.getUserId(), Math.max(u.getLevel()-u.getPreviousLevel(), 0), u.getUpdatedAt()));
	
		DataStream<Score> userTotalLevelGained = userLevelGained.keyBy(Score::getId)
			.process(new TotalScoreTimeRangeBoundedPrecedingFunction(1,5))
			.name("user-total-level-gained-in-last-x-minutes");

		DataStream<ScoreChangeEvent> topNUserTotalLevelGained = userTotalLevelGained.keyBy(u -> "")
			.process(new RetractableTopNFunction(10, 120,5))
			.name("top-n-user-total-level-gained-in-last-x-minutes-recent-y-minutes");

		// Sink the change events to Redis using incremental updates
		topNUserTotalLevelGained.sinkTo(new ScoreChangeEventSink("redis", 6379, "top_level_gainers_recent"))
			.name("redis-top-level-gainers-incremental-sink");



		// // 1) Compute per-event gain
        // DataStream<Score> hotStats = events
        //     .map(u -> new Score(u.getUserId(), Math.max(0, u.getLevel() - u.getPreviousLevel()), u.getUpdatedAt()))
        //     .keyBy(Score::getId)
        //     .process(new HotStreakProcessFunction(10_000L, 60_000L))
        //     .name("hot-streaks");
			
        // // 3) Top N hot streaks in last 5 minutes using TTL-based TopN processor
        // DataStream<TopNDelta> topHotDelta = hotStats
        //     .keyBy(Score::getId)
        //     .process(new TopNScoresInLastXMinutesFunction(10, 5))
        //     .name("top-n-hotstreaks");

        // topHotDelta.sinkTo(new TopNSink("redis", 6379, "top_hotstreaks_5min"))
        //     .name("redis-top-hotstreaks");

		// KeyedStream<User, String> byTeam = events.keyBy(User::getTeamId);
		// KeyedStream<User, String> byUser = events.keyBy(User::getUserId);

		

		env.execute("LeaderBoard DataStream Job");
	}
}
