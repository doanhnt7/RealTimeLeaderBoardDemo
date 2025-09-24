package jobs;

import jobs.deserializer.UserDeser;
import jobs.models.*;
import jobs.operators.*;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
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
						.withTimestampAssigner((SerializableTimestampAssigner<User>) (e, ts) -> e.getEventTimeMillis()),
				"users-source");


		// Write each record to Redis user all-time ZSET: member=uid, score=level
		events.sinkTo(new BaseSink("redis", 6379)).name("base-redis-sink");

		DataStream<TopNDelta> userScores = events
			.map(u -> new Score(u.getUserId(), u.getLevel(), u.getEventTimeMillis()))
			.keyBy(s -> "")
			.process(new TopNScoresInLastXMinutesFunction(10, 5))
			.name("top-n-scores-in-last-x-minutes");
		
		userScores.sinkTo(new TopNSink("redis", 6379, "top_scores_5min"))
			.name("redis-top-scores");



        // 1) Compute per-event gain
        DataStream<Score> hotStats = events
            .map(u -> new Score(u.getUserId(), Math.max(0, u.getLevel() - u.getPreviousLevel()), u.getEventTimeMillis()))
            .keyBy(Score::getId)
            .process(new HotStreakProcessFunction(10_000L, 60_000L))
            .name("hot-streaks");

        // 2) Hot streak ratios (10s / 60s)
      

        // 3) Top N hot streaks in last 5 minutes using TTL-based TopN processor
        DataStream<TopNDelta> topHotDelta = hotStats
            .global()
            .process(new TopNScoresInLastXMinutesFunction(10, 5))
            .name("top-n-hotstreaks");

        topHotDelta.sinkTo(new TopNSink("redis", 6379, "top_hotstreaks_5min"))
            .name("redis-top-hotstreaks");

		KeyedStream<User, String> byTeam = events.keyBy(User::getTeamId);
		KeyedStream<User, String> byUser = events.keyBy(User::getUserId);

		

		env.execute("LeaderBoard DataStream Job");
	}
}
