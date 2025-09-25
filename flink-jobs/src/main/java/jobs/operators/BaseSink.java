package jobs.operators;

import jobs.models.User;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import redis.clients.jedis.JedisPooled;
import java.time.OffsetDateTime;
import java.time.temporal.WeekFields;
import java.util.Locale;

public class BaseSink implements Sink<User> {
    private final String redisHost;
    private final int redisPort;

    public BaseSink(String redisHost, int redisPort) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }

    @Override
    public SinkWriter<User> createWriter(WriterInitContext context) {
        return new RedisSinkWriter(redisHost, redisPort);
    }

    private static class RedisSinkWriter implements SinkWriter<User> {
        private final String redisHost;
        private final int redisPort;
        private transient JedisPooled jedis;

        public RedisSinkWriter(String redisHost, int redisPort) {
            this.redisHost = redisHost;
            this.redisPort = redisPort;
        }

        @Override
        public void write(User value, Context context) {
            if (jedis == null) {
                jedis = new JedisPooled(redisHost, redisPort);
            }
            
            if (value != null) {
                String uid = value.getUid();
                int level = value.getLevel();
                int prevLevel = value.getPreviousLevel();
                if (uid != null) {
                    jedis.zadd("leaderboard_user_alltime", (double) level, uid);

                    OffsetDateTime ts = value.getUpdatedAt();
                    WeekFields wf = WeekFields.of(Locale.JAPANESE);
                    int weekNumber = ts.get(wf.weekOfWeekBasedYear());
                    int year = ts.getYear();
                    String weeklyHighestLevelKey = "leaderboard_weekly_highest_level:" + year + ":" + weekNumber;
                    String weeklyKeyLevelsGained = "leaderboard_weekly_levels_gained:" + year + ":" + weekNumber;
                    int scoreDelta = level - prevLevel;
                    if (scoreDelta > 0) {
                        jedis.zincrby(weeklyKeyLevelsGained, (double) scoreDelta, uid);
                    }
                    jedis.zadd(weeklyHighestLevelKey, (double) level, uid);
                }
            }
        }

        @Override
        public void flush(boolean endOfInput) {
            // Redis operations are immediate, no flush needed
        }

        @Override
        public void close() {
            if (jedis != null) {
                jedis.close();
            }
        }
    }
}
