package jobs.processors;

import jobs.models.User;
import org.apache.flink.api.common.functions.RichMapFunction;
import redis.clients.jedis.JedisPooled;
import org.apache.flink.api.common.functions.OpenContext;
import java.time.OffsetDateTime;
import java.time.temporal.WeekFields;
import java.util.Locale;
public class RedisBaseWriter extends RichMapFunction<User, User> {
    private final String redisHost;
    private final int redisPort;
    private transient JedisPooled jedis;

    public RedisBaseWriter(String redisHost, int redisPort, String redisPassword) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        this.jedis = new JedisPooled(redisHost, redisPort);
    }

    @Override
    public User map(User value) {
        if (value != null) {
            String uid = value.getUid();
            int level = value.getLevel();
            int prevLevel = value.getPreviousLevel();
            if (uid != null) {
                jedis.zadd("leaderboard_user_alltime", (double) level, uid);

                OffsetDateTime ts = value.getUpdatedAt(); // event timestamp
                WeekFields wf = WeekFields.of(Locale.getDefault());
                int weekNumber = ts.get(wf.weekOfWeekBasedYear());
                int year = ts.getYear();
                String weeklyHighestLevelKey = "leaderboard_weekly_highest_level:" + year + ":" + weekNumber;
                String weeklyKeyLevelsGained = "leaderboard_weekly_levels_gained:" + year + ":" + weekNumber;
                int scoreDelta = level - prevLevel;
                if (scoreDelta > 0) {
                    jedis.zincrby(weeklyKeyLevelsGained, (double) scoreDelta, uid);
                }
                jedis.zincrby(weeklyHighestLevelKey, (double) level, uid);
            }
        }
        return value;
    }

    @Override
    public void close() throws Exception {
        if (jedis != null) jedis.close();
    }
}


