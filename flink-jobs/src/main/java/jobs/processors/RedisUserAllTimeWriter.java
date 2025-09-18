package jobs.processors;

import jobs.models.User;
import org.apache.flink.api.common.functions.RichMapFunction;
import redis.clients.jedis.JedisPooled;
import org.apache.flink.api.common.functions.OpenContext;

public class RedisUserAllTimeWriter extends RichMapFunction<User, User> {
    private final String redisHost;
    private final int redisPort;
    private transient JedisPooled jedis;

    public RedisUserAllTimeWriter(String redisHost, int redisPort, String redisPassword) {
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
            if (uid != null) {
                jedis.zadd("leaderboard_user_alltime", (double) level, uid);
            }
        }
        return value;
    }

    @Override
    public void close() throws Exception {
        if (jedis != null) jedis.close();
    }
}


