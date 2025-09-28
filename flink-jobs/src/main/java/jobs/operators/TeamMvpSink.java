package jobs.operators;

import jobs.models.TeamMvp;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import redis.clients.jedis.JedisPooled;

public class TeamMvpSink implements Sink<TeamMvp> {
    private final String redisHost;
    private final int redisPort;
    private final String redisKeyPrefix;

    public TeamMvpSink(String redisHost, int redisPort, String redisKeyPrefix) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisKeyPrefix = redisKeyPrefix;
    }

    @Override
    public SinkWriter<TeamMvp> createWriter(WriterInitContext context) {
        return new Writer(redisHost, redisPort, redisKeyPrefix);
    }

    private static class Writer implements SinkWriter<TeamMvp> {
        private final String host;
        private final int port;
        private final String prefix;
        private transient JedisPooled jedis;

        public Writer(String host, int port, String prefix) {
            this.host = host;
            this.port = port;
            this.prefix = prefix;
        }

        @Override
        public void write(TeamMvp value, Context context) {
            if (value == null) return;
            if (jedis == null) jedis = new JedisPooled(host, port);
            String key = prefix + ":" + value.getTeamId();
            jedis.hset(key, "teamId", value.getTeamId());
            jedis.hset(key, "mvpUserId", value.getMvpUserId());
            jedis.hset(key, "mvpUserTotalScore", Double.toString(value.getMvpUserTotalScore()));
            jedis.hset(key, "teamTotalScore", Double.toString(value.getTeamTotalScore()));
            jedis.hset(key, "currentContributionRatio", Double.toString(value.getCurrentContributionRatio()));
            jedis.hset(key, "lastUpdateTime", Long.toString(value.getLastUpdateTime()));
        }

        @Override
        public void flush(boolean endOfInput) {}

        @Override
        public void close() {
            if (jedis != null) jedis.close();
        }
    }
}


