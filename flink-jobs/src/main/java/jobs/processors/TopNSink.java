package jobs.processors;

import jobs.models.TopNDelta;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import redis.clients.jedis.JedisPooled;

public class TopNSink implements Sink<TopNDelta> {
    private final String redisHost;
    private final int redisPort;
    private final String redisKey;

    public TopNSink(String redisHost, int redisPort, String redisKey) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisKey = redisKey;
    }

    @Override
    public SinkWriter<TopNDelta> createWriter(WriterInitContext context) {
        return new RedisSinkWriter(redisHost, redisPort, redisKey);
    }

    private static class RedisSinkWriter implements SinkWriter<TopNDelta> {
        private final String redisHost;
        private final int redisPort;
        private final String redisKey;
        private transient JedisPooled jedis;

        public RedisSinkWriter(String redisHost, int redisPort, String redisKey) {
            this.redisHost = redisHost;
            this.redisPort = redisPort;
            this.redisKey = redisKey;
        }

        @Override
        public void write(TopNDelta value, Context context) {
            if (jedis == null) {
                jedis = new JedisPooled(redisHost, redisPort);
            }
            if (value != null) {
                if (!value.getToRemove().isEmpty()) {
                    jedis.zrem(redisKey, value.getToRemove().toArray(new String[0]));
                }
                if (!value.getToUpsert().isEmpty()) {
                    value.getToUpsert().forEach((member, score) -> jedis.zadd(redisKey, score, member));
                }
            }
        }

        @Override
        public void flush(boolean endOfInput) {
        }

        @Override
        public void close() {
            if (jedis != null) {
                jedis.close();
            }
        }
    }
}


