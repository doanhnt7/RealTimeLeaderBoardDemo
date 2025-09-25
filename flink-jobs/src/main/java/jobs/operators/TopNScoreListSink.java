package jobs.operators;

import jobs.models.Score;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import redis.clients.jedis.JedisPooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Sink that writes a List<Score> to Redis as a sorted set.
 * Each time a new list is received, it replaces the entire sorted set.
 */
public class TopNScoreListSink implements Sink<List<Score>> {
    private static final Logger LOG = LoggerFactory.getLogger(TopNScoreListSink.class);
    
    private final String redisHost;
    private final int redisPort;
    private final String redisKey;

    public TopNScoreListSink(String redisHost, int redisPort, String redisKey) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisKey = redisKey;
    }

    @Override
    public SinkWriter<List<Score>> createWriter(WriterInitContext context) {
        return new RedisScoreListWriter(redisHost, redisPort, redisKey);
    }

    private static class RedisScoreListWriter implements SinkWriter<List<Score>> {
        private final String redisHost;
        private final int redisPort;
        private final String redisKey;
        private transient JedisPooled jedis;

        public RedisScoreListWriter(String redisHost, int redisPort, String redisKey) {
            this.redisHost = redisHost;
            this.redisPort = redisPort;
            this.redisKey = redisKey;
        }

        @Override
        public void write(List<Score> scoreList, Context context) {
            if (jedis == null) {
                jedis = new JedisPooled(redisHost, redisPort);
            }
            
            if (scoreList != null && !scoreList.isEmpty()) {
                try {
                    // Clear the existing sorted set
                    jedis.del(redisKey);
                    
                    // Add all scores to the sorted set
                    for (Score score : scoreList) {
                        jedis.zadd(redisKey, score.getScore(), score.getId());
                    }
                    
                    // Set expiration time (optional, e.g., 1 hour)
                    jedis.expire(redisKey, 3600);
                    
                    LOG.debug("Updated Redis key '{}' with {} scores", redisKey, scoreList.size());
                    
                } catch (Exception e) {
                    LOG.error("Failed to write score list to Redis key '{}'", redisKey, e);
                }
            } else {
                // If empty list, clear the key
                try {
                    jedis.del(redisKey);
                    LOG.debug("Cleared Redis key '{}' due to empty score list", redisKey);
                } catch (Exception e) {
                    LOG.error("Failed to clear Redis key '{}'", redisKey, e);
                }
            }
        }

        @Override
        public void flush(boolean endOfInput) {
            // No buffering, so nothing to flush
        }

        @Override
        public void close() {
            if (jedis != null) {
                try {
                    jedis.close();
                } catch (Exception e) {
                    LOG.warn("Error closing Redis connection", e);
                }
            }
        }
    }
}
