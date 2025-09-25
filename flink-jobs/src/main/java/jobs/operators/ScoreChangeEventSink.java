package jobs.operators;

import jobs.models.ScoreChangeEvent;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import redis.clients.jedis.JedisPooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sink that processes ScoreChangeEvent objects and applies incremental updates to Redis sorted set.
 * 
 * For INSERT events: adds the score to the sorted set
 * For DELETE events: removes the score from the sorted set
 */
public class ScoreChangeEventSink implements Sink<ScoreChangeEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(ScoreChangeEventSink.class);
    
    private final String redisHost;
    private final int redisPort;
    private final String redisKey;

    public ScoreChangeEventSink(String redisHost, int redisPort, String redisKey) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisKey = redisKey;
    }

    @Override
    public SinkWriter<ScoreChangeEvent> createWriter(WriterInitContext context) {
        return new RedisChangeEventWriter(redisHost, redisPort, redisKey);
    }

    private static class RedisChangeEventWriter implements SinkWriter<ScoreChangeEvent> {
        private final String redisHost;
        private final int redisPort;
        private final String redisKey;
        private transient JedisPooled jedis;

        public RedisChangeEventWriter(String redisHost, int redisPort, String redisKey) {
            this.redisHost = redisHost;
            this.redisPort = redisPort;
            this.redisKey = redisKey;
        }

        @Override
        public void write(ScoreChangeEvent changeEvent, Context context) {
            if (jedis == null) {
                jedis = new JedisPooled(redisHost, redisPort);
            }
            
            if (changeEvent == null || changeEvent.getScore() == null) {
                LOG.warn("Received null change event or score, skipping");
                return;
            }
            
            String userId = changeEvent.getScore().getId();
            double score = changeEvent.getScore().getScore();
            int rank = changeEvent.getRank();
            
            try {
                if (changeEvent.isInsert()) {
                    // INSERT: Add the score to the sorted set
                    jedis.zadd(redisKey, score, userId);
                    LOG.debug("INSERT: Added user {} with score {} to Redis key '{}' at rank {}", 
                        userId, score, redisKey, rank);
                        
                } else if (changeEvent.isDelete()) {
                    // DELETE: Remove the score from the sorted set
                    long removed = jedis.zrem(redisKey, userId);
                    if (removed > 0) {
                        LOG.debug("DELETE: Removed user {} from Redis key '{}' from rank {}", 
                            userId, redisKey, rank);
                    } else {
                        LOG.warn("DELETE: User {} was not found in Redis key '{}'", userId, redisKey);
                    }
                }
                
                // Optional: Set expiration time for the key (e.g., 1 hour)
                jedis.expire(redisKey, 3600);
                
            } catch (Exception e) {
                LOG.error("Failed to process change event {} for Redis key '{}'", 
                    changeEvent, redisKey, e);
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
