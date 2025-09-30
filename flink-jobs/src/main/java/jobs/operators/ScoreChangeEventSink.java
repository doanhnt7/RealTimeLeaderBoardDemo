package jobs.operators;

import jobs.models.ScoreChangeEvent;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
        private transient BufferedWriter fileWriter;
        private transient Path logFilePath;

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

            if (fileWriter == null) {
                try {
                    Path targetPath = Paths.get("/opt/flink/jobs/score-change.txt");
                    Path parentDir = targetPath.getParent();
                    if (parentDir != null) {
                        Files.createDirectories(parentDir);
                    }
                    logFilePath = targetPath.toAbsolutePath();
                    fileWriter = Files.newBufferedWriter(
                        logFilePath,
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND
                    );
                } catch (IOException ioException) {
                    LOG.warn("Failed to initialize file writer for {}", "/opt/flink/jobs/score-change.txt", ioException);
                }
            }

            if (changeEvent == null) {
                LOG.warn("Received null change event or score, skipping");
                return;
            }

            // Log the entire change event for debugging/auditing
            LOG.info("Processing ScoreChangeEvent: {}", changeEvent);

            String userId = changeEvent.getScore().getId();
            double score = changeEvent.getScore().getScore();
            try {
                if (changeEvent.isInsert()) {
                    // INSERT: Add the score to the sorted set
                    jedis.zadd(redisKey, score, userId);
                    LOG.debug("INSERT: Added user {} with score {} to Redis key '{}'", 
                        userId, score, redisKey);
                        
                } else if (changeEvent.isDelete()) {
                    // DELETE: Remove the score from the sorted set
                    long removed = jedis.zrem(redisKey, userId);
                    if (removed > 0) {
                        LOG.debug("DELETE: Removed user {} from Redis key '{}'", 
                            userId, redisKey);
                    } else {
                        LOG.warn("DELETE: User {} was not found in Redis key '{}'", userId, redisKey);
                    }
                }
                else if (changeEvent.isDeleteAll()) {
                    // DELETEALL: Remove all scores from the sorted set
                    jedis.zremrangeByRank(redisKey, 0, -1);
                    LOG.debug("DELETEALL: Removed all scores from Redis key '{}'", redisKey);
                }
                // Append only the change event string to the TXT file
                if (fileWriter != null) {
                    String line = changeEvent.toString() + System.lineSeparator();
                    try {
                        fileWriter.write(line);
                        fileWriter.flush();
                    } catch (IOException ioException) {
                        LOG.warn("Failed to write event to file {}", logFilePath, ioException);
                    }
                }
            } catch (Exception e) {
                LOG.error("Failed to process change event {} for Redis key '{}'", 
                    changeEvent, redisKey, e);
            }
        }

        @Override
        public void flush(boolean endOfInput) {
            if (fileWriter != null) {
                try {
                    fileWriter.flush();
                } catch (IOException e) {
                    LOG.warn("Error flushing file writer", e);
                }
            }
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
            if (fileWriter != null) {
                try {
                    fileWriter.flush();
                    fileWriter.close();
                } catch (IOException e) {
                    LOG.warn("Error closing file writer", e);
                }
            }
        }
    }
}
