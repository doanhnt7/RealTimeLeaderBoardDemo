package jobs.operators;

import jobs.models.ScoreChangeEvent;
import jobs.models.Score;
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
 * Sink that processes ScoreChangeEvent objects and applies incremental updates to Redis sorted sets.
 * 
 * For INSERT events: adds the score and timestamp to their respective sorted sets
 * For DELETE events: removes the score and timestamp from their respective sorted sets
 */
public class ScoreChangeEventSink implements Sink<ScoreChangeEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(ScoreChangeEventSink.class);

    private final String redisHost;
    private final int redisPort;
    private final String redisScoreKey;
    private final String redisTimestampKey;

    /**
     * @param redisHost Redis host
     * @param redisPort Redis port
     * @param redisScoreKey Sorted set key for scores
     * 
     * The timestamp sorted set key will be redisScoreKey + ":ts"
     */
    public ScoreChangeEventSink(String redisHost, int redisPort, String redisScoreKey) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisScoreKey = redisScoreKey;
        this.redisTimestampKey = redisScoreKey + ":ts";
    }

    @Override
    public SinkWriter<ScoreChangeEvent> createWriter(WriterInitContext context) {
        return new RedisChangeEventWriter(redisHost, redisPort, redisScoreKey, redisTimestampKey);
    }

    private static class RedisChangeEventWriter implements SinkWriter<ScoreChangeEvent> {
        private final String redisHost;
        private final int redisPort;
        private final String redisScoreKey;
        private final String redisTimestampKey;
        private transient JedisPooled jedis;
        private transient BufferedWriter fileWriter;
        private transient Path logFilePath;

        public RedisChangeEventWriter(String redisHost, int redisPort, String redisScoreKey, String redisTimestampKey) {
            this.redisHost = redisHost;
            this.redisPort = redisPort;
            this.redisScoreKey = redisScoreKey;
            this.redisTimestampKey = redisTimestampKey;
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
            try {
                if (changeEvent.isDeleteAll()) {
                    // DELETEALL: Remove all scores and timestamps from the sorted sets
                    jedis.del(redisScoreKey);
                    jedis.del(redisTimestampKey);
                    LOG.debug("DELETEALL: Removed all scores from Redis key '{}' and all timestamps from '{}'", redisScoreKey, redisTimestampKey);
                } else {
                    Score scoreObj = changeEvent.getScore();
                    if (scoreObj == null) {
                        LOG.warn("ScoreChangeEvent has null score, skipping");
                        return;
                    }
                    String userId = scoreObj.getId();
                    double score = scoreObj.getScore();
                    long timestamp = scoreObj.getLastUpdateTime();

                    if (changeEvent.isInsert()) {
                        // INSERT: Add the score to the score sorted set and timestamp to the timestamp sorted set
                        jedis.zadd(redisScoreKey, score, userId);
                        jedis.zadd(redisTimestampKey, (double) timestamp, userId);
                        LOG.debug("INSERT: Added user {} with score {} to Redis key '{}', timestamp {} to '{}'",
                                userId, score, redisScoreKey, timestamp, redisTimestampKey);
                    } else if (changeEvent.isDelete()) {
                        // DELETE: Remove the score and timestamp from the sorted sets
                        long removedScore = jedis.zrem(redisScoreKey, userId);
                        long removedTs = jedis.zrem(redisTimestampKey, userId);
                        if (removedScore > 0 || removedTs > 0) {
                            LOG.debug("DELETE: Removed user {} from Redis keys '{}' (score) and '{}' (timestamp)",
                                    userId, redisScoreKey, redisTimestampKey);
                        } else {
                            LOG.warn("DELETE: User {} was not found in Redis keys '{}' or '{}'", userId, redisScoreKey, redisTimestampKey);
                        }
                    }
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
                LOG.error("Failed to process change event {} for Redis keys '{}', '{}'",
                        changeEvent, redisScoreKey, redisTimestampKey, e);
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
