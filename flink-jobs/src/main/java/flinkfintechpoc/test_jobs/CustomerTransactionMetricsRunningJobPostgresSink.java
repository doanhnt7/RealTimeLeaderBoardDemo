package flinkfintechpoc.test_jobs;

import flinkfintechpoc.models.EnrichedTransaction;
import flinkfintechpoc.models.Transaction;
import flinkfintechpoc.models.TransactionMetrics;
import flinkfintechpoc.processors.AccountEnrichmentProcessor;
import flinkfintechpoc.test_processor.CustomerTransactionMetricsRunningProcessor;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSinkBuilder;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Timestamp;
import java.util.Map;

public class CustomerTransactionMetricsRunningJobPostgresSink {

    private static final String TRANSACTIONS_TOPIC = "test_performance";

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setParallelism(1);

        KafkaSource<Transaction> transactionSource = KafkaSource.<Transaction>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics(TRANSACTIONS_TOPIC)
                .setGroupId("flink-running-metrics-postgres-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(Transaction.class))
                .build();

        DataStream<Transaction> transactionStream = env
                .fromSource(transactionSource,
                        WatermarkStrategy.noWatermarks(),
                        "Transactions");
                
        DataStream<EnrichedTransaction> enrichedByAccount = transactionStream
                .filter(t -> t.getFromAccountId() != null)
                .keyBy(Transaction::getFromAccountId)
                .process(new AccountEnrichmentProcessor())
                .name("Account Enrichment");

        DataStream<TransactionMetrics> runningMetrics = enrichedByAccount
                .filter(t -> t.getCustomerId() != null)
                .keyBy(EnrichedTransaction::getCustomerId)
                .process(new CustomerTransactionMetricsRunningProcessor())
                .name("Customer Transaction Metrics (Running, Non-windowed)")
                .disableChaining();
        
        // Create PostgreSQL sink using JdbcSink
        runningMetrics
                .sinkTo(createPostgresSink())
                .slotSharingGroup("postgres-sink")
                .name("Customer Transaction Metrics PostgreSQL Sink");

        env.execute("Customer Transaction Metrics Running Job (PostgreSQL Sink)");
    }

    private static JdbcSink<TransactionMetrics> createPostgresSink() {
        // SQL statement for upsert (insert or update on conflict)
        String upsertSQL = "INSERT INTO customer_transaction_metrics_running " +
                "(customer_id, account_id, event_time, total_amount, transaction_count, " +
                "average_amount, min_amount, max_amount, preferred_transaction_type, " +
                "preferred_location, preferred_device, transaction_velocity, risk_score, " +
                "transaction_type_counts, location_counts, device_counts, " +
                "recent_transaction_count, last_transaction_amount, last_transaction_type, " +
                "created_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (customer_id) " +
                "DO UPDATE SET " +
                "account_id = EXCLUDED.account_id, " +
                "event_time = EXCLUDED.event_time, " +
                "total_amount = EXCLUDED.total_amount, " +
                "transaction_count = EXCLUDED.transaction_count, " +
                "average_amount = EXCLUDED.average_amount, " +
                "min_amount = EXCLUDED.min_amount, " +
                "max_amount = EXCLUDED.max_amount, " +
                "preferred_transaction_type = EXCLUDED.preferred_transaction_type, " +
                "preferred_location = EXCLUDED.preferred_location, " +
                "preferred_device = EXCLUDED.preferred_device, " +
                "transaction_velocity = EXCLUDED.transaction_velocity, " +
                "risk_score = EXCLUDED.risk_score, " +
                "transaction_type_counts = EXCLUDED.transaction_type_counts, " +
                "location_counts = EXCLUDED.location_counts, " +
                "device_counts = EXCLUDED.device_counts, " +
                "recent_transaction_count = EXCLUDED.recent_transaction_count, " +
                "last_transaction_amount = EXCLUDED.last_transaction_amount, " +
                "last_transaction_type = EXCLUDED.last_transaction_type";

        JdbcExecutionOptions jdbcExeOption = JdbcExecutionOptions.builder()
                .withBatchSize(100)
                .withBatchIntervalMs(200)
                .withMaxRetries(3)
                .build();
                
        JdbcConnectionOptions jdbcConnOption = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:postgresql://postgres:5432/fintech_demo")
                .withDriverName("org.postgresql.Driver")
                .withUsername("postgres")
                .withPassword("postgres")
                .build();
                
        return new JdbcSinkBuilder<TransactionMetrics>()
                .withQueryStatement(
                        upsertSQL,
                        (statement, metrics) -> {
                            statement.setString(1, metrics.getCustomerId());
                            statement.setString(2, metrics.getAccountId());
                            statement.setTimestamp(3, new Timestamp(metrics.getEventTime().getTime()));
                            statement.setBigDecimal(4, metrics.getTotalAmount());
                            statement.setInt(5, metrics.getTransactionCount());
                            statement.setBigDecimal(6, metrics.getAverageAmount());
                            statement.setBigDecimal(7, metrics.getMinAmount());
                            statement.setBigDecimal(8, metrics.getMaxAmount());
                            statement.setString(9, metrics.getPreferredTransactionType());
                            statement.setString(10, metrics.getPreferredLocation());
                            statement.setString(11, metrics.getPreferredDevice());
                            statement.setDouble(12, metrics.getTransactionVelocity());
                            statement.setDouble(13, metrics.getRiskScore());
                            
                            // Serialize Map fields to JSON
                            statement.setString(14, mapToJson(metrics.getTransactionTypeCounts()));
                            statement.setString(15, mapToJson(metrics.getLocationCounts()));
                            statement.setString(16, mapToJson(metrics.getDeviceCounts()));
                            
                            statement.setInt(17, metrics.getRecentTransactionCount());
                            statement.setBigDecimal(18, metrics.getLastTransactionAmount());
                            statement.setString(19, metrics.getLastTransactionType());
                            
                            // Set created_at to current time for sink latency measurement
                            Timestamp now = new Timestamp(System.currentTimeMillis());
                            statement.setTimestamp(20, now); // created_at
                            // updated_at will be set by PostgreSQL DEFAULT CURRENT_TIMESTAMP
                        })
                .withExecutionOptions(jdbcExeOption)
                .buildAtLeastOnce(jdbcConnOption);
    }

    private static String mapToJson(Map<String, Integer> map) {
        if (map == null || map.isEmpty()) {
            return "{}";
        }
        try {
            return JSON_MAPPER.writeValueAsString(map);
        } catch (Exception e) {
            return "{}";
        }
    }
}
