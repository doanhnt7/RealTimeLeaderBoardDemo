package flinkfintechpoc.test_jobs;

import flinkfintechpoc.models.EnrichedTransaction;
import flinkfintechpoc.models.Transaction;
import flinkfintechpoc.models.TransactionMetrics;
import flinkfintechpoc.processors.AccountEnrichmentProcessor;
import flinkfintechpoc.test_processor.CustomerTransactionMetricsRunningProcessor;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class CustomerTransactionMetricsRunningJob {

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
                .setGroupId("flink-running-metrics-group")
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
                .name("Customer Transaction Metrics (Running, Non-windowed)");

        runningMetrics
                .map(CustomerTransactionMetricsRunningJob::toJson)
                .disableChaining()
                .sinkTo(createKafkaSink("fintech.customer_transaction_metrics_running"))
                .name("Customer Transaction Metrics Running Output");

        env.execute("Customer Transaction Metrics Running Job");
    }

    private static KafkaSink<String> createKafkaSink(String topic) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("transaction.timeout.ms", "300000");
        // Enable producer-side compression to reduce on-disk topic size
        kafkaProps.put("compression.type", "lz4"); // alternatives: lz4, snappy, gzip
        // Optional batching tweaks for better compression ratio
        // kafkaProps.put("linger.ms", "20");
        
        // kafkaProps.put("batch.size", "32768");

        return KafkaSink.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setKafkaProducerConfig(kafkaProps)
                .setTransactionalIdPrefix(topic + "-tx")
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    private static String toJson(Object obj) {
        try {
            return JSON_MAPPER.writeValueAsString(obj);
        } catch (Exception e) {
            return obj.toString();
        }
    }
}


