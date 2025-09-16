package flinkfintechpoc.jobs;

import flinkfintechpoc.models.*;
import flinkfintechpoc.processors.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import java.util.Properties;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import java.time.Duration;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Flink Job for Real-time Fintech Analytics - 4 Core Processors
 * 
 * This job processes streaming data from Kafka topics and provides:
 * 1. Customer Lifecycle Analysis (Cascade Pattern: CustomerDataEnrichment + CustomerLifecycle)
 * 2. Merchant Performance Analysis (Merchant table) 
 * 3. Customer Transaction Metrics (Transaction table)
 * 4. Customer Fraud Detection (Transaction + Account tables)
 * 
 * Covers all major Flink techniques:
 * - State Management (ValueState, MapState, ListState)
 * - Event Time Processing (Watermarks, Event time windows)
 * - Complex Event Processing (CEP patterns)
 * - Broadcast State Pattern (Reference data enrichment)
 * - Windowed Processing (Tumbling, Sliding, Session windows)
 * - Custom Triggers and Functions
 * - Stream Joins and Side Inputs
 * - Cascade Pattern for Multi-step Enrichment
 */
public class FintechAnalyticsJob {
    
    // Kafka topics - Only streaming topics (transaction and customer_session)
    private static final String TRANSACTIONS_TOPIC = "fintech.public.transactions";
    private static final String CUSTOMER_SESSIONS_TOPIC = "fintech.public.customer_sessions";
    
    // Static reference data will be read from PostgreSQL
    
    // JSON ObjectMapper for serialization
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    
    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Debug: Print current parallelism before setting
        System.out.println("=== FLINK PARALLELISM DEBUG ===");
        System.out.println("Default parallelism before setting: " + env.getParallelism());
        System.out.println("Environment type: " + env.getClass().getSimpleName());
        
        // Set parallelism explicitly to ensure it's 4
        env.setParallelism(4);
        
        // Debug: Print parallelism after setting
        System.out.println("Parallelism after setting: " + env.getParallelism());
        System.out.println("=================================");
        
        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(10000);
        
        // Create Kafka sources for streaming data only
        KafkaSource<Transaction> transactionSource = createJsonKafkaSource(TRANSACTIONS_TOPIC, Transaction.class);
        KafkaSource<CustomerSession> customerSessionSource = createJsonKafkaSource(CUSTOMER_SESSIONS_TOPIC, CustomerSession.class);

        // Create streaming data streams from Kafka with Event Time based on updatedAt
        DataStream<Transaction> transactionStream = env
            .fromSource(transactionSource, 
                WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((event, timestamp) -> 
                        event.getUpdatedAt() != null ? event.getUpdatedAt().getTime() : System.currentTimeMillis()), 
                "Transactions");
        
        DataStream<CustomerSession> customerSessionStream = env
            .fromSource(customerSessionSource, WatermarkStrategy.noWatermarks(), "Customer Sessions");
        
        // Note: Reference data (customers, accounts, merchants) will be loaded directly 
        // in processor open() methods to avoid race conditions and simplify architecture
        
        // ============================================================================
        // 4 CORE PROCESSORS - Covering All Flink Techniques
        // ============================================================================
        
        // Prepare filtered streams to avoid null keys
        DataStream<Transaction> transactionsByAccount = transactionStream
            .filter(t -> t.getFromAccountId() != null)
            .name("Filter Transactions with fromAccountId");
        DataStream<Transaction> transactionsByMerchant = transactionStream
            .filter(t -> t.getMerchantId() != null)
            .name("Filter Transactions with merchantId");

        // 1. CASCADE PATTERN - Customer Lifecycle Analysis (Approach A)
        // Step 1: Transaction + Account → EnrichedTransaction (obtain customerId)
        DataStream<EnrichedTransaction> enrichedByAccount = transactionsByAccount
            .keyBy(Transaction::getFromAccountId)
            .process(new flinkfintechpoc.processors.AccountEnrichmentProcessor())
            .name("Account Enrichment");

        // Step 2: EnrichedTransaction + Customer → EnrichedTransaction (add customer profile fields)
        DataStream<EnrichedTransaction> enrichedByCustomer = enrichedByAccount
            .filter(t -> t.getCustomerId() != null) // Filter out transactions without customerId
            .keyBy(EnrichedTransaction::getCustomerId)
            .process(new flinkfintechpoc.processors.CustomerProfileEnrichmentProcessor())
            .name("Customer Profile Enrichment");

        // Step 3: EnrichedTransaction + CustomerSession → CustomerLifecycleMetrics
        DataStream<CustomerLifecycleMetrics> customerLifecycleStream = enrichedByCustomer
            .keyBy(EnrichedTransaction::getCustomerId)
            .connect(customerSessionStream.broadcast(CustomerLifecycleProcessor.SESSION_STATE_DESCRIPTOR))
            .process(new CustomerLifecycleProcessor())
            .name("Customer Lifecycle Analysis");
        
        // 2. MERCHANT PERFORMANCE PROCESSOR - Transaction + Merchant data
        // Techniques: Keyed Process Function, State Management, Windowed Aggregations
        DataStream<MerchantAnalyticsMetrics> merchantPerformanceStream = transactionsByMerchant
            .keyBy(Transaction::getMerchantId)
            .process(new MerchantPerformanceProcessor())
            .name("Merchant Performance Analysis");
        
        // 3. CUSTOMER TRANSACTION METRICS PROCESSOR - use enrichedByAccount so we have customerId
        // Key by customerId, then map to Transaction to reuse existing aggregator/window function
        DataStream<TransactionMetrics> customerTransactionMetricsStream = enrichedByAccount
            .keyBy(EnrichedTransaction::getCustomerId)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
            .aggregate(new flinkfintechpoc.processors.EnrichedTransactionAggregator(), new flinkfintechpoc.processors.CustomerTransactionMetricsProcessor())
            .name("Customer Transaction Metrics Analysis");
        
        // 4. CUSTOMER FRAUD DETECTION PROCESSOR - EnrichedTransaction with customer profile
        // Techniques: Complex Event Processing, State Management, Pattern Detection
        DataStream<FraudDetectionResult> customerFraudDetectionStream = enrichedByCustomer
            .keyBy(EnrichedTransaction::getCustomerId)
            .process(new flinkfintechpoc.processors.CustomerFraudDetectionWithProfileProcessor())
            .filter(result -> result != null && result.isFraudulent())
            .name("Customer Fraud Detection");
        
        // ============================================================================
        // OUTPUT STREAMS - Analytics Results
        // ============================================================================
        
        // 1. Customer Lifecycle Metrics - Customer analytics & lifecycle events
        customerLifecycleStream
            .map(metrics -> convertToJson(metrics))
            .sinkTo(createKafkaSink("fintech.customer_lifecycle"))
            .name("Customer Lifecycle Output");
        
        // 2. Merchant Performance - Merchant performance & business insights
        merchantPerformanceStream
            .map(metrics -> convertToJson(metrics))
            .sinkTo(createKafkaSink("fintech.merchant_performance"))
            .name("Merchant Performance Output");
        
        // 3. Customer Transaction Metrics - Core transaction analytics & dashboard metrics
        customerTransactionMetricsStream
            .map(metrics -> convertToJson(metrics))
            .sinkTo(createKafkaSink("fintech.customer_transaction_metrics"))
            .name("Customer Transaction Metrics Output");
        
        // 4. Customer Fraud Detection Alerts - Real-time security monitoring
        customerFraudDetectionStream
            .map(alert -> convertToJson(alert))
            .sinkTo(createKafkaSink("fintech.customer_fraud_alerts"))
            .name("Customer Fraud Detection Output");
        
        // Execute the job
        env.execute("Fintech Real-time Analytics Job - 4 Core Processors");
    }
    
    
    private static <T> KafkaSource<T> createJsonKafkaSource(String topic, Class<T> cls) {
        return KafkaSource.<T>builder()
            .setBootstrapServers("kafka:29092")
            .setTopics(topic)
            .setGroupId("flink-analytics-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new JsonDeserializationSchema<>(cls))
            .build();
    }
    
    private static KafkaSink<String> createKafkaSink(String topic) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("transaction.timeout.ms", "300000"); // 5 minutes

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
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .build();
    }
    
    /**
     * Convert object to JSON string for ClickHouse JSONEachRow format
     */
    private static String convertToJson(Object obj) {
        try {
            return JSON_MAPPER.writeValueAsString(obj);
        } catch (Exception e) {
            // Fallback to toString if JSON serialization fails
            System.err.println("JSON serialization failed for object: " + obj.getClass().getName() + ", error: " + e.getMessage());
            return obj.toString();
        }
    }
}