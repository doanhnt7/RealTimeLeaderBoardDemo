# Flink Fintech Analytics Job

Flink job để xử lý dữ liệu fintech realtime và tạo dashboard analytics bằng Grafana.

## Tính năng chính

### 1. Real-time Transaction Processing
- Xử lý giao dịch realtime từ Kafka topics
- Deserialization JSON thành Java objects
- Stream processing với windowing functions

### 2. Fraud Detection & Transaction Analytics
- Phát hiện gian lận realtime
- Risk scoring và pattern analysis
- Suspicious transaction detection
- Geographic và device anomalies
- Transaction metrics và patterns
- Amount analysis và frequency

### 3. Risk Analytics
- Phân tích rủi ro theo customer
- Risk scoring và classification
- Fraud probability calculation
- Risk factor identification

### 4. Customer Behavior Analysis
- Phân tích hành vi khách hàng
- Customer segmentation
- Transaction pattern analysis
- Engagement scoring

### 5. Real-time Dashboard Metrics
- Metrics realtime cho Grafana
- Transaction counts và amounts
- Fraud detection rates và risk scores
- Geographic và merchant analytics

### 6. Anomaly Detection
- Phát hiện bất thường realtime
- Pattern-based anomaly detection
- Confidence scoring
- Alert generation

## Cấu trúc dự án

```
flink-jobs/
├── src/main/java/flinkfintechpoc/
│   ├── jobs/
│   │   └── FintechAnalyticsJob.java          # Main Flink job
│   ├── models/                               # Data models
│   │   ├── Transaction.java
│   │   ├── Customer.java
│   │   ├── Account.java
│   │   ├── Merchant.java
│   │   ├── CustomerSession.java
│   │   ├── OutboxEvent.java
│   │   ├── TransactionMetrics.java
│   │   ├── CustomerBehaviorMetrics.java
│   │   ├── DashboardMetrics.java
│   │   ├── RiskAnalytics.java
│   │   ├── AnomalyAlert.java
│   │   ├── GeographicMetrics.java
│   │   ├── MerchantMetrics.java
│   │   ├── TimePatternMetrics.java
│   │   └── ComplianceMetrics.java
│   ├── deserializers/                        # JSON deserializers
│   │   ├── TransactionDeserializer.java
│   │   ├── CustomerDeserializer.java
│   │   ├── AccountDeserializer.java
│   │   ├── MerchantDeserializer.java
│   │   ├── CustomerSessionDeserializer.java
│   │   └── EventDeserializer.java
│   └── processors/                           # Stream processors
│       ├── TransactionMetricsProcessor.java
│       ├── CustomerBehaviorProcessor.java
│       ├── RiskAnalyticsProcessor.java
│       ├── DashboardMetricsProcessor.java
│       ├── AnomalyDetectionFunction.java
│       ├── GeographicMetricsProcessor.java
│       ├── MerchantMetricsProcessor.java
│       ├── TimePatternProcessor.java
│       └── ComplianceProcessor.java
├── pom.xml                                   # Maven dependencies
└── README.md                                 # This file
```

## Kafka Topics

### Input Topics
- `fintech.transactions` - Giao dịch từ app-python
- `fintech.customers` - Thông tin khách hàng
- `fintech.fraud_alerts` - Cảnh báo gian lận
- `fintech.events` - Events từ outbox pattern

### Output Topics (cho Grafana)
- `fintech.realtime_metrics` - Metrics realtime
- `fintech.fraud_detection` - Kết quả fraud detection
- `fintech.customer_analytics` - Analytics khách hàng

## Cài đặt và chạy

### 1. Prerequisites
- Java 11+
- Maven 3.6+
- Apache Flink 1.18+
- Apache Kafka
- PostgreSQL với Debezium

### 2. Build project
```bash
cd flink-jobs
mvn clean package
```

### 3. Chạy Flink job
```bash
# Local execution
mvn exec:java -Dexec.mainClass="flinkfintechpoc.jobs.FintechAnalyticsJob"

# Submit to Flink cluster
flink run target/flink-jobs-1.0-SNAPSHOT.jar
```

### 4. Environment variables
```bash
# Kafka configuration
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_GROUP_ID=flink-fintech-group

# Flink configuration
export FLINK_PARALLELISM=4
export FLINK_CHECKPOINT_INTERVAL=10000
```

## Cấu hình Flink

### Checkpointing
- Checkpoint interval: 10 seconds
- Checkpoint timeout: 5 minutes
- Minimum pause between checkpoints: 2 seconds

### Parallelism
- Default parallelism: 4
- State backend: RocksDB (recommended for production)
- Backpressure handling: enabled

### Watermarking
- Watermark strategy: no watermarks (for real-time processing)
- Event time processing: enabled
- Late data handling: side output

## Monitoring và Metrics

### Flink Web UI
- Job status và metrics
- Task manager status
- Checkpoint information
- Backpressure monitoring

### Custom Metrics
- Transaction processing rate
- Fraud detection accuracy
- Processing latency
- Error rates

### Logging
- Structured logging với SLF4J
- Log levels: INFO, DEBUG, ERROR
- Performance metrics logging

## Tích hợp với Grafana

### Data Sources
- Kafka topics cho real-time data
- PostgreSQL cho historical data
- InfluxDB cho time-series data (optional)

### Dashboard Panels
1. **Real-time Transactions**
   - Transaction count per minute
   - Total amount per minute
   - Transaction types distribution

2. **Fraud Detection & Risk Monitoring**
- Fraud alerts per minute
- Fraud detection rate trends
- Risk score distribution
- Suspicious transaction patterns

3. **Customer Analytics**
   - Customer segments
   - Transaction patterns
   - Risk profiles

4. **Geographic Analysis**
   - Transaction by country
   - Fraud hotspots
   - Geographic risk patterns

5. **Performance Metrics**
   - Processing latency
   - Throughput
   - Error rates

## Production Deployment

### Docker
```dockerfile
FROM flink:1.18-java11
COPY target/flink-jobs-1.0-SNAPSHOT.jar /opt/flink/usrlib/
```

### Kubernetes
- Flink operator deployment
- ConfigMaps cho configuration
- Secrets cho credentials
- Horizontal pod autoscaling

### Monitoring
- Prometheus metrics
- Grafana dashboards
- Alerting rules
- Log aggregation

## Troubleshooting

### Common Issues
1. **Kafka connection errors**: Kiểm tra Kafka bootstrap servers
2. **State backend errors**: Kiểm tra RocksDB configuration
3. **Memory issues**: Tăng heap size và off-heap memory
4. **Checkpoint failures**: Kiểm tra storage và network

### Debug Mode
```bash
# Enable debug logging
export FLINK_LOG_LEVEL=DEBUG

# Enable Flink debug mode
export FLINK_DEBUG=true
```

### Performance Tuning
- Tăng parallelism cho high-throughput
- Optimize state backend configuration
- Tune checkpoint intervals
- Monitor backpressure

## Mở rộng

### Thêm Processors mới
1. Tạo model class trong `models/`
2. Tạo processor trong `processors/`
3. Thêm vào main job
4. Cập nhật metrics và monitoring

### Custom Rules
1. Extend `FraudDetectionFunction`
2. Implement custom detection logic
3. Add configuration parameters
4. Update alerting rules

### Data Sinks
1. Elasticsearch cho search
2. Redis cho caching
3. External APIs cho notifications
4. Database sinks cho persistence

## Support

- Flink Documentation: https://flink.apache.org/docs/
- Kafka Documentation: https://kafka.apache.org/documentation/
- Grafana Documentation: https://grafana.com/docs/
