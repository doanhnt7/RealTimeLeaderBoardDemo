# Real-Time Leaderboard Performance Demo

Dự án  leaderboard real-time sử dụng Apache Flink

## Kiến trúc

Dự án này mô phỏng một hệ thống leaderboard real-time với các thành phần chính:

- **Data Generator**: Python application tạo dữ liệu user submissions, có thể đẩy thẳng lên kafka hoặc đẩy vào mongoDB
- **MongoDB**: Lưu trữ dữ liệu gốc với Change Data Capture (CDC)
- **Debezium**: Capture changes từ MongoDB và publish vào Kafka
- **Apache Kafka**: Message broker cho real-time streaming
- **Apache Flink**: Stream processing engine cho real-time leaderboard
- **Apache Spark**: Batch processing cho analysis và comparison
- **Redis**: Cache cho leaderboard data
- **Monitoring**: Prometheus
- **Visulization**: Grafa

## Cấu trúc dự án

```
RealTimeLeaderBoardDemo/
├── app-python/                 # Python data generator
│   ├── _00_config.py          # Configuration settings
│   ├── _01_data_generator.py  # User data generator
│   ├── _02_kafka_producer.py  # Kafka producer utilities
│   ├── _03_realtime_producer.py # MongoDB producer
│   ├── _04_main.py            # Main entry point
│   ├── Dockerfile             # Python app container
│   └── requirements.txt       # Python dependencies
├── docker/                    # Docker configurations
│   ├── docker-compose.yml     # Main compose file
│   ├── docker-compose_mongoDB.yml # MongoDB-focused compose
│   ├── debezium-mongo-connector.json # Debezium config
│   ├── flink/                 # Flink container config
│   ├── prometheus/            # Prometheus config
│   └── grafana/               # Grafana dashboards
├── flink-jobs/                # Apache Flink streaming jobs
│   ├── src/main/java/jobs/    # Java source code
│   │   ├── LeaderBoardBuilder.java # Main Flink job
│   │   ├── models/            # Data models
│   │   ├── operators/         # Custom Flink operators
│   │   └── deserializer/      # Kafka deserializers
│   └── pom.xml               # Maven configuration
├── spark-jobs/                # Apache Spark batch jobs
│   ├── leaderboard_snapshot.ipynb # Jupyter notebook
│   ├── compare.ipynb          # Comparison notebook
│   └── requirements.txt       # Python dependencies
```

## 🚀 Cách chạy dự án

### 1. Khởi động toàn bộ hệ thống

```bash
# Clone repository
git clone https://github.com/doanhnt7/RealTimeLeaderBoardDemo.git
cd RealTimeLeaderBoardDemo

# Khởi động tất cả services
docker-compose -f docker/docker-compose.yml up -d
```

# Hiện tại đang disable luồng đẩy data vào mongoDB, data sẽ được đẩy thẳng vào kafka

### 3. Truy cập các giao diện

- **Flink Web UI**: http://localhost:8081
- **Kafka UI**: http://localhost:8080
- **MongoDB Express**: http://localhost:8082
- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **Jupyter Lab**: http://localhost:8888

## Cấu hình và tùy chỉnh

### Data Generator

```bash
# Chạy data generator với các tùy chọn
cd app-python

# Tạo dataset cố định (JSONL)
python _04_main.py generate-json --count 10000 --output fixed-dataset.jsonl

# Tạo dataset cố định (Parquet)
python _04_main.py generate-parquet --count 10000 --output fixed-dataset.parquet

# Replay data vào MongoDB
python _04_main.py replay-json --input fixed-dataset.jsonl --rate 10

# Replay data vào Kafka
python _04_main.py replay-parquet-kafka --input fixed-dataset.parquet --rate 10

# Chạy producer real-time
python _04_main.py start-producer --rate 10 --num-user 200 --num-app 1
```

### Flink Jobs

```bash
# Build Flink job
cd flink-jobs
mvn clean package

# Job sẽ tự động deploy khi container khởi động
# Sau đó submit manual qua Flink Web UI
# Flink job sẽ tạo snapshot leaderboard và đẩy ra mongoDB
# Có thể download database về để so sánh kết quả với sparkjob
```

### Spark Jobs

```bash
# Truy cập Jupyter Lab
# http://localhost:8888

# Chạy notebooks:
# - leaderboard_snapshot.ipynb: Tạo snapshot leaderboard
# - compare.ipynb: So sánh kết quả Flink vs Spark
```

---


