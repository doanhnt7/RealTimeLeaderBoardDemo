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

Luồng data: app-python -> kafka -> flink -> snapshot mongoDB/ leaderboard redis

Luồng app-python -> mongoDB -> kafka qua debezium connector không còn được sử dụng

Có 5 leaderboard chính được tính toán:

- All-time theo level người dùng (Redis ZSET: `leaderboard_user_alltime`)
- Weekly highest level gained theo tuần (Redis ZSET:  `leaderboard_weekly_levels_gained:{year}:{week}`)
- Top-N user tăng level nhiều nhất trong 1 phút, cleanup user không có submission mới trong vòng 5 phút gần nhất, trigger cleanup mỗi 5 phút vào Redis (ZSET: `top_level_gainers_recent`), đồng thời snapshot định kỳ mỗi 7 phút sang MongoDB (`leaderboard.top_level_gainers_snapshots`)

- MVP theo team all-time (Redis Hash: `team_mvp`) — hiện đang comment trong `LeaderBoardBuilder.java`
- Top-N hot streaks gần đây (Redis ZSET: `top_hotstreaks_recent`) — hiện đang comment trong `LeaderBoardBuilder.java`
(Chỉ số này được thiết kế để làm nổi bật top 10 users đang có "chuỗi phong độ cao" (hot streak), nghĩa là hiệu suất ngắn hạn của họ đang vượt trội đáng kể so với trung bình lịch sử. Truy vấn này sử dụng cửa sổ thời gian trượt để tính trung bình ngắn hạn (total gain/total submission) (trong 10 giây) và trung bình dài hạn (trong 60 giây). Tỷ lệ giữa hai giá trị trung bình này xác định mức độ "nóng" của users.)

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

### 1. Yêu cầu cài đặt
Bạn cần cài docker desktop qua https://www.docker.com/products/docker-desktop/

### 2. Khởi động toàn bộ hệ thống

```bash
# Clone repository
git clone https://github.com/doanhnt7/RealTimeLeaderBoardDemo.git
cd RealTimeLeaderBoardDemo

# Khởi động tất cả services
docker-compose -f docker/docker-compose.yml up -d
```

Hiện tại đang disable luồng đẩy data vào mongoDB, data sẽ được đẩy thẳng vào kafka

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
mvn clean package -DskipTests

# Run test Flink job
cd flink-jobs
mvn test

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
## Các địa chỉ dịch vụ qua Cloudflare Tunnel

Hoặc bạn có thể truy cập các dịch vụ qua các URL đã được expose công khai (dùng Cloudflare Tunnel để expose localhost, chưa đẩy lên server):

- **Mongo Express**: [mongo-express.doanhnt.dpdns.org](https://mongo-express.doanhnt.dpdns.org)
- **Kafka UI**: [kafka-ui.doanhnt.dpdns.org](https://kafka-ui.doanhnt.dpdns.org)
- **Flink JobManager**: [jobmanager.doanhnt.dpdns.org](https://jobmanager.doanhnt.dpdns.org)
- **Grafana**: [grafana.doanhnt.dpdns.org](https://grafana.doanhnt.dpdns.org)
- **Spark UI**: [spark-ui.doanhnt.dpdns.org](https://spark-ui.doanhnt.dpdns.org)
- **Jupyter Lab**: [jupiter.doanhnt.dpdns.org](https://jupiter.doanhnt.dpdns.org)
- **Redis Insight**: [redis.doanhnt.dpdns.org](https://redis.doanhnt.dpdns.org)

---


