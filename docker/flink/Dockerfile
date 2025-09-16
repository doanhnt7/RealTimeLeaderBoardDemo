FROM flink:2.0.0

# Tạo thư mục cho user-provided JARs
RUN mkdir -p /opt/flink/usrlib

# Tải Kafka connector JAR vào thư mục usrlib
RUN wget -P /opt/flink/usrlib \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/4.0.0-2.0/flink-connector-kafka-4.0.0-2.0.jar

RUN wget -P /opt/flink/usrlib \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-metrics-prometheus/2.0.0/flink-metrics-prometheus-2.0.0.jar


