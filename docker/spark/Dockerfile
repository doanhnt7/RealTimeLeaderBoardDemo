FROM bitnami/spark:3.5.0

# Switch to root to install packages
USER root

# Install Python dependencies
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.7 \
    pyspark==3.5.0

# Switch back to spark user
USER spark

# Set working directory
WORKDIR /opt/spark/jobs

# Set environment variables
ENV PYTHONPATH=/opt/spark/jobs
ENV SPARK_MASTER=spark://spark-master:7077
