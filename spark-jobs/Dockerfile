FROM openjdk:11-jdk

# Set working directory
WORKDIR /app

# Install Python and system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create symlink for python
RUN ln -s /usr/bin/python3 /usr/bin/python

# Set JAVA_HOME
ENV JAVA_HOME=/usr/local/openjdk-11

# Install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Install Jupyter and notebook extensions
RUN pip3 install jupyter jupyterlab nbconvert

# Copy application files
COPY . /app/

# Create directories for data and results
RUN mkdir -p /app/data /app/results /app/leaderboard_snapshots

# Set permissions
RUN chmod -R 755 /app

# Expose Jupyter port
EXPOSE 8888

# Set environment variables
ENV PYTHONPATH=/app
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Create entrypoint script
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Use entrypoint script
ENTRYPOINT ["/app/entrypoint.sh"]