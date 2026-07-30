# Dockerfile for Spark CDC Consumer
FROM python:3.11-slim

# Install Java 21 (required for Spark)
RUN apt-get update && \
    apt-get install -y openjdk-21-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Set working directory
WORKDIR /app

# Copy requirements first (for better caching)
COPY requirements-cdc.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements-cdc.txt

# Copy CDC consumer script
COPY spark_cdc_consumer.py .
COPY debezium_manager.py .

# Create directories
RUN mkdir -p /app/checkpoints /app/logs

# Expose ports (if needed for monitoring)
EXPOSE 4040

# Run CDC consumer
CMD ["python", "spark_cdc_consumer.py", "--checkpoint", "/app/checkpoints"]
