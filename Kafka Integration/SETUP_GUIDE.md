# Kafka Integration Setup Guide

## Phase 5: Kafka Integration with Spark + Iceberg

This guide walks you through setting up Kafka streaming alongside your existing Spark + Iceberg data lakehouse.

---

## Overview

The Kafka integration is **purely additive**:
- ✅ Existing MinIO bronze-layer writes continue unchanged
- ✅ Data is simultaneously streamed to Kafka topics
- ✅ If Kafka is unavailable, ingestion continues normally
- ✅ Zero disruption to your current pipeline

---

## Architecture

```
Data Sources (Postgres, MariaDB, S3)
         │
         ▼
   Ingestion Service
         │
         ├──> MinIO (Bronze Layer - Parquet) ✓ Existing
         │
         └──> Kafka Topics (Real-time Stream) ✓ NEW
                    │
                    ├──> bronze-postgres
                    ├──> bronze-mariadb
                    ├──> bronze-mariadb-cloud
                    ├──> bronze-mongodb
                    └──> bronze-s3
```

---

## Prerequisites

### 1. Install Kafka

**Windows (via Docker):**

```powershell
# Create docker-compose.yml
docker-compose up -d
```

**docker-compose.yml:**
```yaml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  kafka-connect:
    image: confluentinc/cp-kafka-connect:latest
    depends_on:
      - kafka
    ports:
      - "8083:8083"
    environment:
      CONNECT_BOOTSTRAP_SERVERS: kafka:9092
      CONNECT_REST_PORT: 8083
      CONNECT_GROUP_ID: "syniqai-connect"
      CONNECT_CONFIG_STORAGE_TOPIC: "connect-configs"
      CONNECT_OFFSET_STORAGE_TOPIC: "connect-offsets"
      CONNECT_STATUS_STORAGE_TOPIC: "connect-status"
      CONNECT_KEY_CONVERTER: "org.apache.kafka.connect.json.JsonConverter"
      CONNECT_VALUE_CONVERTER: "org.apache.kafka.connect.json.JsonConverter"
      CONNECT_INTERNAL_KEY_CONVERTER: "org.apache.kafka.connect.json.JsonConverter"
      CONNECT_INTERNAL_VALUE_CONVERTER: "org.apache.kafka.connect.json.JsonConverter"
      CONNECT_REST_ADVERTISED_HOST_NAME: "localhost"
```

### 2. Install Python Dependencies

```powershell
cd "Kafka Integration"
pip install -r requirements.txt
```

### 3. Verify Configuration

Check `Kafka Integration/.env`:
```dotenv
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_CONNECT_HOST=localhost
KAFKA_CONNECT_PORT=8083
KAFKA_DLQ_TOPIC=dlq-errors
KAFKA_MAX_ROWS_PER_MSG=500
```

---

## Installation Steps

### Step 1: Start Kafka Services

```powershell
# Start Kafka & Zookeeper
docker-compose up -d

# Verify Kafka is running
docker ps
```

You should see:
- `zookeeper` on port 2181
- `kafka` on port 9092
- `kafka-connect` on port 8083

### Step 2: Install Kafka Python Client

```powershell
pip install kafka-python requests python-dotenv
```

### Step 3: Start Backend

```powershell
cd gui
python api/backend.py
```

Look for these startup logs:
```
✓ Kafka Integration modules loaded
✓ Kafka bridge initialized (available=True)
✓ Kafka API routes mounted at /api/kafka
```

### Step 4: Verify Integration

**Check Kafka Status:**
```powershell
curl http://localhost:8000/api/kafka/health
```

Expected response:
```json
{
  "kafka_broker": {
    "status": "ok",
    "message": "Broker reachable"
  },
  "kafka_connect": {
    "status": "ok",
    "message": "Kafka Connect reachable",
    "version": "7.x.x"
  },
  "bootstrap_servers": "localhost:9092",
  "connect_url": "http://localhost:8083"
}
```

**Check Bridge Status:**
```powershell
curl http://localhost:8000/api/kafka/bridge
```

Expected response:
```json
{
  "available": true,
  "bootstrap_servers": "localhost:9092",
  "topics": [
    "bronze-postgres",
    "bronze-mariadb",
    "bronze-mariadb-cloud",
    "bronze-mongodb",
    "bronze-s3"
  ],
  "dlq_topic": "dlq-errors",
  "max_rows_per_message": 500
}
```

---

## Testing the Integration

### Test 1: Run a PostgreSQL Ingestion

1. Start a normal ingestion job from the GUI
2. Watch the backend logs for Kafka messages:
   ```
   KafkaBridge: [postgres] chunk=1 → 'bronze-postgres' (5000 rows, 10 msg(s))
   ```

### Test 2: Verify Data in Kafka

```powershell
# List all topics
curl http://localhost:8000/api/kafka/topics

# Sample messages from postgres topic
curl "http://localhost:8000/api/kafka/topics/bronze-postgres/sample?max_messages=5"
```

### Test 3: Check Message Format

Sample message structure:
```json
{
  "data": [
    {"id": 1, "name": "Alice", "age": 30},
    {"id": 2, "name": "Bob", "age": 25}
  ],
  "row_count": 2,
  "metadata": {
    "job_id": "abc123",
    "entity": "users",
    "domain": "finance",
    "minio_path": "finance/postgres/users/chunk_1_20260305_143022.parquet",
    "chunk_num": 1,
    "batch": 1,
    "total_rows_in_chunk": 5000,
    "published_at": "2026-03-05T14:30:22.123456+00:00",
    "source_type": "postgres"
  }
}
```

---

## API Endpoints

All Kafka endpoints are at `/api/kafka/*`:

### Status & Health
- `GET /api/kafka/status` - Dashboard summary
- `GET /api/kafka/health` - Broker + Connect health
- `GET /api/kafka/bridge` - In-process producer status

### Topics
- `GET /api/kafka/topics` - List all topics
- `GET /api/kafka/topics/{name}/sample` - Last N messages
- `POST /api/kafka/topics` - Create new topic

### Connectors (Debezium)
- `GET /api/kafka/connectors` - List all connectors
- `POST /api/kafka/connectors` - Register connector
- `POST /api/kafka/connectors/{name}/restart` - Restart
- `DELETE /api/kafka/connectors/{name}` - Delete

---

## Troubleshooting

### Issue: "kafka-python not installed"

**Solution:**
```powershell
pip install kafka-python
```

### Issue: "Cannot connect to Kafka"

**Check Kafka is running:**
```powershell
docker ps | findstr kafka
```

**Check connectivity:**
```powershell
telnet localhost 9092
```

### Issue: "Kafka publishing disabled"

This is **expected behavior** if Kafka is down. The pipeline continues normally with MinIO-only writes.

**Backend log:**
```
⚠ KafkaBridge: Cannot connect to Kafka (error details). 
  Kafka publishing is disabled — ingestion will continue normally.
```

### Issue: Topics not created automatically

Kafka creates topics on first publish. If you want to pre-create them:

```powershell
curl -X POST http://localhost:8000/api/kafka/topics \
  -H "Content-Type: application/json" \
  -d '{"name": "bronze-postgres", "num_partitions": 3, "replication_factor": 1}'
```

---

## Performance Tuning

### 1. Adjust Batch Size

In `Kafka Integration/.env`:
```dotenv
KAFKA_MAX_ROWS_PER_MSG=1000  # Increase for larger messages (default: 500)
```

### 2. Kafka Compression

The bridge uses `gzip` compression by default. To disable:

Edit [kafka_bridge.py](kafka_bridge.py#L110):
```python
compression_type=None,  # Change from "gzip"
```

### 3. Increase Timeout

If ingestion is slow, increase Kafka timeout:

Edit [kafka_bridge.py](kafka_bridge.py#L108):
```python
max_block_ms=10000,  # Increase from 5000 (10 seconds)
```

---

## Next Steps

1. **Set up Debezium CDC** (optional)
   - Capture real-time database changes
   - Stream to Kafka automatically

2. **Add Consumers**
   - Build Spark Streaming jobs to process Kafka topics
   - Real-time analytics on streaming data

3. **Monitor with Kafka UI**
   ```yaml
   kafka-ui:
     image: provectuslabs/kafka-ui:latest
     ports:
       - "8080:8080"
     environment:
       KAFKA_CLUSTERS_0_NAME: syniqai
       KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
   ```

---

## Integration Checklist

- [x] Kafka Integration folder created
- [x] Backend.py updated with Kafka imports and router
- [x] IngestionService updated to accept kafka_bridge
- [x] Kafka publishing added to all _ingest_* methods
- [x] Shutdown handler added for graceful Kafka close
- [ ] Kafka services started (Docker)
- [ ] Python dependencies installed
- [ ] Backend restarted
- [ ] Test ingestion job run
- [ ] Kafka topics verified

---

## Support

For issues or questions:
1. Check backend logs for Kafka-related warnings
2. Verify Kafka is running: `docker ps`
3. Test connectivity: `curl http://localhost:8000/api/kafka/health`
4. Review `.env` configuration

Remember: **Kafka integration is optional and non-blocking**. If Kafka is unavailable, your pipeline continues normally!
