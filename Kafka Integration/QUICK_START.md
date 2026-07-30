# 🚀 Kafka Integration - Quick Start

**Phase 5: Real-time Streaming with Kafka**

This guide gets you up and running with Kafka integration in **5 minutes**.

---

## What You Get

✅ **Real-time data streaming** alongside your existing MinIO pipeline  
✅ **Zero disruption** - continues working even if Kafka is down  
✅ **Full observability** - monitor topics, messages, and connectors via REST API  
✅ **Kafka UI** - visual dashboard at http://localhost:8080  

---

## Quick Start (3 Commands)

### 1️⃣ Start Kafka Services

```powershell
cd "Kafka Integration"
.\start_kafka.ps1
```

**This starts:**
- Kafka Broker (port 9092)
- Kafka Connect (port 8083)
- Kafka UI (port 8080)
- Zookeeper (port 2181)

### 2️⃣ Start Backend

```powershell
cd ..\gui
python api\backend.py
```

**Look for these logs:**
```
✓ Kafka Integration modules loaded
✓ Kafka bridge initialized (available=True)
✓ Kafka API routes mounted at /api/kafka
```

### 3️⃣ Test Integration

```powershell
cd "..\Kafka Integration"
python test_kafka_integration.py
```

**Expected output:**
```
===================================================================
  Test Summary
===================================================================
✓ Backend Health
✓ Kafka Health
✓ Bridge Status
✓ List Topics
✓ Dashboard Status

5/5 tests passed
🎉 All tests passed! Kafka integration is working correctly.
```

---

## What Happens During Ingestion?

When you run a data ingestion job from the GUI:

### Before (Phase 4)
```
PostgreSQL → Extract → Parquet → MinIO
```

### After (Phase 5)
```
PostgreSQL → Extract → ┬─> Parquet → MinIO  ✓ Unchanged
                        └─> Kafka Topic      ✓ NEW!
```

**Data flows to BOTH destinations simultaneously!**

---

## Verify It's Working

### Check Kafka Status
```powershell
curl http://localhost:8000/api/kafka/status
```

### List Available Topics
```powershell
curl http://localhost:8000/api/kafka/topics
```

Expected topics:
- `bronze-postgres`
- `bronze-mariadb`
- `bronze-mariadb-cloud`
- `bronze-mongodb`
- `bronze-s3`
- `dlq-errors` (dead letter queue)

### Sample Messages from a Topic
```powershell
curl "http://localhost:8000/api/kafka/topics/bronze-postgres/sample?max_messages=5"
```

### Open Kafka UI
```
http://localhost:8080
```

Browse topics, view messages, and monitor connector status visually!

---

## Message Format Example

When you ingest PostgreSQL data, Kafka receives messages like this:

```json
{
  "data": [
    {"id": 1, "name": "Alice", "amount": 150.00},
    {"id": 2, "name": "Bob", "amount": 250.00}
  ],
  "row_count": 2,
  "metadata": {
    "job_id": "abc123",
    "entity": "transactions",
    "domain": "finance",
    "minio_path": "finance/postgres/transactions/chunk_1_20260305.parquet",
    "chunk_num": 1,
    "batch": 1,
    "published_at": "2026-03-05T14:30:22Z",
    "source_type": "postgres"
  }
}
```

---

## REST API Endpoints

All endpoints available at `/api/kafka/*`:

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/kafka/status` | Full dashboard summary |
| `GET` | `/api/kafka/health` | Broker + Connect health |
| `GET` | `/api/kafka/bridge` | In-process producer status |
| `GET` | `/api/kafka/topics` | List all topics |
| `GET` | `/api/kafka/topics/{name}/sample` | View recent messages |
| `POST` | `/api/kafka/topics` | Create new topic |
| `GET` | `/api/kafka/connectors` | List Debezium connectors |

**Full API docs:** [SETUP_GUIDE.md](SETUP_GUIDE.md#api-endpoints)

---

## Troubleshooting

### ❌ "Cannot connect to Kafka"

**Check if Docker is running:**
```powershell
docker ps | findstr kafka
```

**Start services if not running:**
```powershell
cd "Kafka Integration"
docker-compose up -d
```

### ❌ "kafka-python not installed"

```powershell
pip install kafka-python
```

### ⚠️ "Kafka publishing disabled"

**This is normal if Kafka is down.** Your pipeline continues with MinIO-only writes.

Backend will log:
```
⚠ KafkaBridge: Cannot connect to Kafka.
  Kafka publishing is disabled — ingestion will continue normally.
```

**Solution:** Start Kafka services, then restart the backend.

---

## Stop Kafka Services

```powershell
cd "Kafka Integration"
docker-compose down
```

This stops all Kafka containers but keeps your data.

To remove data volumes too:
```powershell
docker-compose down -v
```

---

## File Structure

```
Kafka Integration/
├── __init__.py                  # Package exports
├── kafka_bridge.py              # Core: publishes to Kafka
├── kafka_service.py             # Kafka management service
├── kafka_routes.py              # FastAPI REST endpoints
├── requirements.txt             # Python dependencies
├── .env                         # Kafka configuration
├── docker-compose.yml           # Kafka services (Docker)
├── start_kafka.ps1              # Quick start script
├── test_kafka_integration.py    # Integration test suite
├── README.md                    # Detailed architecture docs
├── SETUP_GUIDE.md               # Full setup guide
└── QUICK_START.md               # This file
```

---

## Next Steps

### 1. Try a Real Ingestion

1. Open GUI: http://localhost:3000
2. Start a PostgreSQL/MariaDB ingestion job
3. Watch backend logs for Kafka messages:
   ```
   KafkaBridge: [postgres] chunk=1 → 'bronze-postgres' (5000 rows)
   ```
4. Check Kafka UI: http://localhost:8080

### 2. Set Up Debezium CDC (Optional)

Capture real-time database changes automatically:
- Read: [SETUP_GUIDE.md](SETUP_GUIDE.md#debezium-cdc)

### 3. Build Spark Streaming Consumer

Process Kafka topics with Spark Structured Streaming:
```python
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bronze-postgres") \
    .load()
```

---

## Need Help?

📖 **Full Documentation:** [SETUP_GUIDE.md](SETUP_GUIDE.md)  
🏗️ **Architecture:** [README.md](README.md)  
🐛 **Issues?** Check backend logs for Kafka warnings  

---

## Summary

**What You Installed:**
- ✅ Kafka broker for real-time messaging
- ✅ Kafka Connect for CDC (optional)
- ✅ Python Kafka client library
- ✅ REST API for Kafka management
- ✅ Kafka UI for visual monitoring

**What Changed in Your Pipeline:**
- ✅ Data is now published to Kafka **alongside** MinIO
- ✅ Zero impact on existing functionality
- ✅ Graceful fallback if Kafka is unavailable

**You're now ready for Phase 5!** 🎉
