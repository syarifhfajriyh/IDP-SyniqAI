# MongoDB CDC Integration - Complete Guide

## 📋 Overview

This MongoDB CDC (Change Data Capture) system provides **production-ready batch extraction** with watermark tracking, orchestrated by Apache Airflow.

### Key Features:
- ✅ **Watermark-Based Extraction**: Uses MongoDB ObjectId timestamps for incremental processing
- ✅ **Batch Processing**: Configurable batch sizes (default: 1000 documents)
- ✅ **Kafka Integration**: Streams data to per-collection Kafka topics
- ✅ **Airflow Orchestration**: Scheduled DAG runs every 30 minutes
- ✅ **Error Handling**: Retries, dead letter queue, health monitoring
- ✅ **Zero Data Loss**: Persistent watermarks ensure no missed documents
- ✅ **Parallel Extraction**: Multiple collections processed simultaneously

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          MONGODB CDC FLOW                                    │
└─────────────────────────────────────────────────────────────────────────────┘

MongoDB Database
    ↓
┌───────────────────┐
│ Collections:      │
│ - users           │──> ObjectId: 507f1f77bcf86cd799439011
│ - transactions    │              └─────┬────┘
│ - clickstream     │         Timestamp (4 bytes)
│ - sessions        │         = Watermark
│ - products        │
│ - orders          │
└───────────────────┘
    ↓
[Airflow DAG Trigger - Every 30 min]
    ↓
┌───────────────────────────────────────────────────────────────┐
│ mongodb_batch_extractor.py                                     │
│ --------------------------------------------------------      │
│ 1. Load last watermark from JSON file                         │
│ 2. Query: db.collection.find({ _id: { $gt: watermark } })     │
│ 3. Split results into batches (1000 docs each)                │
│ 4. Send each batch to Kafka topic                             │
│ 5. Update watermark to latest ObjectId processed              │
└───────────────────────────────────────────────────────────────┘
    ↓
Kafka Topics (Per Collection)
    - airflow.mongodb.users
    - airflow.mongodb.transactions
    - airflow.mongodb.clickstream
    ↓
spark_mongodb_batch_consumer.py
    ↓
Bronze Layer (MinIO S3)
    - bronze/mongodb/users/*.parquet
    - bronze/mongodb/transactions/*.parquet
```

---

## 📦 Components

### 1. **mongodb_batch_extractor.py**
Main extraction engine that connects to MongoDB, queries for new data using watermarks, and sends batches to Kafka.

**Key Methods:**
- `load_watermark()`: Retrieves last processed ObjectId from JSON file
- `extract_new_documents()`: Queries MongoDB with watermark filter
- `send_batch_to_kafka()`: Publishes batches to Kafka topics
- `update_watermark()`: Persists new watermark after successful extraction

### 2. **airflow_dags/mongodb_cdc_dag.py**
Airflow DAG that orchestrates the entire CDC pipeline with pre-flight checks, parallel extraction, and post-processing.

**Task Structure:**
```
preflight_checks (TaskGroup)
├─> check_mongodb_connection
├─> check_kafka_connection
└─> load_watermarks
    ↓
extract_collections (TaskGroup)
├─> extract_users
├─> extract_transactions
├─> extract_clickstream
└─> ... (parallel execution)
    ↓
aggregate_metrics
    ↓
trigger_spark_consumers
    ↓
health_check
```

### 3. **watermarks/mongodb_watermarks.json**
Persistent storage for tracking the last processed ObjectId per collection.

**Format:**
```json
{
  "users": {
    "last_watermark": "507f1f77bcf86cd799439011",
    "last_updated": "2026-03-11T10:30:00Z",
    "documents_processed": 15420
  },
  "transactions": {
    "last_watermark": "507f1f88bcf86cd799439022",
    "last_updated": "2026-03-11T10:30:00Z",
    "documents_processed": 98234
  }
}
```

### 4. **spark_mongodb_batch_consumer.py**
Spark Structured Streaming job that reads from Kafka topics and writes to Bronze layer.

**Features:**
- Reads JSON batches from Kafka
- Flattens nested documents
- Converts to Parquet format
- Writes to MinIO S3 (bronze bucket)

### 5. **.env.mongodb**
Configuration file for all MongoDB CDC settings.

**Key Variables:**
```bash
MONGODB_URI=mongodb://localhost:27017/
MONGODB_DATABASE=syniqai_production
MONGODB_COLLECTIONS=users,transactions,clickstream
MONGODB_BATCH_SIZE=1000
MONGODB_POLL_INTERVAL_MINUTES=30
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

---

## 🚀 Quick Start

### **Option 1: Automated Setup (Recommended)**

Run the production quick start script:
```powershell
.\production_quickstart.ps1
```

This will:
1. Install Airflow with Python 3.11 virtual environment
2. Configure MongoDB CDC components
3. Start Kafka, MinIO, Airflow, Backend, Frontend
4. Deploy MongoDB CDC DAG to Airflow
5. Create necessary buckets and topics

### **Option 2: Manual Setup**

#### Step 1: Install Airflow
```powershell
.\setup_production_orchestration.ps1
```

#### Step 2: Configure MongoDB
Edit `Kafka Integration\.env.mongodb`:
```bash
MONGODB_URI=mongodb://your-host:27017/
MONGODB_DATABASE=your_database
MONGODB_COLLECTIONS=collection1,collection2,collection3
```

#### Step 3: Copy DAG to Airflow
```powershell
Copy-Item ".\airflow_dags\mongodb_cdc_dag.py" -Destination "C:\airflow\dags\"
```

#### Step 4: Start Services
```powershell
# Start Kafka
cd "Kafka Integration"
docker-compose up -d

# Start Airflow
.\start_airflow.ps1

# Start Backend
cd gui
python app.py

# Start Frontend
npm run dev
```

#### Step 5: Enable DAG
1. Open http://localhost:8080 (admin/admin123)
2. Find `mongodb_cdc_extraction` DAG
3. Toggle the switch to enable
4. Click "Trigger DAG" to start first run

---

## 🔧 Configuration

### Extraction Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGODB_BATCH_SIZE` | 1000 | Documents per batch sent to Kafka |
| `MONGODB_POLL_INTERVAL_MINUTES` | 30 | How often DAG runs |
| `MONGODB_MAX_BATCHES_PER_RUN` | 0 | Max batches per extraction (0=unlimited) |
| `MONGODB_DRY_RUN` | false | Test mode - no Kafka sending |
| `MONGODB_WORKER_THREADS` | 4 | Parallel extraction threads |

### Watermark Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGODB_WATERMARK_FILE` | ./watermarks/mongodb_watermarks.json | Watermark storage path |
| `MONGODB_WATERMARK_BACKUP_ENABLED` | true | Backup to MinIO |

### Performance Tuning

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGODB_MAX_POOL_SIZE` | 50 | Connection pool size |
| `MONGODB_READ_PREFERENCE` | secondaryPreferred | MongoDB read strategy |
| `MONGODB_QUERY_TIMEOUT` | 300 | Query timeout in seconds |
| `KAFKA_COMPRESSION_TYPE` | snappy | Compression algorithm |

### Error Handling

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_RETRIES` | 3 | Task retry attempts |
| `RETRY_DELAY_SECONDS` | 60 | Delay between retries |
| `ENABLE_DLQ` | true | Dead letter queue for failures |
| `KAFKA_DLQ_TOPIC` | dlq.mongodb.failures | DLQ topic name |

---

## 📊 Monitoring

### Airflow UI Dashboard

#### View DAG Status:
1. Navigate to http://localhost:8080
2. Check DAG State:
   - **Green**: All tasks succeeded
   - **Red**: Task failed (check logs)
   - **Yellow**: Task running or queued

#### View Task Logs:
1. Click on DAG name
2. Select "Graph" view
3. Click on task node
4. Click "Log" button

#### Check Extraction Metrics:
1. Go to **Admin → Variables**
2. Find `mongodb_cdc_last_run_summary`
3. View JSON with:
   - Total documents processed
   - Batches sent
   - Failed collections
   - Timestamp

### Kafka Monitoring

#### List Topics:
```powershell
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Expected output:
```
airflow.mongodb.users
airflow.mongodb.transactions
airflow.mongodb.clickstream
airflow.mongodb.sessions
```

#### Check Topic Contents:
```powershell
docker exec kafka kafka-console-consumer `
    --bootstrap-server localhost:9092 `
    --topic airflow.mongodb.users `
    --from-beginning `
    --max-messages 5
```

#### Check Consumer Lag:
```powershell
docker exec kafka kafka-consumer-groups `
    --bootstrap-server localhost:9092 `
    --describe `
    --group spark-mongodb-consumers
```

### Watermark Monitoring

#### View Current Watermarks:
```powershell
Get-Content "Kafka Integration\watermarks\mongodb_watermarks.json" | ConvertFrom-Json | ConvertTo-Json
```

#### Decode ObjectId Timestamp:
```python
from bson import ObjectId
from datetime import datetime

oid = ObjectId("507f1f77bcf86cd799439011")
timestamp = oid.generation_time
print(f"Last processed: {timestamp}")
```

### Bronze Layer Verification

#### Check MinIO Console:
1. Open http://localhost:9001
2. Login: minioadmin / minioadmin
3. Navigate to `bronze` bucket
4. Look for: `mongodb/<collection>/` folders

#### List Files via Python:
```python
import boto3

s3 = boto3.client('s3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

response = s3.list_objects_v2(Bucket='bronze', Prefix='mongodb/')
for obj in response.get('Contents', []):
    print(obj['Key'])
```

---

## 🔍 Troubleshooting

### Common Issues

#### **Problem: DAG not appearing in Airflow UI**

**Symptoms:**
- DAG file exists in `C:\airflow\dags\` but not visible in UI
- No errors in Airflow logs

**Solutions:**
```powershell
# Check DAG syntax
cd C:\airflow_venv\Scripts
.\Activate.ps1
$env:AIRFLOW_HOME = "C:\airflow"
airflow dags list | Select-String "mongodb_cdc"

# Force DAG reload
# Delete DAG and re-copy
Remove-Item "C:\airflow\dags\mongodb_cdc_dag.py"
Copy-Item ".\airflow_dags\mongodb_cdc_dag.py" -Destination "C:\airflow\dags\"

# Restart scheduler
Get-Process *airflow* | Stop-Process -Force
.\start_airflow.ps1
```

#### **Problem: MongoDB connection timeout**

**Symptoms:**
- Task `check_mongodb_connection` fails
- Error: "ServerSelectionTimeoutError"

**Solutions:**
```powershell
# Test MongoDB connectivity
mongo --host localhost --port 27017 --eval "db.stats()"

# Check firewall rules
Test-NetConnection -ComputerName localhost -Port 27017

# Verify MONGODB_URI in .env.mongodb
Get-Content "Kafka Integration\.env.mongodb" | Select-String "MONGODB_URI"

# Try with authentication
MONGODB_URI=mongodb://username:password@localhost:27017/?authSource=admin
```

#### **Problem: No data extracted (but collections have data)**

**Symptoms:**
- Tasks succeed but `documents_processed: 0`
- Watermark file exists

**Solutions:**
```powershell
# Check if watermark is newer than data
$watermarks = Get-Content "Kafka Integration\watermarks\mongodb_watermarks.json" | ConvertFrom-Json
Write-Host $watermarks

# Option 1: Reset watermarks (CAUTION: may duplicate data)
Remove-Item "Kafka Integration\watermarks\mongodb_watermarks.json"

# Option 2: Manually adjust watermark to older ObjectId
# Edit mongodb_watermarks.json with older ObjectId

# Option 3: Enable dry-run mode to test
# In .env.mongodb:
MONGODB_DRY_RUN=true
```

#### **Problem: Kafka producer errors**

**Symptoms:**
- Task fails with "KafkaError: unable to bootstrap"
- Cannot send messages to Kafka

**Solutions:**
```powershell
# Check Kafka is running
docker ps | Select-String "kafka"

# Test Kafka connectivity
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Check Kafka logs
docker logs kafka --tail 100

# Restart Kafka
cd "Kafka Integration"
docker-compose restart kafka
```

#### **Problem: Airflow tasks stuck in "running" state**

**Symptoms:**
- Task runs forever, never completes
- No errors in logs

**Solutions:**
```powershell
# Check for hung processes
Get-Process python | Where-Object {$_.Path -like "*airflow*"}

# Kill stuck tasks
airflow tasks clear mongodb_cdc_extraction --yes

# Restart Airflow scheduler
Get-Process *airflow* | Where-Object {$_.ProcessName -like "*scheduler*"} | Stop-Process
# Scheduler will restart automatically

# Check task execution timeout
# In DAG: execution_timeout=timedelta(minutes=45)
```

---

## 🧪 Testing

### Test MongoDB Extraction

#### 1. Seed Test Data:
```powershell
cd "Kafka Integration"
python seed_dummy_data.py
```

This creates:
- 10,000 users
- 50,000 transactions
- 100,000 clickstream events

#### 2. Trigger Manual Extraction:
```powershell
# Via Airflow UI
# or via CLI:
cd C:\airflow_venv\Scripts
.\Activate.ps1
$env:AIRFLOW_HOME = "C:\airflow"
airflow dags trigger mongodb_cdc_extraction
```

#### 3. Monitor Progress:
```powershell
# Watch task logs in real-time
Get-Content "C:\airflow\logs\dag_id=mongodb_cdc_extraction\*\extract_users\*.log" -Wait

# Check watermark updates
Get-Content "Kafka Integration\watermarks\mongodb_watermarks.json" -Wait
```

#### 4. Verify Kafka Messages:
```powershell
docker exec kafka kafka-console-consumer `
    --bootstrap-server localhost:9092 `
    --topic airflow.mongodb.users `
    --property print.key=true `
    --max-messages 10
```

### Dry-Run Mode

Test extraction without sending to Kafka:

```bash
# In .env.mongodb:
MONGODB_DRY_RUN=true
```

Logs will show:
```
[DRY RUN] Would send batch 1/5 with 1000 documents
[DRY RUN] Would send batch 2/5 with 1000 documents
...
```

---

## 🔐 Security Best Practices

### Production Checklist:

- [ ] **Change default passwords:**
  ```powershell
  # Airflow admin password
  airflow users password --username admin
  ```

- [ ] **Enable MongoDB authentication:**
  ```bash
  MONGODB_USERNAME=your_user
  MONGODB_PASSWORD=strong_password
  MONGODB_AUTH_SOURCE=admin
  ```

- [ ] **Secure Kafka with SASL/SSL** (if needed):
  ```bash
  KAFKA_SECURITY_PROTOCOL=SASL_SSL
  KAFKA_SASL_MECHANISM=PLAIN
  ```

- [ ] **Enable Airflow RBAC:**
  ```ini
  # airflow.cfg
  [webserver]
  rbac = True
  ```

- [ ] **Rotate MinIO credentials:**
  ```bash
  MINIO_ACCESS_KEY=new_access_key
  MINIO_SECRET_KEY=new_secret_key_with_32chars
  ```

- [ ] **Set up SSL/TLS for web endpoints**

- [ ] **Configure firewall rules** (close unused ports)

---

## 📈 Performance Optimization

### For Large Collections (> 1M documents):

```bash
# Increase batch size
MONGODB_BATCH_SIZE=5000

# Increase poll interval (reduce frequency)
MONGODB_POLL_INTERVAL_MINUTES=60

# Use multiple worker threads
MONGODB_WORKER_THREADS=8

# Optimize MongoDB read preference
MONGODB_READ_PREFERENCE=secondaryPreferred

# Increase connection pool
MONGODB_MAX_POOL_SIZE=100
```

### For High-Frequency Changes:

```bash
# Decrease poll interval (more frequent)
MONGODB_POLL_INTERVAL_MINUTES=15

# Smaller batches for faster throughput
MONGODB_BATCH_SIZE=500

# Enable Kafka compression
KAFKA_COMPRESSION_TYPE=snappy

# Increase Kafka batch size
KAFKA_BATCH_SIZE=32768
```

---

## 📚 Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/)
- [MongoDB ObjectId Specification](https://docs.mongodb.com/manual/reference/method/ObjectId/)
- [Kafka Producer Configuration](https://kafka.apache.org/documentation/#producerconfigs)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

---

## 🆘 Support

For issues:
1. Check Airflow task logs in UI
2. Review watermark files for corruption
3. Test MongoDB/Kafka connectivity
4. Examine Docker container logs
5. See main deployment guide: `documentation/PRODUCTION_DEPLOYMENT_GUIDE.md`

---

## 📝 Changelog

### v1.0.0 (2026-03-11)
- Initial production-ready MongoDB CDC implementation
- Airflow orchestration with DAG
- Watermark-based incremental extraction
- Kafka integration with per-collection topics
- Comprehensive monitoring and error handling
- Full documentation and testing tools
