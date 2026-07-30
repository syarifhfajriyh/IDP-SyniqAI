# CDC Pipeline Setup Guide

Complete guide for setting up Change Data Capture (CDC) pipeline with Debezium, Kafka, Spark Streaming, and Iceberg.

## Architecture Overview

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌─────────────┐
│  PostgreSQL │────>│   Debezium   │────>│    Kafka    │────>│    Spark    │
│  (WAL/CDC)  │     │  Connector   │     │   Topics    │     │  Streaming  │
└─────────────┘     └──────────────┘     └─────────────┘     └─────────────┘
                                                                       │
┌─────────────┐     ┌──────────────┐                                  │
│   MariaDB   │────>│   Debezium   │────>│            │────────────────┘
│ (Binlog/CDC)│     │  Connector   │     │            │
└─────────────┘     └──────────────┘     └────────────┘
                                                  │
                                                  ▼
                                          ┌─────────────┐
                                          │   Iceberg   │
                                          │   Bronze    │
                                          └─────────────┘
```

## Prerequisites

### Software Requirements
- **Docker & Docker Compose** (for Kafka stack)
- **Python 3.8+** with pip
- **PostgreSQL 10+** (preferably 12+)
- **MariaDB 10.2+** (preferably 10.5+)
- **MinIO** (already set up in Phase 4)
- **Java 11** (for Spark)

### Python Packages
```bash
pip install kafka-python pyspark psycopg2-binary mysql-connector-python requests
```

### Network Access
- PostgreSQL: Port 5432 accessible
- MariaDB: Port 3306 accessible
- Kafka: Port 9092 accessible
- Kafka Connect: Port 8083 accessible
- MinIO: Port 9000 accessible

---

## Step 1: Configure Database for CDC

### 1.1 PostgreSQL WAL Configuration

#### Edit postgresql.conf
Location:
- Windows: `C:\Program Files\PostgreSQL\XX\data\postgresql.conf`
- Linux: `/etc/postgresql/XX/main/postgresql.conf`

Add these lines:
```ini
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
wal_keep_size = 1024  # PostgreSQL 13+
```

#### Run Setup Script
```bash
cd "Kafka Integration"
psql -U postgres -d postgres -f setup_postgres_wal.sql
```

This script:
- Creates `debezium_user` with replication privileges
- Creates publication `dbz_publication`
- Creates test table `cdc_test`

#### Restart PostgreSQL
- **Windows**: Services → PostgreSQL → Restart
- **Linux**: `sudo systemctl restart postgresql`

#### Verify Configuration
```bash
psql -U postgres -c "SELECT setting FROM pg_settings WHERE name = 'wal_level';"
# Should return: logical

psql -U postgres -c "SELECT * FROM pg_publication WHERE pubname = 'dbz_publication';"
# Should return the publication
```

---

### 1.2 MariaDB Binlog Configuration

#### Edit my.cnf / my.ini
Location:
- Windows: `C:\Program Files\MariaDB XX.X\data\my.ini`
- Linux: `/etc/mysql/my.cnf` or `/etc/my.cnf`

Add under `[mysqld]`:
```ini
server-id=1
log-bin=mysql-bin
binlog-format=ROW
binlog-row-image=FULL
expire_logs_days=7
max_binlog_size=100M
gtid_strict_mode=ON
```

#### Run Setup Script
```bash
mysql -u root -p < setup_mariadb_binlog.sql
```

This script:
- Creates `debezium_user` with replication privileges
- Creates test database `test_cdc` with table `cdc_test`

#### Restart MariaDB
- **Windows**: Services → MariaDB → Restart
- **Linux**: `sudo systemctl restart mariadb`

#### Verify Configuration
```bash
mysql -u root -p -e "SELECT @@log_bin, @@binlog_format, @@binlog_row_image;"
# Should return: 1, ROW, FULL

mysql -u root -p -e "SHOW BINARY LOGS;"
# Should list binlog files
```

---

## Step 2: Start Kafka Stack with Debezium

### 2.1 Restart Docker Compose (to install Debezium connectors)

```powershell
cd "Kafka Integration"

# Stop existing containers
docker-compose down

# Start with new configuration (installs Debezium connectors)
docker-compose up -d

# Wait for services to initialize (this takes 2-3 minutes)
Start-Sleep -Seconds 120

# Check status
docker ps
```

Expected containers:
- `syniqai-zookeeper` (Up)
- `syniqai-kafka` (Up)
- `syniqai-kafka-connect` (Up, healthy) ← This will install Debezium connectors on startup
- `syniqai-kafka-ui` (Up)

### 2.2 Verify Debezium Connectors Installed

```powershell
# Check Kafka Connect
curl http://localhost:8083/

# List available connector plugins
curl http://localhost:8083/connector-plugins | ConvertFrom-Json | Select-Object class
```

Should see:
- `io.debezium.connector.postgresql.PostgresConnector`
- `io.debezium.connector.mysql.MySqlConnector`

---

## Step 3: Create CDC Connectors

### 3.1 Update Database Connection Configs

Edit connection details if different from defaults:

**For PostgreSQL**: Edit `setup_cdc_connectors.py` line ~50
```python
conn_config = {
    "host": "YOUR_POSTGRES_HOST",  # e.g., "192.168.2.114"
    "port": 5432,
    "database": "postgres",
    "user": "postgres",
    "password": "YOUR_PASSWORD"
}
```

**For MariaDB**: Edit `setup_cdc_connectors.py` line ~120
```python
conn_config = {
    "host": "YOUR_MARIADB_HOST",  # e.g., "192.168.2.114"
    "port": 3306,
    "database": "mysql",
    "user": "remote_user",
    "password": "YOUR_PASSWORD"
}
```

### 3.2 Create Connectors

```powershell
cd "Kafka Integration"

# Create all connectors
python setup_cdc_connectors.py

# OR create specific connector
python setup_cdc_connectors.py --source postgres
python setup_cdc_connectors.py --source mariadb
```

Expected output:
```
==========================================================
  SYINIQ CDC Connector Setup
==========================================================

Kafka Connect is running
  Version: X.X.X

==========================================================
Setting up PostgreSQL CDC Connector
==========================================================
Creating connector: postgres-cdc-connector
  Host: 192.168.2.114:5432
  Database: postgres
  Tables: ['public.hosp_raya_patient_record', 'public.cdc_test']
Connector created successfully!
Topics will be prefixed with: cdc.syniq_postgres
Waiting for connector to start...
Connector state: RUNNING
  Task 0: RUNNING

Setup Complete: 2/2 connectors created
```

### 3.3 Verify Connectors via UI

Open Kafka UI: http://localhost:8080

Navigate to:
1. **Kafka Connect** → Should see:
   - `postgres-cdc-connector` (RUNNING)
   - `mariadb-cdc-connector` (RUNNING)

2. **Topics** → Should see CDC topics:
   - `cdc.syniq_postgres.public.hosp_raya_patient_record`
   - `cdc.syniq_postgres.public.cdc_test`
   - `cdc.syniq_mariadb.mysql.test_table`
   - and others...

---

## Step 4: Test CDC Event Flow

### 4.1 Run Test Suite

```powershell
python test_cdc_pipeline.py
```

This will:
1. ✓ Test Kafka Connect health
2. ✓ Verify PostgreSQL WAL configuration
3. ✓ Verify MariaDB binlog configuration
4. ✓ Check connector statuses
5. ✓ Insert test data and verify CDC events in Kafka

Expected output:
```
================================================================================
                        CDC PIPELINE TESTING
================================================================================

[TEST] Kafka Connect Health
  Verify Kafka Connect is running
    Kafka Connect version: 7.5.0
  [PASS] Completed in 0.15s

[TEST] PostgreSQL WAL Configuration
  Verify PostgreSQL WAL is configured for CDC
    wal_level: logical
    debezium_user exists: True
    dbz_publication exists: True
  [PASS] Completed in 0.23s

...

================================================================================
                             TEST SUMMARY
================================================================================
Total: 7 | Passed: 7 | Failed: 0 | Errors: 0

  [PASS] Kafka Connect Health
  [PASS] PostgreSQL WAL Configuration
  [PASS] PostgreSQL Connector Status
  [PASS] PostgreSQL CDC Events
  [PASS] PostgreSQL Kafka Topics
  [PASS] MariaDB Binlog Configuration
  [PASS] MariaDB Connector Status

All tests passed! CDC pipeline is operational.
```

### 4.2 Manual Testing

#### Insert data into PostgreSQL:
```sql
INSERT INTO public.cdc_test (message) 
VALUES ('Hello from CDC!');
```

#### Check Kafka topic (via UI or consumer):
```powershell
# Using Kafka console consumer
docker exec -it syniqai-kafka kafka-console-consumer `
  --bootstrap-server localhost:9092 `
  --topic cdc.syniq_postgres.public.cdc_test `
  --from-beginning
```

Expected CDC event:
```json
{
  "id": 123,
  "message": "Hello from CDC!",
  "created_at": "2026-03-05T10:30:00Z",
  "__op": "c",
  "__source_ts_ms": 1709636400000,
  "__source_db": "postgres",
  "__source_table": "cdc_test"
}
```

---

## Step 5: Start Spark Streaming Consumer

### 5.1 Install Spark Dependencies

The consumer will auto-download required JARs:
- `spark-sql-kafka-0-10_2.12:3.5.0`
- `iceberg-spark-runtime-3.5_2.12:1.4.2`
- `hadoop-aws:3.3.4`

### 5.2 Configure MinIO Credentials

Edit `spark_cdc_consumer.py` if needed (lines ~20):
```python
minio_endpoint="http://localhost:9000"
minio_access_key="minioadmin"
minio_secret_key="minioadmin"
```

### 5.3 Start Consumer

```powershell
cd "Kafka Integration"

# Consume all CDC sources
python spark_cdc_consumer.py

# OR consume specific source
python spark_cdc_consumer.py --source postgres
python spark_cdc_consumer.py --source mariadb
```

Expected output:
```
2026-03-05 10:30:00 - INFO - Creating Spark session with Iceberg + Kafka support...
2026-03-05 10:30:15 - INFO - Spark session created: 3.5.0
2026-03-05 10:30:15 - INFO - Starting Spark CDC Consumers
2026-03-05 10:30:15 - INFO - Starting PostgreSQL CDC consumer for tables: public.*
2026-03-05 10:30:20 - INFO - PostgreSQL CDC consumer started
2026-03-05 10:30:20 - INFO - Started 2 streaming queries
2026-03-05 10:30:20 - INFO - Waiting for termination... (Ctrl+C to stop)

-------------------------------------------
Batch: 0
-------------------------------------------
+---+------------------+----+---------------+--------+...
|key|op                |ts  |source_db      |table   |...
+---+------------------+----+---------------+--------+...
|123|c                 |... |postgres       |cdc_test|...
2026-03-05 10:30:25 - INFO - Processing batch 0 with 1 records
2026-03-05 10:30:26 - INFO - Successfully wrote 1 records to local.bronze.postgres_cdc_test
```

### 5.4 Verify Iceberg Tables Created

Check MinIO browser: http://localhost:9001
- Bucket: `bronze`
- Path: `iceberg/bronze/postgres_cdc_test/`

Should see:
- `metadata/` folder with table metadata
- `data/` folder with Parquet files

---

## Step 6: Monitor the Pipeline

### 6.1 Start Monitoring Dashboard

```powershell
python cdc_monitor.py
```

Expected dashboard:
```
================================================================================
                    SYINIQ CDC PIPELINE MONITOR
================================================================================
Timestamp: 2026-03-05 10:35:00

CONNECTORS:
--------------------------------------------------------------------------------
  [OK] postgres-cdc-connector
      State: RUNNING | Tasks: 1 running, 0 failed
  [OK] mariadb-cdc-connector
      State: RUNNING | Tasks: 1 running, 0 failed

KAFKA TOPICS:
--------------------------------------------------------------------------------
  cdc.syniq_postgres.public.cdc_test
      Messages: 125 | Rate: 2.3/sec | Partitions: 1
  cdc.syniq_postgres.public.hosp_raya_patient_record
      Messages: 54,231 | Rate: 45.1/sec | Partitions: 1

RECENT ALERTS:
--------------------------------------------------------------------------------
  No alerts

================================================================================
Press Ctrl+C to exit
================================================================================
```

### 6.2 Export Metrics

```powershell
python cdc_monitor.py --export metrics_report.json
```

---

## Step 7: Production Configuration

### 7.1 Connector Tuning

For high-throughput scenarios, edit connector configs in `setup_cdc_connectors.py`:

```python
# Increase task parallelism
"tasks.max": "4"

# Adjust batch sizes
"max.batch.size": "2048"

# Tune poll intervals
"poll.interval.ms": "1000"
```

### 7.2 Spark Tuning

Edit `spark_cdc_consumer.py`:

```python
# Increase parallelism
.config("spark.sql.shuffle.partitions", "16")

# Adjust memory
.config("spark.executor.memory", "4g")
.config("spark.driver.memory", "2g")

# Tune Kafka consumer
.option("maxOffsetsPerTrigger", "50000")
```

### 7.3 Kafka Topic Configuration

Increase retention for CDC topics:
```bash
docker exec -it syniqai-kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name cdc.syniq_postgres.public.cdc_test \
  --alter --add-config retention.ms=604800000  # 7 days
```

---

## Troubleshooting

### Issue: Connector fails with "replication slot already exists"

**Solution**: Drop the slot and restart connector
```sql
-- PostgreSQL
SELECT pg_drop_replication_slot('debezium_syniq_slot');

-- Then restart connector
python -c "from debezium_manager import get_debezium_manager; \
           m = get_debezium_manager(); \
           m.restart_connector('postgres-cdc-connector')"
```

### Issue: "No snapshot found" error

**Solution**: Change snapshot mode to "always"
```python
# In setup_cdc_connectors.py
snapshot_mode="always"  # Instead of "initial"
```

### Issue: High replication lag

**Symptoms**: `pg_replication_slots` shows large `restart_lsn` gap

**Solutions**:
1. Increase `wal_keep_size` in postgresql.conf
2. Reduce connector batch size
3. Add more connector tasks

### Issue: Spark job OOM errors

**Solution**: Adjust batch size and memory
```python
.config("spark.streaming.kafka.maxRatePerPartition", "500")
.config("spark.executor.memory", "8g")
```

### Issue: Iceberg table schema conflicts

**Solution**: Enable schema evolution
```python
# In spark_cdc_consumer.py
df.writeTo(table_name).using("iceberg") \
  .option("write.format.default", "parquet") \
  .option("write.merge.mode", "merge-on-read") \
  .createOrReplace()
```

---

## Monitoring Queries

### PostgreSQL Replication Status
```sql
-- Check replication slots
SELECT * FROM pg_replication_slots;

-- Check replication lag
SELECT 
    slot_name,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag
FROM pg_replication_slots;
```

### MariaDB Binlog Status
```sql
-- Check binlog position
SHOW MASTER STATUS;

-- View recent binlog events
SHOW BINLOG EVENTS LIMIT 10;

-- Check binlog file sizes
SELECT 
    Log_name,
    ROUND(File_size/1024/1024, 2) AS size_mb
FROM information_schema.BINARY_LOGS
ORDER BY Log_name DESC;
```

---

## Next Steps

1. **Enable CDC for Production Tables**
   - Add your production tables to `table_include_list` in `setup_cdc_connectors.py`
   - Restart connectors

2. **Set Up Silver Layer Processing**
   - Create transformation jobs to clean and deduplicate Bronze data
   - Schedule with Airflow (Phase 5b)

3. **Configure Alerts**
   - Integrate `cdc_monitor.py` with Slack/Email notifications
   - Set up alerting for connector failures and lag

4. **Performance Optimization**
   - Benchmark throughput and latency
   - Tune connector and Spark configurations
   - Set up partitioning strategy for high-volume tables

---

## Architecture Validation

Your CDC pipeline now implements:

✅ **Real-time Path**: PostgreSQL/MariaDB → Debezium → Kafka → Spark Streaming → Iceberg (15-30 sec latency)

✅ **CDC Format**: Debezium change events with operation type (INSERT/UPDATE/DELETE)

✅ **ACID Guarantees**: Iceberg provides transactional writes

✅ **Schema Evolution**: Automatic handling via Iceberg

✅ **Monitoring**: Real-time dashboard with connector health and metrics

Next phase will add:
- Batch orchestration with Airflow (MongoDB/S3 sources)
- Silver layer automation with quality gates
- Gold layer aggregations and reports

---

## Support

For issues or questions:
1. Check logs: `docker logs syniqai-kafka-connect`
2. Review connector status: http://localhost:8080
3. Run diagnostic test: `python test_cdc_pipeline.py`
4. Refer to Debezium docs: https://debezium.io/documentation/

**End of Setup Guide**
