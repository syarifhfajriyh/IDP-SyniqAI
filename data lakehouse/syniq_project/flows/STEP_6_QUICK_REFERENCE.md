# STEP 6 Main CLI - Quick Reference

## 🚀 Quick Commands

```bash
# Setup (first time)
python setup_minio.py                           # Create MinIO buckets

# Daily operations
python main.py ingest                           # Run all ingestion
python main.py ingest --source postgres_prod    # Specific source
python main.py ingest --entity customers        # Specific entity

# Testing & validation
python main.py test                             # Test all connections
python main.py validate                         # Validate configs

# Information
python main.py list sources                     # List all sources
python main.py list entities                    # List all entities
python main.py status                           # System status
```

---

## 📋 All Commands

### 1. INGEST

Run data ingestion with optional filters.

```bash
# Run all
python main.py ingest

# Filter by source
python main.py ingest --source postgres_prod

# Filter by entity
python main.py ingest --entity customers

# Both filters
python main.py ingest --source postgres_prod --entity customers

# Save report
python main.py ingest --report reports/ingestion.json
```

**Aliases:**
- `--source` or `-s`
- `--entity` or `-e`
- `--report` or `-r`

---

### 2. TEST

Test all database and MinIO connections.

```bash
python main.py test
```

**Tests:**
- ✅ PostgreSQL connection
- ✅ MariaDB connection
- ✅ MariaDB Cloud connection
- ✅ MongoDB connection
- ✅ MinIO connection
- ✅ Bucket existence

---

### 3. VALIDATE

Validate all configuration files.

```bash
python main.py validate
```

**Validates:**
- ✅ YAML syntax
- ✅ Required keys
- ✅ Source configuration
- ✅ Entity configuration

---

### 4. LIST

List available sources or entities.

```bash
# List all sources
python main.py list sources

# List all entities
python main.py list entities

# List entities for specific source
python main.py list entities --source postgres_prod
```

---

### 5. SETUP

Create MinIO buckets (bronze, silver, gold).

```bash
python main.py setup
```

**Creates:**
- bronze bucket
- silver bucket
- gold bucket

---

### 6. STATUS

Show current system status.

```bash
python main.py status
```

**Shows:**
- Configuration paths
- Source count (total/enabled)
- Entity count (total/enabled)
- MinIO connection status

---

## 🛠️ setup_minio.py

Standalone MinIO setup and verification.

```bash
python setup_minio.py
```

**Steps:**
1. Test MinIO connection
2. Create required buckets
3. Verify bucket access

---

## 📊 Output Examples

### Successful Ingestion

```
======================================================================
INGESTION REPORT
======================================================================

📊 Summary:
  Start Time:      2024-02-18 12:00:00
  End Time:        2024-02-18 12:05:30
  Duration:        330.50 seconds
  Total Jobs:      5
  ✅ Successful:   4
  ❌ Failed:       1
  Total Rows:      1,250,000
  Total Files:     125
  Total Size:      485.20 MB

📋 Job Details:
  ✅ postgres_prod/customers
     Status:   SUCCESS
     Rows:     50,000
     Files:    10
     Duration: 45.23s
```

### Connection Test

```
📡 Testing Database Connections:

  Testing postgresql...
    ✅ Connected successfully
       Database: mydb
       Version: PostgreSQL 14.5

📦 Testing MinIO Connection:
  ✅ Connected to MinIO
     Endpoint: localhost:9000
     Buckets: bronze, silver, gold
```

### List Sources

```
📊 Available Sources:
======================================================================

✅ Enabled postgres_prod
  Connector: postgres
  Entities: 5/6 enabled

✅ Enabled mariadb_prod
  Connector: mariadb
  Entities: 3/3 enabled
```

---

## 🔧 Configuration

### MinIO (config/minio_config.yaml)

```yaml
minio:
  endpoint: localhost:9000
  access_key: admin
  secret_key: password123
  secure: false
  
  buckets:
    bronze: "bronze"
    silver: "silver"
    gold: "gold"
```

### Environment Variables

Override configuration values:

```powershell
# Windows PowerShell
$env:MINIO_ENDPOINT = "minio.example.com:9000"
$env:MINIO_ACCESS_KEY = "mykey"
$env:MINIO_SECRET_KEY = "mysecret"

python main.py test
```

---

## 🎯 Common Workflows

### First-Time Setup

```bash
# 1. Setup MinIO buckets
python setup_minio.py

# 2. Validate configurations
python main.py validate

# 3. Test connections
python main.py test

# 4. Check status
python main.py status

# 5. List available sources
python main.py list sources

# 6. Run first ingestion
python main.py ingest
```

### Daily Ingestion

```bash
# Full ingestion with report
python main.py ingest --report reports/daily_$(date +%Y%m%d).json

# Or specific source/entity
python main.py ingest --source postgres_prod --report reports/postgres_daily.json
```

### Troubleshooting

```bash
# 1. Check system status
python main.py status

# 2. Test connections
python main.py test

# 3. Validate configs
python main.py validate

# 4. Check logs
cat logs/syniq_$(date +%Y-%m-%d).log
```

---

## 🐛 Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Error (check logs) |
| 130 | User interrupted (Ctrl+C) |

---

## 📁 File Structure

```
syniq_project/
├── main.py                    # Main CLI application
├── setup_minio.py             # MinIO setup script
├── config/
│   ├── db_config.yaml         # Database configurations
│   ├── ingestion_config.yaml  # Ingestion configurations
│   └── minio_config.yaml      # MinIO configuration
├── logs/
│   ├── syniq_2026-02-18.log   # Daily logs
│   └── syniq_2026-02-18.json  # Structured logs
└── reports/
    └── ingestion_*.json       # Ingestion reports
```

---

## 💡 Tips & Tricks

### Scheduled Execution (Windows)

**Create batch file:** `daily_ingest.bat`
```batch
@echo off
cd "C:\path\to\syniq_project"
..\syniq_env\Scripts\python.exe main.py ingest --report reports\daily_%DATE%.json
```

**Schedule with Task Scheduler:**
```powershell
schtasks /create /tn "Syniq Daily Ingestion" /tr "C:\path\to\daily_ingest.bat" /sc daily /st 02:00
```

### Quick Status Check

```bash
# One-liner for status + test
python main.py status && python main.py test
```

### Filtered Reporting

```bash
# Ingest only critical sources
python main.py ingest --source postgres_prod --report reports/critical.json
```

### Debug Mode

View detailed logs:
```powershell
# Set log level (future enhancement)
$env:LOG_LEVEL = "DEBUG"
python main.py ingest
```

---

## 🔗 Related Files

- **STEP 5:** [Ingestion Layer](STEP_5_INGESTION_SUMMARY.md)
- **STEP 4:** [Utils Layer](STEP_4_UTILS_SUMMARY.md)
- **STEP 3:** [Connectors](STEP_3_CONNECTORS_SUMMARY.md)
- **Full Summary:** [STEP 6 Summary](STEP_6_MAIN_CLI_SUMMARY.md)

---

## ✅ Checklist

Before first run:
- [ ] MinIO is running (check Docker)
- [ ] Configuration files are present
- [ ] Run `python setup_minio.py`
- [ ] Run `python main.py validate`
- [ ] Run `python main.py test`

---

## 🚀 Quick Start

```bash
# Activate virtual environment (Windows)
syniq_env\Scripts\activate

# Navigate to project
cd syniq_project

# Setup MinIO
python setup_minio.py

# Test everything
python main.py test

# Run first ingestion
python main.py ingest

# Check status
python main.py status
```

---

**Status:** ✅ PROJECT COMPLETE  
**Version:** Syniq Data Lakehouse v1.0

---

*Quick Reference - STEP 6*  
*Syniq Data Lakehouse Project*
