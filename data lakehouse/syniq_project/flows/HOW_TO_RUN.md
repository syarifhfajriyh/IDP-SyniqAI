# 🚀 HOW TO RUN - Step-by-Step Guide

## Prerequisites Check

Before starting, ensure:
- ✅ MinIO is running in Docker
- ✅ Virtual environment is activated
- ✅ All dependencies are installed

```powershell
# Check MinIO is running
docker ps

# You should see minio/minio container

# Activate virtual environment (if not already)
cd "c:\Users\Syarifah\OneDrive - M Telecommunication Sdn Bhd\INTERNSHIP\SyniqAi\data lakehouse"
.\syniq_env\Scripts\activate

# Navigate to project
cd syniq_project
```

---

## STEP 1: Fix MinIO Connection (First Time Only)

### Method 1: Get Credentials from MinIO Console

1. Open browser: http://localhost:9001
2. Login with: `admin` / `password123`
3. Go to "Access Keys" → "Create access key +"
4. Copy the access key and secret key
5. Edit `config\minio_config.yaml`:

```yaml
minio:
  endpoint: localhost:9000
  access_key: YOUR_ACCESS_KEY_HERE    # Paste from console
  secret_key: YOUR_SECRET_KEY_HERE    # Paste from console
  secure: false
```

### Method 2: Use Standard Credentials (Quick)

Edit `config\minio_config.yaml`:

```yaml
minio:
  endpoint: localhost:9000
  access_key: minioadmin
  secret_key: minioadmin
  secure: false
```

If it doesn't work, restart MinIO:

```powershell
# Stop MinIO
docker stop $(docker ps -q --filter ancestor=minio/minio)

# Start with standard credentials
docker run -d -p 9000:9000 -p 9001:9001 `
  -e "MINIO_ROOT_USER=minioadmin" `
  -e "MINIO_ROOT_PASSWORD=minioadmin" `
  minio/minio server /data --console-address ":9001"
```

---

## STEP 2: Run Setup Test

Test everything is working:

```powershell
# Run comprehensive test
python test_setup.py
```

This will check:
- ✅ Python modules installed
- ✅ Project structure
- ✅ Configuration files
- ✅ MinIO connection
- ✅ Sample data ingestion

**Expected output:**
```
✅ PASSED - IMPORTS
✅ PASSED - STRUCTURE
✅ PASSED - CONFIGS
✅ PASSED - MINIO
✅ PASSED - INGESTION

🎉 All tests passed! Your lakehouse is ready.
```

---

## STEP 3: Setup MinIO Buckets

Create bronze/silver/gold buckets:

```powershell
python main.py setup
```

**Expected output:**
```
📋 Creating buckets:
  ✅ bronze (created)
  ✅ silver (created)
  ✅ gold (created)

✅ MinIO setup completed
```

---

## STEP 4: Check System Status

Verify everything is connected:

```powershell
python main.py status
```

**Expected output:**
```
📊 Configuration:
  Project Root: C:\...\syniq_project
  Config Dir:   C:\...\syniq_project\config

📦 MinIO:
  Status:  ✅ Connected
  Buckets: 3

✅ All systems operational
```

---

## STEP 5: Configure Your Data Sources

Now you need to tell the system about your databases.

### Option A: I Have Real Databases

1. **Edit `config\db_config.yaml`:**

For PostgreSQL:
```yaml
postgresql:
  host: your-postgres-host
  port: 5432
  database: your-database
  user: your-username
  password: your-password
  schema: public
```

For MariaDB:
```yaml
mariadb:
  host: your-mariadb-host
  port: 3306
  database: your-database
  user: your-username
  password: your-password
```

2. **Edit `config\ingestion_config.yaml`:**

Enable your sources and add tables:

```yaml
sources:
  postgres_prod:
    enabled: true              # ← Set to true
    connector_type: postgres
    
    entities:
      customers:               # ← Your table name
        enabled: true
        extraction:
          mode: full
          batch_size: 5000
      
      orders:                  # ← Another table
        enabled: true
        extraction:
          mode: full
          batch_size: 10000
```

See `config\ingestion_config_EXAMPLE.yaml` for more options.

### Option B: Test with Sample Data First

Use the test script from STEP 2 - it creates sample data automatically!

Or skip to STEP 8 to see examples.

---

## STEP 6: Validate Configuration

Check your config files are correct:

```powershell
python main.py validate
```

**Expected output:**
```
📋 Validating Configuration Files:

  ✅ Database Config (db_config.yaml)
     Keys: postgresql, mariadb, mongodb

  ✅ Ingestion Config (ingestion_config.yaml)
     Keys: sources

  ✅ MinIO Config (minio_config.yaml)
     Keys: minio

✅ All configurations are valid
```

---

## STEP 7: Test Database Connections

Test connections to your databases:

```powershell
python main.py test
```

**Expected output:**
```
📡 Testing Database Connections:

  Testing postgresql...
    ✅ Connected successfully
       Database: mydb
       Version: PostgreSQL 14.5

  Testing mariadb...
    ✅ Connected successfully
       Database: mydb
       Version: MariaDB 10.6

📦 Testing MinIO Connection:
  ✅ Connected to MinIO
     Endpoint: localhost:9000
     Buckets: bronze, silver, gold

✅ All connection tests passed
```

If any fail, check your credentials in `config\db_config.yaml`.

---

## STEP 8: List Available Sources

See what's configured:

```powershell
# List all sources
python main.py list sources

# List all entities/tables
python main.py list entities

# List entities for specific source
python main.py list entities --source postgres_prod
```

**Expected output:**
```
📊 Available Sources:

✅ Enabled postgres_prod
  Connector: postgres
  Entities: 2/2 enabled

📋 Available Entities:

📦 postgres_prod
  ✅ customers
      Mode: full, Batch: 5000
  ✅ orders
      Mode: full, Batch: 10000
```

---

## STEP 9: Run Your First Ingestion 🎉

Now for the main event!

### Test with One Table First

```powershell
# Ingest just one table
python main.py ingest --entity customers
```

### Run Full Ingestion

```powershell
# Ingest all configured tables
python main.py ingest
```

### With Report

```powershell
# Save detailed report
python main.py ingest --report reports\first_run.json
```

**Expected output:**
```
============================================================
SYNIQ DATA LAKEHOUSE - INGESTION
============================================================
Starting ingestion...

============================================================
INGESTION REPORT
============================================================

📊 Summary:
  Start Time:      2026-02-18 14:00:00
  End Time:        2026-02-18 14:05:30
  Duration:        330.50 seconds
  Total Jobs:      2
  ✅ Successful:   2
  ❌ Failed:       0
  Total Rows:      15,000
  Total Files:     5
  Total Size:      2.5 MB

📋 Job Details:

  ✅ postgres_prod/customers
     Status:   SUCCESS
     Rows:     5,000
     Files:    1
     Duration: 12.34s

  ✅ postgres_prod/orders
     Status:   SUCCESS
     Rows:     10,000
     Files:    2
     Duration: 18.45s

============================================================
✅ All jobs completed successfully
```

---

## STEP 10: Verify Data in MinIO

Check your data was ingested:

1. **Open MinIO Console:** http://localhost:9001
2. **Navigate to bronze bucket**
3. **Browse folders:**
   ```
   bronze/
   └── postgres_prod/
       ├── customers/
       │   └── year=2026/
       │       └── month=02/
       │           └── day=18/
       │               └── part-20260218_140000.parquet
       └── orders/
           └── year=2026/...
   ```

4. **Download a parquet file** and open with Excel or:

```powershell
# View parquet file with Python
python
```

```python
import pandas as pd
df = pd.read_parquet('downloaded_file.parquet')
print(df.head())
```

---

## STEP 11: Check Logs

View detailed logs:

```powershell
# View today's log
type logs\syniq_2026-02-18.log

# Or open in notepad
notepad logs\syniq_2026-02-18.log
```

---

## Common Use Cases

### Daily Scheduled Ingestion

```powershell
# Create batch file: daily_ingest.bat
cd "C:\Users\Syarifah\...\syniq_project"
..\syniq_env\Scripts\python.exe main.py ingest --report reports\daily_%DATE%.json
```

### Ingest Specific Source

```powershell
python main.py ingest --source postgres_prod
```

### Ingest Specific Tables

```powershell
python main.py ingest --entity customers
python main.py ingest --entity orders
```

### Re-run Failed Ingestion

Just run the same command again:
```powershell
python main.py ingest
```

---

## Troubleshooting

### "InvalidAccessKeyId"
- Fix MinIO credentials in `config\minio_config.yaml`
- See MINIO_TROUBLESHOOTING.md

### "Cannot connect to database"
- Check database is running
- Verify credentials in `config\db_config.yaml`
- Run: `python main.py test`

### "No module named 'XXX'"
```powershell
pip install -r requirements.txt
```

### "No sources configured"
- Enable sources in `config\ingestion_config.yaml`
- Add tables under `entities`
- Run: `python main.py validate`

---

## What's Next?

After successful ingestion:

1. **Schedule Regular Runs:**
   - Windows Task Scheduler for daily ingestion
   - Or use cron-like tool

2. **Add More Tables:**
   - Edit `config\ingestion_config.yaml`
   - Add more entities under each source

3. **Add Validation Rules:**
   - See `config\ingestion_config_EXAMPLE.yaml`
   - Add data quality checks

4. **Build Silver Layer:**
   - Transform and clean data
   - Join multiple sources
   - Create business logic

5. **Build Gold Layer:**
   - Aggregated metrics
   - Analytics-ready datasets
   - ML features

---

## Quick Command Reference

```powershell
# Setup & Testing
python test_setup.py              # Run all setup tests
python main.py setup              # Create MinIO buckets
python main.py status             # Check system status
python main.py validate           # Validate configs
python main.py test               # Test connections

# Information
python main.py list sources       # List all sources
python main.py list entities      # List all tables

# Ingestion
python main.py ingest             # Ingest all
python main.py ingest --source postgres_prod    # One source
python main.py ingest --entity customers        # One table
python main.py ingest --report reports\run.json # With report
```

---

**🎉 Congratulations! Your Syniq Data Lakehouse is running!**

For more help, see:
- GETTING_STARTED.md - Detailed setup guide
- README.md - Project overview
- STEP_6_QUICK_REFERENCE.md - All CLI commands
- MINIO_TROUBLESHOOTING.md - MinIO connection help
