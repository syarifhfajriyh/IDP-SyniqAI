# 🔧 Getting Your Data Lakehouse Running

## Step 1: Fix MinIO Connection

Your MinIO is running but needs proper credentials.

### Option A: Get Credentials from MinIO Console (Recommended)

1. **Open MinIO Console in browser:**
   ```
   http://localhost:9001
   ```

2. **Login with:**
   - Username: `admin`
   - Password: `password123`

3. **Create Access Keys:**
   - Click on "Access Keys" in left menu
   - Click "Create access key +"
   - Copy both keys (will look like: `minioadmin` and `minioadmin`)
   - Click "Create"

4. **Update config file:**
   
   Open: `config/minio_config.yaml`
   
   Change this section:
   ```yaml
   minio:
     endpoint: localhost:9000
     access_key: admin              # ← Change to your new access key
     secret_key: password123        # ← Change to your new secret key
     secure: false
   ```

### Option B: Use Default MinIO Credentials (Quick Test)

If the Console won't let you in, restart MinIO with standard credentials:

```powershell
# Stop current MinIO
docker stop $(docker ps -q --filter ancestor=minio/minio)

# Start with standard credentials
docker run -d -p 9000:9000 -p 9001:9001 `
  -e "MINIO_ROOT_USER=minioadmin" `
  -e "MINIO_ROOT_PASSWORD=minioadmin" `
  minio/minio server /data --console-address ":9001"
```

Then update `config/minio_config.yaml`:
```yaml
minio:
  endpoint: localhost:9000
  access_key: minioadmin
  secret_key: minioadmin
  secure: false
```

### Test Connection

```powershell
cd "c:\Users\Syarifah\OneDrive - M Telecommunication Sdn Bhd\INTERNSHIP\SyniqAi\data lakehouse\syniq_project"

..\syniq_env\Scripts\python.exe main.py status
```

Expected output:
```
📦 MinIO:
  Status:  ✅ Connected
  Buckets: 0 (or more if already created)
```

---

## Step 2: Setup Data Sources in ingestion_config.yaml

I've created a starter configuration with example sources. You need to tell me about your actual databases.

### What I Need to Know:

**For each database you want to ingest:**

1. **PostgreSQL:**
   - Do you have a PostgreSQL database?
   - What tables do you want to ingest?
   - Example: `customers`, `policies`, `transactions`

2. **MariaDB:**
   - Do you have MariaDB?
   - Which tables?

3. **MongoDB:**
   - Do you have MongoDB?
   - Which collections?

### Example Configuration

I've created `config/ingestion_config_EXAMPLE.yaml` with a simple setup.

**If you want to test with dummy data first:**
- I can help you create a simple test database
- Or we can use a sample SQLite database

**If you have real databases:**
- Tell me the database details (I'll help you configure them)
- We'll update `db_config.yaml` and `ingestion_config.yaml`

---

## Step 3: Run Your First Ingestion

Once MinIO is connected and data sources are configured:

### A. Setup MinIO Buckets

```powershell
cd "c:\Users\Syarifah\OneDrive - M Telecommunication Sdn Bhd\INTERNSHIP\SyniqAi\data lakehouse\syniq_project"

# Create bronze/silver/gold buckets
..\syniq_env\Scripts\python.exe main.py setup
```

Expected output:
```
📋 Creating buckets:
  ✅ bronze (created)
  ✅ silver (created)
  ✅ gold (created)
```

### B. Test Database Connections

```powershell
# Test all configured connections
..\syniq_env\Scripts\python.exe main.py test
```

### C. List Available Sources

```powershell
# See what's configured
..\syniq_env\Scripts\python.exe main.py list sources
```

### D. Run Ingestion

```powershell
# Ingest all configured tables
..\syniq_env\Scripts\python.exe main.py ingest

# Or just one source
..\syniq_env\Scripts\python.exe main.py ingest --source postgres_prod

# Or just one table
..\syniq_env\Scripts\python.exe main.py ingest --entity customers

# With report
..\syniq_env\Scripts\python.exe main.py ingest --report reports/first_run.json
```

---

## Quick Start Checklist

- [ ] **MinIO Running:** `docker ps` should show minio/minio
- [ ] **MinIO Console Accessible:** http://localhost:9001
- [ ] **Credentials Updated:** in `config/minio_config.yaml`
- [ ] **Connection Test Passes:** `python main.py status` shows ✅
- [ ] **Buckets Created:** `python main.py setup` succeeds
- [ ] **Database Config Updated:** `config/db_config.yaml` has your DB details
- [ ] **Ingestion Config Updated:** `config/ingestion_config.yaml` has your tables
- [ ] **Database Test Passes:** `python main.py test` shows ✅
- [ ] **First Ingestion:** `python main.py ingest` runs successfully

---

## What to Tell Me

To help you configure your data sources, please tell me:

1. **Which databases do you have?**
   - [ ] PostgreSQL
   - [ ] MariaDB (local)
   - [ ] MariaDB Cloud (SkySQL)
   - [ ] MongoDB
   - [ ] Other

2. **Database connection details:**
   - Host/IP
   - Port
   - Database name
   - Do you have credentials? (don't share them here - we'll use environment variables)

3. **Which tables/collections do you want to ingest?**
   - Just list the names, e.g., "customers, orders, products"

4. **Do you want to test first with dummy data?**
   - I can help create a simple SQLite test database
   - Or show you how to generate sample data

---

## Common Issues & Solutions

### Issue: "No module named 'psycopg2'"
```powershell
cd "c:\Users\Syarifah\OneDrive - M Telecommunication Sdn Bhd\INTERNSHIP\SyniqAi\data lakehouse"
.\syniq_env\Scripts\pip.exe install psycopg2-binary
```

### Issue: "Cannot connect to database"
- Check database is running
- Verify credentials in `config/db_config.yaml`
- Test with database client first

### Issue: "MinIO connection refused"
- Check MinIO is running: `docker ps`
- Check port 9000 is accessible: http://localhost:9000
- Verify credentials in MinIO console

---

## Next Steps After First Ingestion

Once you successfully run your first ingestion:

1. **Check Data in MinIO:**
   - Go to http://localhost:9001
   - Browse to "bronze" bucket
   - You should see: `bronze/source_name/table_name/year=2026/month=02/day=18/`

2. **View Logs:**
   ```powershell
   cd syniq_project\logs
   type syniq_2026-02-18.log
   ```

3. **Check Report:**
   - Open `reports/first_run.json` (if you saved a report)
   - See metrics: rows ingested, files created, duration

4. **Schedule Regular Ingestion:**
   - Use Windows Task Scheduler
   - Or set up a cron-like scheduler
   - Run daily: `python main.py ingest --report reports/daily.json`

---

**Let me know your database details and I'll help configure everything!** 🚀
