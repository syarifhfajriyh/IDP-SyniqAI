# STEP 5 Ingestion Layer - Quick Reference

## 🚀 Quick Import

```python
from ingestion import (
    BronzeIngestor,
    IngestionOrchestrator,
    run_ingestion,
    create_bronze_validator
)
```

---

## 📦 BronzeIngestor

### Basic Usage

```python
from ingestion import BronzeIngestor

# Initialize
ingestor = BronzeIngestor(
    minio_config={
        "endpoint": "localhost:9000",
        "access_key": "minioadmin",
        "secret_key": "minioadmin",
        "secure": False
    },
    bronze_bucket="bronze"
)

# Ingest DataFrame
result = ingestor.ingest_batch(
    df=df,
    source_system="postgres_prod",
    source_entity="customers"
)
```

### With Validation

```python
from utils import DataValidator

validator = DataValidator(entity="customers")
validator.add_not_null_rule(["customer_id"])

ingestor = BronzeIngestor(minio_config, enable_validation=True)

result = ingestor.ingest_batch(
    df=df,
    source_system="mydb",
    source_entity="customers",
    validator=validator
)
```

### From Connector

```python
from connectors import PostgresConnector

connector = PostgresConnector(db_config)
connector.connect()

result = ingestor.ingest_from_connector(
    connector=connector,
    extraction_plan={
        "source_system": "postgres_prod",
        "entity": "customers",
        "mode": "full",
        "batch_size": 5000
    }
)
```

---

## 🎯 IngestionOrchestrator

### Simple Orchestration

```python
from ingestion import run_ingestion

# Run all configured ingestions
report = run_ingestion()

print(f"Jobs: {report['successful_jobs']}/{report['total_jobs']}")
print(f"Rows: {report['total_rows']:,}")
```

### Filtered Orchestration

```python
# Specific source
report = run_ingestion(sources=["postgres_prod"])

# Specific entities
report = run_ingestion(entities=["customers", "policies"])

# Both
report = run_ingestion(
    sources=["postgres_prod"],
    entities=["customers"]
)
```

### Advanced Usage

```python
from ingestion import IngestionOrchestrator

orchestrator = IngestionOrchestrator(
    db_config_path="config/db_config.yaml",
    ingestion_config_path="config/ingestion_config.yaml",
    minio_config_path="config/minio_config.yaml"
)

jobs = orchestrator.create_ingestion_jobs()
print(f"Created {len(jobs)} jobs")

report = orchestrator.run(
    sources=["postgres_prod"],
    entities=["customers"]
)

orchestrator.save_report(report, "reports/ingestion_report.json")
```

---

## 📋 Configuration Templates

### ingestion_config.yaml

```yaml
sources:
  postgres_prod:
    enabled: true
    connector_type: postgres
    entities:
      customers:
        enabled: true
        extraction:
          mode: full            # full | incremental
          batch_size: 5000
        validation_rules:
          - type: not_null
            columns: [customer_id, name]
            severity: ERROR
          - type: range
            column: age
            min: 0
            max: 120
            severity: WARNING
      
      policies:
        enabled: true
        extraction:
          mode: incremental
          watermark_field: updated_at
          batch_size: 2000
  
  mariadb_prod:
    enabled: false    # Disabled source
    connector_type: mariadb
    entities:
      claims:
        enabled: true
        extraction:
          mode: full
```

### minio_config.yaml

```yaml
minio:
  endpoint: "localhost:9000"
  access_key: "minioadmin"
  secret_key: "minioadmin"
  secure: false

buckets:
  bronze: "bronze"
  silver: "silver"
  gold: "gold"
```

---

## 🗂️ Bronze Path Structure

```
bronze/
├── postgres_prod/
│   ├── customers/
│   │   └── year=2024/
│   │       └── month=02/
│   │           └── day=18/
│   │               ├── part-20240218_120000.parquet
│   │               └── _metadata.json
│   └── policies/
│       └── year=2024/...
└── mariadb_prod/
    └── claims/
        └── year=2024/...
```

---

## 📊 Bronze Metadata

Every row automatically gets these columns:

```python
{
    "_ingestion_timestamp": "2024-02-18T12:34:56.789Z",
    "_source_system": "postgres_prod",
    "_source_entity": "customers",
    "_schema_hash": "abc123def456",
    "_row_hash": "xyz789abc012",
    # Original columns...
}
```

---

## 📈 Metrics & Results

### Batch Result

```python
result = ingestor.ingest_batch(...)
# {
#     "success": True,
#     "rows_written": 5000,
#     "file_path": "bronze/postgres_prod/customers/year=2024/.../part-xxx.parquet",
#     "file_size_bytes": 245678,
#     "partition": "year=2024/month=02/day=18"
# }
```

### Connector Ingestion Result

```python
result = ingestor.ingest_from_connector(...)
# {
#     "success": True,
#     "total_rows": 125000,
#     "total_files": 25,
#     "total_bytes": 12345678,
#     "files_written": [...]
# }
```

### Orchestration Report

```python
report = run_ingestion()
# {
#     "orchestration_start": "2024-02-18T12:00:00Z",
#     "orchestration_end": "2024-02-18T12:05:30Z",
#     "elapsed_seconds": 330.5,
#     "total_jobs": 5,
#     "successful_jobs": 4,
#     "failed_jobs": 1,
#     "total_rows": 1250000,
#     "total_files": 125,
#     "total_size_mb": 485.2,
#     "jobs": [
#         {
#             "source": "postgres_prod",
#             "entity": "customers",
#             "status": "SUCCESS",
#             "rows": 50000,
#             "files": 10,
#             "duration_seconds": 45.2
#         }
#     ]
# }
```

---

## ⚙️ Configuration Options

### BronzeIngestor Options

```python
ingestor = BronzeIngestor(
    minio_config=config,
    bronze_bucket="bronze",           # Bucket name
    enable_validation=True,           # Validate before write
    enable_partitioning=True,         # Partition by year/month/day
    compression="snappy",             # snappy | gzip | zstd | brotli
    batch_size=10000,                # Rows per file
    write_metadata_sidecar=True      # Create _metadata.json
)
```

### Compression Options

| Option | Speed | Ratio | Use Case |
|--------|-------|-------|----------|
| `snappy` | ⚡⚡⚡ | 2-3x | Default, fast read/write |
| `gzip` | ⚡ | 3-4x | Better compression |
| `zstd` | ⚡⚡ | 3-5x | Modern, best balance |
| `brotli` | ⚡ | 4-5x | Best compression, slower |

---

## 🔄 Extraction Modes

### Full Extraction

```yaml
extraction:
  mode: full
  batch_size: 5000
```

Extracts all data from table.

### Incremental Extraction

```yaml
extraction:
  mode: incremental
  watermark_field: updated_at
  watermark_value: "2024-02-18T00:00:00"
  batch_size: 2000
```

Extracts only new/updated rows based on watermark field.

---

## 🛡️ Validation Rules

```yaml
validation_rules:
  # Not Null
  - type: not_null
    columns: [customer_id, name]
    severity: ERROR
  
  # Range
  - type: range
    column: age
    min: 0
    max: 120
    severity: WARNING
  
  # Pattern  
  - type: pattern
    column: email
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    severity: ERROR
  
  # Unique
  - type: unique
    columns: [customer_id]
    severity: ERROR
```

**Severities:**
- `ERROR`: Blocks ingestion
- `WARNING`: Logs warning, continues
- `INFO`: Informational only

---

## 🔧 Error Handling

### With Try-Catch

```python
try:
    result = ingestor.ingest_batch(df, "mydb", "customers")
    if result["success"]:
        print(f"✅ Ingested {result['rows_written']} rows")
    else:
        print(f"❌ Failed: {result['error']}")
except Exception as e:
    print(f"❌ Exception: {e}")
```

### Orchestrator Error Handling

```python
report = run_ingestion()

for job in report["jobs"]:
    if job["status"] == "FAILED":
        print(f"❌ {job['source']}/{job['entity']}: {job['error']}")
    elif job["status"] == "SUCCESS":
        print(f"✅ {job['source']}/{job['entity']}: {job['rows']:,} rows")
```

---

## 📝 Common Patterns

### Pattern 1: Quick Test Ingestion

```python
from ingestion import BronzeIngestor
import pandas as pd

df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

ingestor = BronzeIngestor(minio_config)
result = ingestor.ingest_batch(df, "test", "data")
```

### Pattern 2: Validated Production Ingestion

```python
from ingestion import BronzeIngestor, create_bronze_validator

validator = create_bronze_validator("customers", validation_rules)

ingestor = BronzeIngestor(minio_config, enable_validation=True)
result = ingestor.ingest_batch(df, "prod", "customers", validator=validator)
```

### Pattern 3: Full Pipeline

```python
from connectors import PostgresConnector
from ingestion import BronzeIngestor

# Extract
connector = PostgresConnector(db_config)
connector.connect()

# Ingest
ingestor = BronzeIngestor(minio_config)
result = ingestor.ingest_from_connector(
    connector=connector,
    extraction_plan={
        "source_system": "postgres_prod",
        "entity": "customers",
        "mode": "full",
        "batch_size": 5000
    }
)

connector.disconnect()
```

### Pattern 4: Scheduled Orchestration

```python
from ingestion import run_ingestion
import schedule
import time

def daily_ingestion():
    report = run_ingestion()
    print(f"Ingested {report['total_rows']:,} rows")

schedule.every().day.at("02:00").do(daily_ingestion)

while True:
    schedule.run_pending()
    time.sleep(60)
```

---

## 🎯 CLI Commands (Coming in STEP 6)

Future CLI interface:

```bash
# Run all ingestion
python -m syniq ingest

# Specific source
python -m syniq ingest --source postgres_prod

# Specific entity
python -m syniq ingest --entity customers

# Both filters
python -m syniq ingest --source postgres_prod --entity customers

# Generate report
python -m syniq ingest --report reports/ingestion.json
```

---

## 🐛 Troubleshooting

### Issue: MinIO connection failed

```python
# Check MinIO config
from minio import Minio

client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# Test connection
buckets = client.list_buckets()
print([b.name for b in buckets])
```

### Issue: Validation blocking writes

```python
# Check validation results
validator.validate(df)
results = validator.get_results()

for result in results["rule_results"]:
    if result["passed"] == False:
        print(f"{result['rule_type']}: {result['message']}")
```

### Issue: No data extracted

```python
# Check connector
connector.connect()
result = connector.test_connection()
print(result)

# Check extraction plan
plan = {
    "source_system": "mydb",
    "entity": "customers",
    "mode": "full",
    "batch_size": 1000
}

for batch in connector.extract(plan):
    print(f"Batch: {len(batch['data'])} rows")
    break
```

---

## 📚 Related Documentation

- **STEP 3:** [Connectors](STEP_3_CONNECTORS_SUMMARY.md)
- **STEP 4:** [Utils](STEP_4_UTILS_SUMMARY.md)
- **STEP 5:** [Full Summary](STEP_5_INGESTION_SUMMARY.md)
- **Examples:** [ingestion_usage_example.py](examples/ingestion_usage_example.py)

---

## ✅ Checklist

Before running ingestion:

- [ ] MinIO is running and accessible
- [ ] Config files are present (db_config, ingestion_config, minio_config)
- [ ] Connectors are configured correctly
- [ ] Bronze bucket exists in MinIO
- [ ] Validation rules are defined (if needed)
- [ ] Extraction plans are correct

---

**Status:** STEP 5 ✅ COMPLETE  
**Next:** STEP 6 (Main Application & CLI)

---

*Quick Reference Guide - STEP 5*  
*Syniq Data Lakehouse Project*
