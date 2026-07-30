# Syniq Data Lakehouse

<div align="center">

**Modern Data Lakehouse Platform**  
*MinIO • Apache Iceberg • Delta Lake • Medallion Architecture*

![Status](https://img.shields.io/badge/status-production--ready-green)
![Python](https://img.shields.io/badge/python-3.12+-blue)
![License](https://img.shields.io/badge/license-MIT-blue)

</div>

---

## 🎯 Overview

Syniq Data Lakehouse is a production-ready data platform that implements a modern medallion architecture (Bronze/Silver/Gold) with MinIO object storage. The system provides:

- ✅ **Multi-Source Data Ingestion** - PostgreSQL, MariaDB, MongoDB support
- ✅ **Bronze Layer** - Raw data storage with metadata enrichment
- ✅ **Data Validation** - Configurable validation rules (not_null, range, pattern, unique)
- ✅ **Orchestration** - Configuration-driven multi-source ingestion
- ✅ **CLI Interface** - Complete command-line tools for operations
- ✅ **Partitioning** - Hive-style partitioning (year/month/day)
- ✅ **Retry Logic** - Automatic retry for transient failures
- ✅ **Logging** - Structured logging with rotation

---

## 📦 Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    SYNIQ DATA LAKEHOUSE                      │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│  STEP 6: CLI Application (main.py)                           │
│  Commands: ingest | test | validate | list | setup | status  │
└──────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────┐
│  STEP 5: Ingestion Layer                                     │
│  - BronzeIngestor (Parquet write to MinIO)                   │
│  - IngestionOrchestrator (Multi-source coordination)         │
│  - ConnectorFactory (Dynamic connector creation)             │
└──────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────┐
│  STEP 4: Utils Layer                                         │
│  - Logger (Structured logging with Loguru)                   │
│  - Retry (Exponential backoff retry logic)                   │
│  - Metadata (Bronze metadata enrichment)                     │
│  - Validator (Data quality validation)                       │
└──────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────┐
│  STEP 3: Connectors Layer                                    │
│  - PostgresConnector                                         │
│  - MariaDBConnector                                          │
│  - MariaDBCloudConnector (SSL support)                       │
│  - MongoDBConnector (Atlas support)                          │
└──────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────┐
│  STEP 1-2: Configuration Layer                               │
│  - db_config.yaml (Database credentials)                     │
│  - ingestion_config.yaml (Ingestion jobs)                    │
│  - minio_config.yaml (Storage configuration)                 │
└──────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────┐
│  Storage: MinIO (S3-Compatible)                              │
│  bronze/ silver/ gold/                                       │
└──────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

### 1. Prerequisites

- Python 3.12+
- MinIO Docker container or server
- Source databases (PostgreSQL, MariaDB, MongoDB)

### 2. Installation

```bash
# Clone repository
git clone <repository-url>

# Create virtual environment
python -m venv syniq_env

# Activate environment (Windows)
syniq_env\Scripts\activate

# Install dependencies
pip install -r syniq_project/requirements.txt
```

### 3. MinIO Setup

```bash
# Start MinIO (Docker)
docker run -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=admin" \
  -e "MINIO_ROOT_PASSWORD=password123" \
  minio/minio server /data --console-address ":9001"

# Create buckets
cd syniq_project
python setup_minio.py
```

### 4. Configuration

Edit configuration files in `config/`:

**db_config.yaml:**
```yaml
postgresql:
  host: localhost
  port: 5432
  database: mydb
  username: ${DB_USERNAME}
  password: ${DB_PASSWORD}
```

**ingestion_config.yaml:**
```yaml
sources:
  postgres_prod:
    enabled: true
    connector_type: postgres
    entities:
      customers:
        enabled: true
        extraction:
          mode: full
          batch_size: 5000
```

**minio_config.yaml:**
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

### 5. First Run

```bash
# Test connections
python main.py test

# Validate configurations
python main.py validate

# Run ingestion
python main.py ingest

# Check status
python main.py status
```

---

## 📋 CLI Commands

| Command | Description | Example |
|---------|-------------|---------|
| `ingest` | Run data ingestion | `python main.py ingest` |
| `test` | Test all connections | `python main.py test` |
| `validate` | Validate configurations | `python main.py validate` |
| `list` | List sources/entities | `python main.py list sources` |
| `setup` | Setup MinIO buckets | `python main.py setup` |
| `status` | Show system status | `python main.py status` |

### Ingestion Examples

```bash
# Run all sources
python main.py ingest

# Specific source
python main.py ingest --source postgres_prod

# Specific entity
python main.py ingest --entity customers

# With report
python main.py ingest --report reports/daily.json
```

---

## 🗂️ Project Structure

```
syniq_project/
├── main.py                         # Main CLI application
├── setup_minio.py                  # MinIO setup script
├── requirements.txt                # Python dependencies
│
├── config/                         # Configuration files
│   ├── db_config.yaml
│   ├── ingestion_config.yaml
│   └── minio_config.yaml
│
├── connectors/                     # Database connectors
│   ├── __init__.py
│   ├── base_connector.py
│   ├── postgres_connector.py
│   ├── mariadb_connector.py
│   ├── mariadb_cloud_connector.py
│   └── mongodb_connector.py
│
├── utils/                          # Utility modules
│   ├── __init__.py
│   ├── config_loader.py
│   ├── logger.py
│   ├── retry.py
│   ├── metadata.py
│   └── validator.py
│
├── ingestion/                      # Ingestion layer
│   ├── __init__.py
│   ├── bronze_ingestor.py
│   └── ingestion_orchestrator.py
│
├── examples/                       # Usage examples
│   ├── mongodb_usage_example.py
│   └── ingestion_usage_example.py
│
├── tests/                          # Unit tests
│   ├── test_connectors.py
│   ├── test_utils.py
│   └── test_config_loader.py
│
├── logs/                           # Application logs
│   ├── syniq_YYYY-MM-DD.log
│   └── syniq_YYYY-MM-DD.json
│
└── reports/                        # Ingestion reports
    └── ingestion_*.json
```

---

## 📊 Data Flow

```
┌─────────────────┐
│  Source DBs     │
│  - PostgreSQL   │
│  - MariaDB      │
│  - MongoDB      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   CONNECTORS    │
│  extract()      │
│  (Generator)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  VALIDATION     │
│  (Optional)     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ BRONZE INGESTOR │
│ - Add metadata  │
│ - Partition     │
│ - Write Parquet │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  MinIO Storage  │
│  bronze/        │
│  └── source/    │
│      └── entity/│
│          └── yr/│
└─────────────────┘
```

---

## 🎨 Features

### Bronze Layer Metadata

Every row automatically gets:
- `_ingestion_timestamp` - When data was ingested
- `_source_system` - Source database name
- `_source_entity` - Source table/collection name
- `_schema_hash` - Hash of schema structure
- `_row_hash` - Hash of row content

### Data Validation

Configurable validation rules:
```yaml
validation_rules:
  - type: not_null
    columns: [customer_id, name]
    severity: ERROR
  
  - type: range
    column: age
    min: 0
    max: 120
    severity: WARNING
  
  - type: pattern
    column: email
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    severity: ERROR
  
  - type: unique
    columns: [customer_id]
    severity: ERROR
```

### Partitioning

Hive-style partitioning for efficient querying:
```
bronze/
└── postgres_prod/
    └── customers/
        └── year=2024/
            └── month=02/
                └── day=18/
                    └── part-20240218_120000.parquet
```

### Retry Logic

Automatic retry with exponential backoff:
```python
@retry_on_s3_error(max_attempts=3, base_delay=1.0)
def write_to_minio():
    # Automatic retry on MinIO errors
    pass
```

---

## 📖 Documentation

- **[STEP 3 - Connectors](STEP_3_CONNECTORS_SUMMARY.md)** - Database connector implementation
- **[STEP 4 - Utils](STEP_4_UTILS_SUMMARY.md)** - Utility modules (logger, retry, metadata, validator)
- **[STEP 5 - Ingestion](STEP_5_INGESTION_SUMMARY.md)** - Bronze ingestion and orchestration
- **[STEP 6 - Main CLI](STEP_6_MAIN_CLI_SUMMARY.md)** - CLI application and commands

### Quick References

- **[STEP 4 Quick Reference](STEP_4_QUICK_REFERENCE.md)**
- **[STEP 5 Quick Reference](STEP_5_QUICK_REFERENCE.md)**
- **[STEP 6 Quick Reference](STEP_6_QUICK_REFERENCE.md)**

---

## 🧪 Testing

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_connectors.py

# Run with coverage
pytest --cov=. --cov-report=html tests/
```

---

## 📊 Project Statistics

| Component | Files | Lines | Purpose |
|-----------|-------|-------|---------|
| Configuration | 5 | 300 | YAML configs |
| Connectors | 5 | 2,000+ | Database drivers |
| Utils | 5 | 2,942 | Core utilities |
| Ingestion | 3 | 1,715 | Bronze layer |
| CLI | 2 | 1,100+ | Main application |
| **Total** | **20** | **~8,000** | **Production code** |

---

## 🔧 Configuration Management

### Environment Variables

Override config values using environment variables:

```powershell
# Windows PowerShell
$env:MINIO_ENDPOINT = "minio.example.com:9000"
$env:MINIO_ACCESS_KEY = "mykey"
$env:MINIO_SECRET_KEY = "mysecret"
$env:DB_USERNAME = "myuser"
$env:DB_PASSWORD = "mypassword"

python main.py ingest
```

### .env File Support

Create `.env` file in project root:
```env
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=password123

DB_USERNAME=myuser
DB_PASSWORD=mypassword
```

---

## 🚀 Deployment

### Docker Deployment (Future)

```dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY syniq_project /app
RUN pip install -r requirements.txt

CMD ["python", "main.py", "ingest"]
```

### Scheduled Execution

**Windows Task Scheduler:**
```powershell
schtasks /create /tn "Syniq Daily Ingestion" \
  /tr "C:\path\to\daily_ingest.bat" \
  /sc daily /st 02:00
```

**Linux Cron:**
```bash
0 2 * * * cd /path/to/syniq_project && python main.py ingest --report reports/daily_$(date +\%Y\%m\%d).json
```

---

## 🐛 Troubleshooting

### MinIO Connection Failed

```
❌ Connection failed: MaxRetryError(...)

Solution:
1. Check MinIO is running: docker ps
2. Verify endpoint in config/minio_config.yaml
3. Test access: curl http://localhost:9000
```

### Database Connection Failed

```
❌ Could not connect to database

Solution:
1. Verify database is running
2. Check credentials in db_config.yaml
3. Test connection: psql -h localhost -U user -d mydb
```

### Import Errors

```
ModuleNotFoundError: No module named 'minio'

Solution:
pip install -r requirements.txt
```

---

## 📝 TODOs & Future Enhancements

### Phase 2 (Silver Layer)
- [ ] Data transformation pipeline
- [ ] Schema evolution handling
- [ ] Data deduplication
- [ ] Slowly Changing Dimensions (SCD)

### Phase 3 (Gold Layer)
- [ ] Aggregated metrics
- [ ] Feature engineering
- [ ] ML-ready datasets
- [ ] Data marts

### Additional Features
- [ ] S3 connector for cloud storage
- [ ] Parallel ingestion (multi-process)
- [ ] Incremental watermark management
- [ ] Web UI for monitoring
- [ ] Email/Slack notifications
- [ ] Data lineage tracking
- [ ] Query layer (Spark/Trino)

---

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

---

## 📄 License

This project is licensed under the MIT License - see LICENSE file for details.

---

## 👥 Authors

- **Syniq Team** - *Initial work*

---

## 🙏 Acknowledgments

- Apache Iceberg community
- MinIO team
- Delta Lake contributors
- Python data community

---

## 📞 Support

For issues or questions:
- Open an issue on GitHub
- Email: support@syniq.example.com
- Documentation: See `docs/` directory

---

<div align="center">

**Built with ❤️ by Syniq Team**

*Data Engineering • Lakehouse Architecture • Production-Ready*

</div>
