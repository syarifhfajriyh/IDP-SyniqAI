# 📋 Syniq Data Lakehouse - Quick Reference

## 🚀 CLI Commands Cheat Sheet

### Catalog Commands
```powershell
# List all tables
python main.py catalog list

# Filter by source
python main.py catalog list --source postgres_prod

# Show table details
python main.py catalog info postgres_prod customers

# View schema history
python main.py catalog history postgres_prod customers
```

### Quality Commands
```powershell
# View quality report
python main.py quality report postgres_prod customers
```

### Lineage Commands
```powershell
# Show lineage
python main.py lineage show bronze postgres_prod/customers
```

### Ingestion Commands
```powershell
# Run all ingestions
python main.py ingest

# Run specific source
python main.py ingest --source postgres_prod

# Run specific entity
python main.py ingest --entity customers
```

---

## 📊 MinIO Buckets

| Bucket | Purpose | Example Path |
|--------|---------|-------------|
| `bronze` | Raw data | `bronze/postgres_prod/customers/year=2026/month=02/day=18/part-001.parquet` |
| `catalog` | Table metadata | `catalog/tables/postgres_prod/customers/current.json` |
| `quality` | Quality profiles | `quality/profiles/postgres_prod/customers/2026/02/18/profile.json` |
| `lineage` | Transformation lineage | `lineage/transformations/bronze/2026/02/18/postgres_prod_customers.json` |

---

## 🔑 Key Files

| File | Purpose |
|------|---------|
| `catalog/catalog_manager.py` | Metadata catalog management |
| `quality/profiler.py` | Data quality profiling |
| `catalog/schema_evolution.py` | Schema change detection |
| `lineage/lineage_tracker.py` | Data lineage tracking |
| `ingestion/bronze_ingestor.py` | Bronze layer ingestion (updated) |
| `main.py` | CLI entry point (updated) |

---

## 📝 Configuration Files

| File | Purpose |
|------|---------|
| `config/catalog_config.yaml` | Catalog settings |
| `config/quality_config.yaml` | Quality thresholds |
| `config/lineage_config.yaml` | Lineage tracking settings |
| `config/minio_config.yaml` | MinIO connection |
| `config/ingestion_config.yaml` | Ingestion jobs |

---

## 🧪 Testing

```powershell
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_catalog.py -v
pytest tests/test_quality.py -v
pytest tests/test_schema_evolution.py -v
pytest tests/test_lineage.py -v
```

---

## 🐛 Quick Troubleshooting

### MinIO Connection Issues
```powershell
# Check if MinIO is running
docker ps

# Restart MinIO
docker restart <container_id>

# Test connection
python -c "from minio import Minio; client = Minio('localhost:9000', access_key='admin', secret_key='password123', secure=False); print(client.list_buckets())"
```

### Check Logs
```powershell
# View latest logs
Get-Content logs\syniq_*.log -Tail 50
```

---

## 📦 Python API Quick Examples

### Catalog
```python
from catalog.catalog_manager import CatalogManager

catalog = CatalogManager(minio_client)
catalog.register_table(source, entity, schema, location, row_count)
table = catalog.get_table(source, entity)
tables = catalog.list_tables()
```

### Quality
```python
from quality.profiler import DataProfiler

profiler = DataProfiler(minio_client)
profile = profiler.profile_dataframe(df, source, entity)
print(f"Quality Score: {profile['quality_score']}")
```

### Lineage
```python
from lineage.lineage_tracker import LineageTracker

lineage = LineageTracker(minio_client)
lineage.record_transformation(source_layer, source_location, target_layer, target_location, transformation, row_count)
report = lineage.generate_lineage_report(layer, location)
```

---

## ✅ Post-Ingestion Checklist

After running ingestion, verify:

- [ ] Data written to `bronze/` bucket
- [ ] Table registered in `catalog/`
- [ ] Quality profile in `quality/`
- [ ] Lineage recorded in `lineage/`
- [ ] Quality score >= 70%
- [ ] No critical validation errors

---

## 🎯 Quality Score Interpretation

| Score | Status | Action |
|-------|--------|--------|
| 90-100 | Excellent ✅ | No action needed |
| 70-89 | Good ⚠️ | Monitor for issues |
| 50-69 | Poor ❌ | Investigate data issues |
| 0-49 | Critical 🔥 | Immediate investigation required |

---

## 📞 Quick Links

- **MinIO Console:** http://localhost:9001
- **MinIO API:** http://localhost:9000
- **Logs Directory:** `logs/`
- **Config Directory:** `config/`
- **Tests Directory:** `tests/`

---

## 🚀 Typical Workflow

```
1. Configure sources (config/ingestion_config.yaml)
   ↓
2. Run ingestion (python main.py ingest)
   ↓
3. Check catalog (python main.py catalog list)
   ↓
4. Review quality (python main.py quality report source entity)
   ↓
5. Verify lineage (python main.py lineage show bronze source/entity)
   ↓
6. Monitor MinIO buckets
```

---

**Updated:** 2026-02-18  
**Version:** 1.0 - Phase 1-4 Complete
