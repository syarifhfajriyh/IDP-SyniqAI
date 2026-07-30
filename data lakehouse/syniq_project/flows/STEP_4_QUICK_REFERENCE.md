# STEP 4 Utilities - Quick Reference Guide

## 🚀 Quick Start

```python
# Import utilities
from utils import (
    get_logger,              # Structured logging
    retry,                   # Retry decorator
    detect_schema,           # Schema detection
    add_bronze_metadata,     # Bronze metadata
    DataValidator            # Data validation
)
```

---

## 📝 Logger (utils/logger.py)

### Basic Logging
```python
from utils import get_logger

logger = get_logger("my_module")

logger.debug("Debug message")
logger.info("Info message")
logger.warning("Warning message")
logger.error("Error message")
```

### Structured Logging
```python
logger.info(
    "Processing batch",
    source_system="postgres",
    entity="customers",
    batch_index=5,
    rows=1000
)
```

### Context Manager
```python
from utils import LogContext

with LogContext(job_id="12345", source="postgres"):
    logger.info("Extracting data")
    # All logs include job_id and source
```

### Performance Tracking
```python
from utils import log_execution_time

@log_execution_time
def expensive_operation():
    # Your code here
    pass
```

### Specialized Logging
```python
from utils import (
    log_extraction_summary,
    log_ingestion_summary,
    log_validation_results,
    log_schema_change
)

# After extraction
log_extraction_summary(
    source_system="postgres",
    source_entity="customers",
    total_rows=10000,
    total_batches=10,
    elapsed_time=45.5
)

# After ingestion
log_ingestion_summary(
    source_system="postgres",
    source_entity="customers",
    zone="bronze",
    total_rows=10000,
    files_written=5,
    total_size_mb=128.5,
    elapsed_time=30.2
)
```

---

## 🔄 Retry (utils/retry.py)

### Simple Retry
```python
from utils import retry

@retry(max_retries=3, base_delay=1.0)
def flaky_database_query():
    # Query that might fail temporarily
    conn = connect_to_db()
    return conn.execute("SELECT ...")
```

### Advanced Configuration
```python
from utils import retry, RetryConfig

config = RetryConfig(
    max_retries=5,
    base_delay=2.0,
    max_delay=60.0,
    exponential_base=2.0,
    jitter=True,
    retry_on=(ConnectionError, TimeoutError)
)

@retry(config=config)
def critical_operation():
    pass
```

### Pre-configured Decorators
```python
from utils import (
    retry_on_connection_error,  # Database/network errors
    retry_on_network_error,     # HTTP/API errors
    retry_on_s3_error           # S3/MinIO errors
)

@retry_on_connection_error
def connect_to_database():
    return psycopg2.connect(...)

@retry_on_s3_error
def upload_to_minio():
    minio_client.put_object(...)
```

---

## 🔍 Metadata (utils/metadata.py)

### Schema Detection
```python
from utils import detect_schema
import pandas as pd

df = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", None],
    "balance": [1000.0, 2000.0, 3000.0]
})

schema = detect_schema(df)

print(schema.columns)           # {'id': 'int64', 'name': 'object', ...}
print(schema.row_count)         # 3
print(schema.nullable_columns)  # ['name']
```

### Schema Hashing
```python
from utils import compute_schema_hash

schema_hash = compute_schema_hash(df)
print(schema_hash)  # 'a3f5e8d9...' (deterministic)

# Use for change detection
if current_hash != previous_hash:
    print("Schema changed!")
```

### Row Hashing (for deduplication)
```python
from utils import compute_row_hash

df_with_hash = compute_row_hash(df, columns=["id", "name", "balance"])

# Now df has '_row_hash' column
duplicates = df_with_hash[df_with_hash.duplicated(subset="_row_hash")]
```

### Bronze Metadata Enrichment
```python
from utils import add_bronze_metadata

df_bronze = add_bronze_metadata(
    df=df,
    source_system="postgres_prod",
    source_entity="customers"
)

# Result includes:
# - _ingestion_timestamp
# - _source_system
# - _source_entity
# - _schema_hash
# - _row_hash
```

### Schema Comparison
```python
from utils import compare_schemas

old_schema = detect_schema(df_v1)
new_schema = detect_schema(df_v2)

comparison = compare_schemas(old_schema, new_schema)

if comparison.has_changes():
    print(f"Added: {comparison.added_columns}")
    print(f"Removed: {comparison.removed_columns}")
    print(f"Type changes: {comparison.type_changes}")
    print(f"Compatible: {comparison.is_compatible}")
```

### Partition Columns
```python
from utils import add_partition_columns

# Adds year, month, day columns from _ingestion_timestamp
df_partitioned = add_partition_columns(df_bronze)

# For Hive-style partitioning: year=2024/month=02/day=18/
```

### Deduplication
```python
from utils import deduplicate_by_hash

# Remove duplicates based on _row_hash
df_unique = deduplicate_by_hash(df_with_hash)

print(f"Removed {len(df_with_hash) - len(df_unique)} duplicates")
```

---

## ✅ Validator (utils/validator.py)

### Basic Validation
```python
from utils import DataValidator, ValidationSeverity

df = pd.DataFrame({
    "customer_id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", None, "Dave", "Eve"],
    "age": [25, 30, 35, 200, 45]
})

# Create validator
validator = DataValidator(entity="customers")

# Add rules
validator.add_not_null_rule(
    columns=["customer_id", "name"],
    threshold=0.0,  # 0% nulls allowed
    severity=ValidationSeverity.ERROR
)

validator.add_range_rule(
    column="age",
    min_value=0,
    max_value=120,
    severity=ValidationSeverity.WARNING
)

# Run validation
report = validator.validate(df)

# Check results
if report.has_errors():
    print("Validation failed!")
    for result in report.get_failures():
        print(f"❌ {result.rule_name}: {result.message}")
```

### Available Rules
```python
# Not null
validator.add_not_null_rule(
    columns=["id", "name"],
    threshold=0.05  # Allow 5% nulls
)

# Type validation
validator.add_type_rule(
    column="balance",
    expected_type="float64"
)

# Range validation
validator.add_range_rule(
    column="age",
    min_value=0,
    max_value=120
)

# Pattern validation (regex)
validator.add_pattern_rule(
    column="email",
    pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
    description="Valid email format"
)

# Uniqueness
validator.add_unique_rule(
    columns=["customer_id"]
)

# Custom validation
def validate_phone_number(df):
    # Custom logic
    invalid = df[df["phone"].str.len() != 10]
    return len(invalid) == 0, f"Found {len(invalid)} invalid phones"

validator.add_custom_rule(
    name="phone_validation",
    validation_func=validate_phone_number
)
```

### Validation Report
```python
report = validator.validate(df)

# Summary
print(f"Total rows: {report.total_rows}")
print(f"Rules executed: {len(report.results)}")
print(f"Failures: {len(report.get_failures())}")
print(f"Errors: {report.has_errors()}")
print(f"Warnings: {report.has_warnings()}")

# Get specific results
errors = report.get_by_severity(ValidationSeverity.ERROR)
warnings = report.get_by_severity(ValidationSeverity.WARNING)

# Export report
summary = report.summary()
print(summary)
```

---

## 🎯 Common Patterns

### Pattern 1: Extraction Pipeline
```python
from utils import get_logger, retry_on_connection_error
from connectors import PostgresConnector

logger = get_logger(__name__)

@retry_on_connection_error
def extract_customers():
    logger.info("Starting extraction", source="postgres", table="customers")
    
    connector = PostgresConnector(config)
    connector.connect()
    
    total_rows = 0
    for batch in connector.extract(extraction_plan):
        df = batch["data"]
        total_rows += len(df)
        logger.info("Extracted batch", rows=len(df), total=total_rows)
    
    logger.info("Extraction complete", total_rows=total_rows)
    return total_rows
```

### Pattern 2: Bronze Ingestion with Validation
```python
from utils import (
    get_logger,
    add_bronze_metadata,
    DataValidator,
    ValidationSeverity
)

logger = get_logger(__name__)

def ingest_to_bronze(df, source_system, source_entity):
    # Validate
    validator = DataValidator(entity=source_entity)
    validator.add_not_null_rule(["id"])
    
    report = validator.validate(df)
    
    if report.has_errors():
        logger.error("Validation failed", entity=source_entity)
        raise ValueError("Data quality check failed")
    
    # Add metadata
    df_bronze = add_bronze_metadata(
        df=df,
        source_system=source_system,
        source_entity=source_entity
    )
    
    logger.info("Bronze metadata added", rows=len(df_bronze))
    
    # Write to MinIO (STEP 5)
    return df_bronze
```

### Pattern 3: Schema Evolution Handling
```python
from utils import detect_schema, compare_schemas, get_logger

logger = get_logger(__name__)

def handle_schema_evolution(df_new, previous_schema_hash):
    # Detect new schema
    new_schema = detect_schema(df_new)
    new_hash = compute_schema_hash(df_new)
    
    # Check for changes
    if new_hash != previous_schema_hash:
        # Load previous schema from metadata store
        comparison = compare_schemas(previous_schema, new_schema)
        
        if comparison.has_changes():
            logger.warning(
                "Schema changed",
                added=comparison.added_columns,
                removed=comparison.removed_columns
            )
            
            if not comparison.is_compatible:
                # Breaking change!
                logger.error("Breaking schema change detected")
                # Alert, create new table version, etc.
            else:
                # Additive change (safe)
                logger.info("Schema evolved (compatible)")
    
    return new_schema
```

---

## 📚 Full API Reference

See individual module documentation:
- [logger.py](utils/logger.py) - Lines 1-561
- [retry.py](utils/retry.py) - Lines 1-507
- [metadata.py](utils/metadata.py) - Lines 1-588
- [validator.py](utils/validator.py) - Lines 1-695

---

## 🧪 Testing

Run test suite:
```bash
python tests/test_utils.py
```

Test specific module:
```bash
python -c "from utils import get_logger; logger = get_logger('test'); logger.info('Works!'); print('✅ Logger OK')"
```

---

**Last Updated:** February 18, 2026  
**Status:** All utilities tested and production-ready ✅
