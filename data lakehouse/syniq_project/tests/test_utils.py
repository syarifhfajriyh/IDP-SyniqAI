"""
Test suite for utils modules
=============================
Tests for logger, retry, metadata, and validator utilities.
"""

import sys
import os
import tempfile
import time
from datetime import datetime

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

import pandas as pd
import numpy as np

# Import utils modules
from utils.logger import (
    SyniqLogger,
    LoggerConfig,
    get_logger,
    LogContext,
    log_execution_time
)
from utils.retry import (
    retry,
    RetryError,
    RetryConfig,
    retry_on_connection_error
)
from utils.metadata import (
    detect_schema,
    compute_schema_hash,
    compute_row_hash,
    add_bronze_metadata,
    compare_schemas,
    extract_metadata_stats,
    add_partition_columns,
    deduplicate_by_hash
)
from utils.validator import (
    DataValidator,
    ValidationSeverity,
    NotNullRule,
    TypeValidationRule,
    RangeValidationRule,
    PatternValidationRule,
    UniqueRule
)


# ============================================================
# LOGGER TESTS
# ============================================================

def test_logger_initialization():
    """Test logger initialization"""
    print("\n" + "="*60)
    print("TEST: Logger Initialization")
    print("="*60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        config = LoggerConfig(
            log_dir=tmpdir,
            log_level="DEBUG",
            enable_console=True,
            enable_file=True
        )
        
        # Force re-initialization
        SyniqLogger._initialized = False
        SyniqLogger.initialize(config)
        
        logger = get_logger()
        logger.info("Logger test message")
        
        # Check log files were created
        log_files = os.listdir(tmpdir)
        assert len(log_files) > 0, "Log files should be created"
        
        print("✅ Logger initialization successful")


def test_log_context():
    """Test context-aware logging"""
    print("\n" + "="*60)
    print("TEST: Log Context")
    print("="*60)
    
    logger = get_logger()
    
    with LogContext(source_system="postgres", entity="customers"):
        logger.info("Processing with context")
    
    print("✅ Log context works")


def test_execution_time_decorator():
    """Test execution time logging decorator"""
    print("\n" + "="*60)
    print("TEST: Execution Time Decorator")
    print("="*60)
    
    @log_execution_time(level="INFO")
    def slow_function():
        time.sleep(0.1)
        return "done"
    
    result = slow_function()
    assert result == "done", "Function should return correct value"
    
    print("✅ Execution time decorator works")


# ============================================================
# RETRY TESTS
# ============================================================

def test_retry_success_after_failures():
    """Test retry succeeds after initial failures"""
    print("\n" + "="*60)
    print("TEST: Retry Success After Failures")
    print("="*60)
    
    attempt_count = [0]
    
    @retry(max_retries=3, base_delay=0.1)
    def flaky_function():
        attempt_count[0] += 1
        if attempt_count[0] < 3:
            raise ConnectionError("Temporary failure")
        return "success"
    
    result = flaky_function()
    assert result == "success", "Should succeed after retries"
    assert attempt_count[0] == 3, "Should have attempted 3 times"
    
    print(f"✅ Retry succeeded after {attempt_count[0]} attempts")


def test_retry_exhaustion():
    """Test retry exhaustion raises RetryError"""
    print("\n" + "="*60)
    print("TEST: Retry Exhaustion")
    print("="*60)
    
    @retry(max_retries=2, base_delay=0.1)
    def always_fails():
        raise ValueError("Always fails")
    
    try:
        always_fails()
        assert False, "Should have raised RetryError"
    except RetryError as e:
        assert e.attempts == 3, "Should have 3 attempts (initial + 2 retries)"
        print(f"✅ Retry exhausted after {e.attempts} attempts")


def test_retry_specific_exceptions():
    """Test retry only on specific exception types"""
    print("\n" + "="*60)
    print("TEST: Retry Specific Exceptions")
    print("="*60)
    
    @retry(max_retries=2, base_delay=0.1, retry_on=(ConnectionError, TimeoutError))
    def selective_retry():
        raise ValueError("Non-retryable error")
    
    try:
        selective_retry()
        assert False, "Should have raised ValueError"
    except ValueError:
        print("✅ Non-retryable exception raised immediately")


def test_exponential_backoff():
    """Test exponential backoff calculation"""
    print("\n" + "="*60)
    print("TEST: Exponential Backoff")
    print("="*60)
    
    config = RetryConfig(
        base_delay=1.0,
        exponential_base=2.0,
        max_delay=10.0,
        jitter=False
    )
    
    delays = [config.calculate_delay(i) for i in range(5)]
    expected = [1.0, 2.0, 4.0, 8.0, 10.0]  # Last capped at max_delay
    
    assert delays == expected, f"Expected {expected}, got {delays}"
    print(f"✅ Exponential backoff: {delays}")


# ============================================================
# METADATA TESTS
# ============================================================

def test_schema_detection():
    """Test schema detection from DataFrame"""
    print("\n" + "="*60)
    print("TEST: Schema Detection")
    print("="*60)
    
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
        "score": [92.5, 88.0, 95.5],
        "active": [True, False, True],
        "created_at": pd.date_range("2024-01-01", periods=3)
    })
    
    schema = detect_schema(df)
    
    assert schema.columns["id"] == "integer", "id should be integer"
    assert schema.columns["name"] == "string", "name should be string"
    assert schema.columns["age"] == "integer", "age should be integer"
    assert schema.columns["score"] == "float", "score should be float"
    assert schema.columns["active"] == "boolean", "active should be boolean"
    assert schema.columns["created_at"] == "timestamp", "created_at should be timestamp"
    assert schema.row_count == 3, "Should have 3 rows"
    
    print(f"✅ Schema detected: {schema.columns}")


def test_schema_hashing():
    """Test schema hash determinism"""
    print("\n" + "="*60)
    print("TEST: Schema Hashing")
    print("="*60)
    
    df = pd.DataFrame({
        "col1": [1, 2, 3],
        "col2": ["a", "b", "c"]
    })
    
    schema1 = detect_schema(df)
    schema2 = detect_schema(df)
    
    hash1 = compute_schema_hash(schema1)
    hash2 = compute_schema_hash(schema2)
    
    assert hash1 == hash2, "Same schema should produce same hash"
    assert len(hash1) == 64, "SHA256 hash should be 64 characters"
    
    print(f"✅ Schema hash: {hash1[:16]}... (deterministic)")


def test_row_hashing():
    """Test row hash computation"""
    print("\n" + "="*60)
    print("TEST: Row Hashing")
    print("="*60)
    
    df = pd.DataFrame({
        "id": [1, 1],  # Same values
        "name": ["Alice", "Alice"]
    })
    
    hash1 = compute_row_hash(df.iloc[0])
    hash2 = compute_row_hash(df.iloc[1])
    
    assert hash1 == hash2, "Identical rows should have same hash"
    assert len(hash1) == 64, "SHA256 hash should be 64 characters"
    
    print(f"✅ Row hash: {hash1[:16]}... (deterministic)")


def test_bronze_metadata_addition():
    """Test bronze metadata column addition"""
    print("\n" + "="*60)
    print("TEST: Bronze Metadata Addition")
    print("="*60)
    
    df = pd.DataFrame({
        "customer_id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"]
    })
    
    df_bronze = add_bronze_metadata(
        df,
        source_system="postgres_prod",
        source_entity="customers",
        ingestion_id="ing_001",
        schema_version="abc123",
        add_row_hash=True
    )
    
    assert "_ingestion_timestamp" in df_bronze.columns, "Should have _ingestion_timestamp"
    assert "_source_system" in df_bronze.columns, "Should have _source_system"
    assert "_source_entity" in df_bronze.columns, "Should have _source_entity"
    assert "_ingestion_id" in df_bronze.columns, "Should have _ingestion_id"
    assert "_schema_version" in df_bronze.columns, "Should have _schema_version"
    assert "_row_hash" in df_bronze.columns, "Should have _row_hash"
    
    assert df_bronze["_source_system"].iloc[0] == "postgres_prod"
    assert df_bronze["_source_entity"].iloc[0] == "customers"
    
    print("✅ Bronze metadata added successfully")


def test_schema_comparison():
    """Test schema evolution detection"""
    print("\n" + "="*60)
    print("TEST: Schema Comparison")
    print("="*60)
    
    df_old = pd.DataFrame({
        "id": [1, 2],
        "name": ["Alice", "Bob"],
        "age": [25, 30]
    })
    
    df_new = pd.DataFrame({
        "id": [1, 2],
        "name": ["Alice", "Bob"],
        "email": ["alice@example.com", "bob@example.com"]  # Added
        # age removed
    })
    
    old_schema = detect_schema(df_old)
    new_schema = detect_schema(df_new)
    comparison = compare_schemas(old_schema, new_schema)
    
    assert "email" in comparison.added_columns, "Should detect added column"
    assert "age" in comparison.removed_columns, "Should detect removed column"
    assert not comparison.is_compatible, "Removed columns make schema incompatible"
    
    print(f"✅ Schema comparison: Added={comparison.added_columns}, Removed={comparison.removed_columns}")


def test_partition_columns():
    """Test partition column addition"""
    print("\n" + "="*60)
    print("TEST: Partition Columns")
    print("="*60)
    
    df = pd.DataFrame({
        "_ingestion_timestamp": pd.date_range("2024-01-15", periods=3)
    })
    
    df_partitioned = add_partition_columns(df, "_ingestion_timestamp")
    
    assert "_ingestion_timestamp_year" in df_partitioned.columns
    assert "_ingestion_timestamp_month" in df_partitioned.columns
    assert "_ingestion_timestamp_day" in df_partitioned.columns
    
    assert df_partitioned["_ingestion_timestamp_year"].iloc[0] == 2024
    assert df_partitioned["_ingestion_timestamp_month"].iloc[0] == 1
    assert df_partitioned["_ingestion_timestamp_day"].iloc[0] == 15
    
    print("✅ Partition columns added successfully")


def test_deduplication():
    """Test deduplication by hash"""
    print("\n" + "="*60)
    print("TEST: Deduplication")
    print("="*60)
    
    df = pd.DataFrame({
        "id": [1, 2, 3, 1],  # Duplicate
        "name": ["Alice", "Bob", "Charlie", "Alice"],
        "_row_hash": ["aaa", "bbb", "ccc", "aaa"]  # Duplicate hash
    })
    
    df_deduped, dup_count = deduplicate_by_hash(df, "_row_hash")
    
    assert len(df_deduped) == 3, "Should have 3 unique rows"
    assert dup_count == 1, "Should remove 1 duplicate"
    
    print(f"✅ Deduplication: Removed {dup_count} duplicates")


# ============================================================
# VALIDATOR TESTS
# ============================================================

def test_not_null_validation():
    """Test not-null validation rule"""
    print("\n" + "="*60)
    print("TEST: Not-Null Validation")
    print("="*60)
    
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", None, "Charlie"]
    })
    
    rule = NotNullRule(columns="name", threshold=0.0)
    result = rule.validate(df)
    
    assert not result.passed, "Should fail with null values"
    assert result.affected_rows == 1, "Should have 1 null value"
    
    print("✅ Not-null validation works")


def test_type_validation():
    """Test type validation rule"""
    print("\n" + "="*60)
    print("TEST: Type Validation")
    print("="*60)
    
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "age": [25, 30, 35],
        "active": [True, False, True]
    })
    
    rule = TypeValidationRule({
        "id": "integer",
        "age": "integer",
        "active": "boolean"
    })
    result = rule.validate(df)
    
    assert result.passed, "All types should match"
    
    print("✅ Type validation works")


def test_range_validation():
    """Test range validation rule"""
    print("\n" + "="*60)
    print("TEST: Range Validation")
    print("="*60)
    
    df = pd.DataFrame({
        "age": [25, 30, 150, 40]  # 150 is out of range
    })
    
    rule = RangeValidationRule(column="age", min_value=0, max_value=120)
    result = rule.validate(df)
    
    assert not result.passed, "Should fail with out-of-range values"
    assert result.affected_rows == 1, "Should have 1 violation"
    
    print("✅ Range validation works")


def test_pattern_validation():
    """Test pattern validation rule"""
    print("\n" + "="*60)
    print("TEST: Pattern Validation")
    print("="*60)
    
    df = pd.DataFrame({
        "email": ["alice@example.com", "bob@example.com", "invalid-email"]
    })
    
    rule = PatternValidationRule(
        column="email",
        pattern=r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    )
    result = rule.validate(df)
    
    assert not result.passed, "Should fail with invalid pattern"
    assert result.affected_rows == 1, "Should have 1 violation"
    
    print("✅ Pattern validation works")


def test_unique_validation():
    """Test uniqueness validation rule"""
    print("\n" + "="*60)
    print("TEST: Unique Validation")
    print("="*60)
    
    df = pd.DataFrame({
        "id": [1, 2, 3, 1]  # Duplicate
    })
    
    rule = UniqueRule(columns="id")
    result = rule.validate(df)
    
    assert not result.passed, "Should fail with duplicates"
    assert result.affected_rows == 2, "Should have 2 duplicates"
    
    print("✅ Unique validation works")


def test_data_validator_integration():
    """Test DataValidator with multiple rules"""
    print("\n" + "="*60)
    print("TEST: DataValidator Integration")
    print("="*60)
    
    df = pd.DataFrame({
        "id": [1, 2, 3, 4],
        "name": ["Alice", "Bob", None, "David"],
        "age": [25, 30, 35, 150],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com", "invalid"]
    })
    
    validator = DataValidator("customers")
    validator.add_not_null(["id", "name"], threshold=0.0, severity=ValidationSeverity.ERROR)
    validator.add_unique("id", severity=ValidationSeverity.ERROR)
    validator.add_range_validation("age", min_value=0, max_value=120, severity=ValidationSeverity.WARNING)
    validator.add_pattern_validation(
        "email",
        r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        severity=ValidationSeverity.WARNING
    )
    
    report = validator.validate(df)
    
    assert report.total_rows == 4, "Should have 4 rows"
    assert len(report.results) == 4, "Should have 4 validation results"
    assert report.has_errors() or report.has_warnings(), "Should have some failures"
    
    summary = report.summary()
    print(f"   Total rules: {summary['total_rules']}")
    print(f"   Passed: {summary['passed_rules']}")
    print(f"   Failed: {summary['failed_rules']}")
    print(f"   Errors: {summary['errors']}")
    print(f"   Warnings: {summary['warnings']}")
    
    print("✅ DataValidator integration works")


# ============================================================
# RUN ALL TESTS
# ============================================================

if __name__ == "__main__":
    print("\n" + "🔬"*30)
    print("SYNIQ UTILS TEST SUITE")
    print("🔬"*30)
    
    test_functions = [
        # Logger tests
        test_logger_initialization,
        test_log_context,
        test_execution_time_decorator,
        
        # Retry tests
        test_retry_success_after_failures,
        test_retry_exhaustion,
        test_retry_specific_exceptions,
        test_exponential_backoff,
        
        # Metadata tests
        test_schema_detection,
        test_schema_hashing,
        test_row_hashing,
        test_bronze_metadata_addition,
        test_schema_comparison,
        test_partition_columns,
        test_deduplication,
        
        # Validator tests
        test_not_null_validation,
        test_type_validation,
        test_range_validation,
        test_pattern_validation,
        test_unique_validation,
        test_data_validator_integration
    ]
    
    passed = 0
    failed = 0
    
    for test_func in test_functions:
        try:
            test_func()
            passed += 1
        except AssertionError as e:
            print(f"❌ FAILED: {test_func.__name__}")
            print(f"   Error: {e}")
            failed += 1
        except Exception as e:
            print(f"❌ ERROR: {test_func.__name__}")
            print(f"   Exception: {e}")
            failed += 1
    
    print("\n" + "🔬"*30)
    print(f"TEST RESULTS: {passed} passed, {failed} failed")
    if failed == 0:
        print("✅ ALL TESTS PASSED!")
    else:
        print(f"❌ {failed} TEST(S) FAILED")
    print("🔬"*30 + "\n")
    
    sys.exit(0 if failed == 0 else 1)
