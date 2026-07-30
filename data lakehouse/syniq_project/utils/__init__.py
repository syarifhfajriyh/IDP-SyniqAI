"""
Utility modules for Syniq Lakehouse.

Available modules:
- config_loader: Configuration loading and validation
- logger: Structured logging setup
- metadata: Schema detection and metadata generation
- validator: Data validation utilities
- retry: Retry decorators for resilience
"""

from .config_loader import ConfigLoader, load_config
from .logger import (
    SyniqLogger,
    get_logger,
    LogContext,
    LoggerConfig,
    log_execution_time,
    log_batch_processing,
    log_extraction_summary,
    log_ingestion_summary,
    log_validation_results,
    log_schema_change
)
from .retry import (
    retry,
    async_retry,
    RetryConfig,
    RetryError,
    retry_on_connection_error,
    retry_on_network_error,
    retry_on_s3_error
)
from .metadata import (
    SchemaInfo,
    SchemaComparison,
    detect_schema,
    compute_schema_hash,
    compute_row_hash,
    add_bronze_metadata,
    compare_schemas,
    extract_metadata_stats,
    generate_metadata_json,
    add_partition_columns,
    deduplicate_by_hash
)
from .validator import (
    DataValidator,
    ValidationRule,
    ValidationResult,
    ValidationReport,
    ValidationSeverity,
    NotNullRule,
    TypeValidationRule,
    RangeValidationRule,
    PatternValidationRule,
    UniqueRule,
    CustomRule
)

__all__ = [
    # Config
    "ConfigLoader",
    "load_config",
    # Logger
    "SyniqLogger",
    "get_logger",
    "LogContext",
    "LoggerConfig",
    "log_execution_time",
    "log_batch_processing",
    "log_extraction_summary",
    "log_ingestion_summary",
    "log_validation_results",
    "log_schema_change",
    # Retry
    "retry",
    "async_retry",
    "RetryConfig",
    "RetryError",
    "retry_on_connection_error",
    "retry_on_network_error",
    "retry_on_s3_error",
    # Metadata
    "SchemaInfo",
    "SchemaComparison",
    "detect_schema",
    "compute_schema_hash",
    "compute_row_hash",
    "add_bronze_metadata",
    "compare_schemas",
    "extract_metadata_stats",
    "generate_metadata_json",
    "add_partition_columns",
    "deduplicate_by_hash",
    # Validator
    "DataValidator",
    "ValidationRule",
    "ValidationResult",
    "ValidationReport",
    "ValidationSeverity",
    "NotNullRule",
    "TypeValidationRule",
    "RangeValidationRule",
    "PatternValidationRule",
    "UniqueRule",
    "CustomRule",
]

__version__ = "0.1.0"