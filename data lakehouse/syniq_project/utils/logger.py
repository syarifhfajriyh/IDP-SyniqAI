"""
Structured Logging with Loguru
================================
Provides centralized logging configuration with context-aware logging capabilities.

Features:
- Structured logging with JSON serialization
- Log rotation and compression
- Context propagation (source_system, entity, batch_id)
- Performance tracking (execution time, row counts)
- Integration with connectors and ingestors
- Multiple log levels and handlers
"""

import sys
import os
from pathlib import Path
from typing import Optional, Dict, Any, Callable
from functools import wraps
from datetime import datetime
import time

from loguru import logger


# Global context storage for thread-safe context propagation
_context_store = {}


class LoggerConfig:
    """Configuration for Syniq logger"""
    
    def __init__(
        self,
        log_dir: str = "logs",
        log_level: str = "INFO",
        rotation: str = "100 MB",
        retention: str = "30 days",
        compression: str = "zip",
        format_type: str = "detailed",  # "detailed", "simple", "json"
        enable_console: bool = True,
        enable_file: bool = True
    ):
        """
        Initialize logger configuration
        
        Args:
            log_dir: Directory for log files
            log_level: Minimum log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            rotation: When to rotate logs (e.g., "100 MB", "1 day")
            retention: How long to keep logs (e.g., "30 days")
            compression: Compression format for rotated logs
            format_type: Log format style
            enable_console: Enable console logging
            enable_file: Enable file logging
        """
        self.log_dir = log_dir
        self.log_level = log_level
        self.rotation = rotation
        self.retention = retention
        self.compression = compression
        self.format_type = format_type
        self.enable_console = enable_console
        self.enable_file = enable_file
        
        # Ensure log directory exists
        Path(self.log_dir).mkdir(parents=True, exist_ok=True)


class SyniqLogger:
    """Syniq Lakehouse structured logger with context awareness"""
    
    _initialized = False
    _config: Optional[LoggerConfig] = None
    
    @classmethod
    def initialize(cls, config: Optional[LoggerConfig] = None):
        """
        Initialize the global logger with configuration
        
        Args:
            config: Logger configuration (uses defaults if None)
        """
        if cls._initialized:
            logger.debug("Logger already initialized, skipping")
            return
        
        cls._config = config or LoggerConfig()
        
        # Remove default handler
        logger.remove()
        
        # Add console handler
        if cls._config.enable_console:
            console_format = cls._get_format(cls._config.format_type, is_console=True)
            logger.add(
                sys.stdout,
                format=console_format,
                level=cls._config.log_level,
                colorize=True,
                backtrace=True,
                diagnose=True
            )
        
        # Add file handlers
        if cls._config.enable_file:
            # All logs
            all_logs_path = os.path.join(cls._config.log_dir, "syniq_{time:YYYY-MM-DD}.log")
            file_format = cls._get_format(cls._config.format_type, is_console=False)
            
            logger.add(
                all_logs_path,
                format=file_format,
                level=cls._config.log_level,
                rotation=cls._config.rotation,
                retention=cls._config.retention,
                compression=cls._config.compression,
                backtrace=True,
                diagnose=True
            )
            
            # Error logs (separate file)
            error_logs_path = os.path.join(cls._config.log_dir, "syniq_errors_{time:YYYY-MM-DD}.log")
            logger.add(
                error_logs_path,
                format=file_format,
                level="ERROR",
                rotation=cls._config.rotation,
                retention=cls._config.retention,
                compression=cls._config.compression,
                backtrace=True,
                diagnose=True
            )
        
        cls._initialized = True
        logger.info(f"Syniq Logger initialized | Level: {cls._config.log_level} | Dir: {cls._config.log_dir}")
    
    @staticmethod
    def _get_format(format_type: str, is_console: bool) -> str:
        """Get log format string based on type"""
        
        if format_type == "simple":
            return "<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"
        
        elif format_type == "json":
            # JSON format for machine parsing
            return "{message}"
        
        else:  # detailed (default)
            if is_console:
                return (
                    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                    "<level>{level: <8}</level> | "
                    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                    "<level>{message}</level>"
                )
            else:
                return (
                    "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
                    "{level: <8} | "
                    "{name}:{function}:{line} | "
                    "{message}"
                )
    
    @classmethod
    def get_logger(cls) -> "logger":
        """
        Get the configured logger instance
        
        Returns:
            Configured loguru logger
        """
        if not cls._initialized:
            cls.initialize()
        return logger


class LogContext:
    """Context manager for adding structured context to logs"""
    
    def __init__(self, **context):
        """
        Initialize log context
        
        Args:
            **context: Key-value pairs for context (e.g., source_system="postgres", entity="customers")
        """
        self.context = context
        self.token = None
    
    def __enter__(self):
        """Enter context"""
        self.token = logger.contextualize(**self.context)
        self.token.__enter__()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context"""
        if self.token:
            self.token.__exit__(exc_type, exc_val, exc_tb)


def log_execution_time(func: Optional[Callable] = None, *, level: str = "INFO", extra_context: Optional[Dict[str, Any]] = None):
    """
    Decorator to log function execution time
    
    Args:
        func: Function to decorate
        level: Log level (INFO, DEBUG, etc.)
        extra_context: Additional context to include in logs
    
    Example:
        @log_execution_time
        def my_function():
            pass
        
        @log_execution_time(level="DEBUG", extra_context={"module": "ingestion"})
        def another_function():
            pass
    """
    def decorator(f: Callable) -> Callable:
        @wraps(f)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            func_name = f.__name__
            
            context = extra_context or {}
            context["function"] = func_name
            
            with LogContext(**context):
                log_func = getattr(logger, level.lower(), logger.info)
                log_func(f"Starting {func_name}")
                
                try:
                    result = f(*args, **kwargs)
                    elapsed = time.time() - start_time
                    log_func(f"Completed {func_name} | Elapsed: {elapsed:.2f}s")
                    return result
                
                except Exception as e:
                    elapsed = time.time() - start_time
                    logger.error(f"Failed {func_name} | Elapsed: {elapsed:.2f}s | Error: {e}")
                    raise
        
        return wrapper
    
    if func is None:
        return decorator
    else:
        return decorator(func)


def log_batch_processing(
    source_system: str,
    source_entity: str,
    batch_index: int,
    total_batches: Optional[int] = None,
    row_count: int = 0,
    level: str = "INFO"
):
    """
    Log batch processing progress
    
    Args:
        source_system: Source system name (e.g., "postgres_prod")
        source_entity: Entity name (e.g., "customers")
        batch_index: Current batch number
        total_batches: Total number of batches (if known)
        row_count: Number of rows in batch
        level: Log level
    """
    context = {
        "source_system": source_system,
        "source_entity": source_entity,
        "batch_index": batch_index
    }
    
    if total_batches:
        progress = (batch_index / total_batches) * 100
        message = (
            f"Processing batch {batch_index}/{total_batches} ({progress:.1f}%) | "
            f"Rows: {row_count:,} | "
            f"Entity: {source_system}.{source_entity}"
        )
    else:
        message = (
            f"Processing batch {batch_index} | "
            f"Rows: {row_count:,} | "
            f"Entity: {source_system}.{source_entity}"
        )
    
    with LogContext(**context):
        log_func = getattr(logger, level.lower(), logger.info)
        log_func(message)


def log_extraction_summary(
    source_system: str,
    source_entity: str,
    total_rows: int,
    total_batches: int,
    elapsed_time: float,
    level: str = "INFO"
):
    """
    Log extraction summary statistics
    
    Args:
        source_system: Source system name
        source_entity: Entity name
        total_rows: Total rows extracted
        total_batches: Total batches processed
        elapsed_time: Total elapsed time in seconds
        level: Log level
    """
    rows_per_second = total_rows / elapsed_time if elapsed_time > 0 else 0
    
    context = {
        "source_system": source_system,
        "source_entity": source_entity,
        "total_rows": total_rows,
        "total_batches": total_batches,
        "elapsed_time": elapsed_time
    }
    
    message = (
        f"Extraction complete | "
        f"Entity: {source_system}.{source_entity} | "
        f"Rows: {total_rows:,} | "
        f"Batches: {total_batches} | "
        f"Time: {elapsed_time:.2f}s | "
        f"Throughput: {rows_per_second:.0f} rows/s"
    )
    
    with LogContext(**context):
        log_func = getattr(logger, level.lower(), logger.info)
        log_func(message)


def log_ingestion_summary(
    source_system: str,
    source_entity: str,
    zone: str,
    total_rows: int,
    files_written: int,
    total_size_mb: float,
    elapsed_time: float,
    level: str = "INFO"
):
    """
    Log ingestion summary statistics
    
    Args:
        source_system: Source system name
        source_entity: Entity name
        zone: Data zone (bronze, silver, gold)
        total_rows: Total rows ingested
        files_written: Number of files written
        total_size_mb: Total size in MB
        elapsed_time: Total elapsed time in seconds
        level: Log level
    """
    mb_per_second = total_size_mb / elapsed_time if elapsed_time > 0 else 0
    
    context = {
        "source_system": source_system,
        "source_entity": source_entity,
        "zone": zone,
        "total_rows": total_rows,
        "files_written": files_written,
        "total_size_mb": total_size_mb
    }
    
    message = (
        f"Ingestion complete | "
        f"Zone: {zone} | "
        f"Entity: {source_system}.{source_entity} | "
        f"Rows: {total_rows:,} | "
        f"Files: {files_written} | "
        f"Size: {total_size_mb:.2f} MB | "
        f"Time: {elapsed_time:.2f}s | "
        f"Throughput: {mb_per_second:.2f} MB/s"
    )
    
    with LogContext(**context):
        log_func = getattr(logger, level.lower(), logger.info)
        log_func(message)


def log_validation_results(
    entity: str,
    total_rows: int,
    validation_rules: int,
    failures: int,
    warnings: int,
    level: str = "INFO"
):
    """
    Log data validation results
    
    Args:
        entity: Entity name
        total_rows: Total rows validated
        validation_rules: Number of validation rules applied
        failures: Number of validation failures
        warnings: Number of validation warnings
        level: Log level
    """
    status = "FAILED" if failures > 0 else "PASSED"
    
    context = {
        "entity": entity,
        "total_rows": total_rows,
        "validation_rules": validation_rules,
        "failures": failures,
        "warnings": warnings,
        "status": status
    }
    
    message = (
        f"Validation {status} | "
        f"Entity: {entity} | "
        f"Rows: {total_rows:,} | "
        f"Rules: {validation_rules} | "
        f"Failures: {failures} | "
        f"Warnings: {warnings}"
    )
    
    with LogContext(**context):
        if failures > 0:
            logger.error(message)
        else:
            log_func = getattr(logger, level.lower(), logger.info)
            log_func(message)


def log_schema_change(
    entity: str,
    change_type: str,
    details: str,
    level: str = "WARNING"
):
    """
    Log schema evolution changes
    
    Args:
        entity: Entity name
        change_type: Type of change (added_columns, removed_columns, type_changes)
        details: Change details
        level: Log level
    """
    context = {
        "entity": entity,
        "change_type": change_type
    }
    
    message = (
        f"Schema change detected | "
        f"Entity: {entity} | "
        f"Change: {change_type} | "
        f"Details: {details}"
    )
    
    with LogContext(**context):
        log_func = getattr(logger, level.lower(), logger.warning)
        log_func(message)


# Initialize default logger on module import
SyniqLogger.initialize()

# Export convenience function
def get_logger(name: Optional[str] = None) -> "logger":
    """
    Get configured logger instance.
    
    Args:
        name: Optional logger name for identification
    
    Returns:
        Configured loguru logger
    """
    if not SyniqLogger._initialized:
        SyniqLogger.initialize()
    
    if name:
        return logger.bind(logger_name=name)
    return logger

# Export convenience alias
log = logger


if __name__ == "__main__":
    # Demo usage
    print("\n" + "="*60)
    print("Syniq Logger Demo")
    print("="*60)
    
    # Initialize with custom config
    config = LoggerConfig(
        log_dir="logs",
        log_level="DEBUG",
        format_type="detailed"
    )
    SyniqLogger.initialize(config)
    
    # Basic logging
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    
    # Context-aware logging
    with LogContext(source_system="postgres", entity="customers"):
        logger.info("Processing customers data")
        logger.warning("Missing values detected")
    
    # Batch processing logging
    log_batch_processing(
        source_system="postgres_prod",
        source_entity="customers",
        batch_index=5,
        total_batches=10,
        row_count=1000
    )
    
    # Extraction summary
    log_extraction_summary(
        source_system="postgres_prod",
        source_entity="customers",
        total_rows=10000,
        total_batches=10,
        elapsed_time=45.5
    )
    
    # Ingestion summary
    log_ingestion_summary(
        source_system="postgres_prod",
        source_entity="customers",
        zone="bronze",
        total_rows=10000,
        files_written=10,
        total_size_mb=125.5,
        elapsed_time=30.2
    )
    
    # Validation results
    log_validation_results(
        entity="customers",
        total_rows=10000,
        validation_rules=15,
        failures=0,
        warnings=3
    )
    
    # Schema change
    log_schema_change(
        entity="customers",
        change_type="added_columns",
        details="Added columns: customer_segment, loyalty_tier"
    )
    
    # Decorator demo
    @log_execution_time
    def example_function():
        """Example function"""
        import time
        time.sleep(0.5)
        return "Done"
    
    result = example_function()
    
    print("\n" + "="*60)
    print("Check logs/ directory for log files")
    print("="*60)
