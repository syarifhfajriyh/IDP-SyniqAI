"""
Metadata Management and Schema Detection
=========================================
Provides schema detection, hashing, and metadata enrichment for data lakehouse.

Features:
- Schema detection from DataFrames with type inference
- Schema hashing for change detection
- Row-level hashing for deduplication
- Bronze metadata enrichment (_ingestion_timestamp, _source_system, etc.)
- Schema evolution tracking and comparison
- Metadata extraction and statistics
"""

import hashlib
import json
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import pandas as pd
import numpy as np

from utils.logger import get_logger

logger = get_logger()


class SchemaInfo:
    """Container for schema information"""
    
    def __init__(
        self,
        columns: Dict[str, str],
        row_count: int = 0,
        nullable_columns: List[str] = None,
        primary_key: Optional[List[str]] = None
    ):
        """
        Initialize schema information
        
        Args:
            columns: Dictionary of column_name -> data_type
            row_count: Number of rows in dataset
            nullable_columns: List of columns that contain nulls
            primary_key: List of columns forming primary key
        """
        self.columns = columns
        self.row_count = row_count
        self.nullable_columns = nullable_columns or []
        self.primary_key = primary_key or []
        self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "columns": self.columns,
            "row_count": self.row_count,
            "nullable_columns": self.nullable_columns,
            "primary_key": self.primary_key,
            "timestamp": self.timestamp.isoformat(),
            "column_count": len(self.columns)
        }
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), indent=2, sort_keys=True)
    
    def __str__(self) -> str:
        return f"SchemaInfo(columns={len(self.columns)}, rows={self.row_count})"


class SchemaComparison:
    """Result of schema comparison"""
    
    def __init__(
        self,
        added_columns: List[str],
        removed_columns: List[str],
        type_changes: Dict[str, Tuple[str, str]],
        is_compatible: bool
    ):
        """
        Initialize schema comparison result
        
        Args:
            added_columns: Columns present in new schema but not old
            removed_columns: Columns present in old schema but not new
            type_changes: Dictionary of column -> (old_type, new_type)
            is_compatible: Whether schemas are compatible (additive changes only)
        """
        self.added_columns = added_columns
        self.removed_columns = removed_columns
        self.type_changes = type_changes
        self.is_compatible = is_compatible
    
    def has_changes(self) -> bool:
        """Check if there are any schema changes"""
        return bool(self.added_columns or self.removed_columns or self.type_changes)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "added_columns": self.added_columns,
            "removed_columns": self.removed_columns,
            "type_changes": self.type_changes,
            "is_compatible": self.is_compatible,
            "has_changes": self.has_changes()
        }
    
    def __str__(self) -> str:
        if not self.has_changes():
            return "No schema changes"
        
        changes = []
        if self.added_columns:
            changes.append(f"Added: {', '.join(self.added_columns)}")
        if self.removed_columns:
            changes.append(f"Removed: {', '.join(self.removed_columns)}")
        if self.type_changes:
            type_strs = [f"{col}: {old}->{new}" for col, (old, new) in self.type_changes.items()]
            changes.append(f"Type changes: {', '.join(type_strs)}")
        
        return " | ".join(changes)


def detect_schema(df: pd.DataFrame, sample_size: Optional[int] = None) -> SchemaInfo:
    """
    Detect schema from DataFrame with accurate type inference
    
    Args:
        df: Input DataFrame
        sample_size: Number of rows to sample for analysis (None = all rows)
    
    Returns:
        SchemaInfo object containing schema details
    """
    if sample_size and len(df) > sample_size:
        df_sample = df.sample(n=sample_size, random_state=42)
    else:
        df_sample = df
    
    # Build column type mapping
    columns = {}
    for col in df.columns:
        dtype = df[col].dtype
        
        # Map pandas dtypes to standard types
        if pd.api.types.is_integer_dtype(dtype):
            columns[col] = "integer"
        elif pd.api.types.is_float_dtype(dtype):
            columns[col] = "float"
        elif pd.api.types.is_bool_dtype(dtype):
            columns[col] = "boolean"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            columns[col] = "timestamp"
        elif pd.api.types.is_string_dtype(dtype) or dtype == object:
            # Check if it's actually JSON/dict
            if df_sample[col].notna().any():
                first_val = df_sample[col].dropna().iloc[0]
                if isinstance(first_val, (dict, list)):
                    columns[col] = "json"
                elif isinstance(first_val, bytes):
                    columns[col] = "binary"
                else:
                    columns[col] = "string"
            else:
                columns[col] = "string"
        else:
            columns[col] = str(dtype)
    
    # Detect nullable columns
    nullable_columns = [col for col in df.columns if df[col].isna().any()]
    
    schema_info = SchemaInfo(
        columns=columns,
        row_count=len(df),
        nullable_columns=nullable_columns
    )
    
    logger.debug(f"Detected schema: {len(columns)} columns, {len(df)} rows")
    return schema_info


def compute_schema_hash(schema_info: SchemaInfo) -> str:
    """
    Compute deterministic hash of schema structure
    
    Args:
        schema_info: Schema information
    
    Returns:
        SHA256 hash of schema (hex string)
    """
    # Create deterministic representation (sorted by column name)
    schema_dict = {
        "columns": dict(sorted(schema_info.columns.items()))
    }
    
    schema_json = json.dumps(schema_dict, sort_keys=True)
    schema_hash = hashlib.sha256(schema_json.encode()).hexdigest()
    
    return schema_hash


def compute_row_hash(row: pd.Series, columns: Optional[List[str]] = None) -> str:
    """
    Compute deterministic hash of a data row
    
    Args:
        row: DataFrame row (Series)
        columns: Specific columns to include in hash (None = all columns)
    
    Returns:
        SHA256 hash of row (hex string)
    """
    if columns:
        row = row[columns]
    
    # Convert to JSON (handles various types)
    row_dict = row.to_dict()
    
    # Handle special types
    for key, value in row_dict.items():
        if pd.isna(value):
            row_dict[key] = None
        elif isinstance(value, (pd.Timestamp, datetime)):
            row_dict[key] = value.isoformat()
        elif isinstance(value, bytes):
            row_dict[key] = value.hex()
        elif isinstance(value, (np.integer, np.floating)):
            row_dict[key] = value.item()
    
    row_json = json.dumps(row_dict, sort_keys=True)
    row_hash = hashlib.sha256(row_json.encode()).hexdigest()
    
    return row_hash


def add_bronze_metadata(
    df: pd.DataFrame,
    source_system: str,
    source_entity: str,
    ingestion_id: Optional[str] = None,
    schema_version: Optional[str] = None,
    add_row_hash: bool = False,
    row_hash_columns: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Add bronze layer metadata columns to DataFrame
    
    Args:
        df: Input DataFrame
        source_system: Source system name (e.g., "postgres_prod")
        source_entity: Entity/table name (e.g., "customers")
        ingestion_id: Unique ingestion run identifier
        schema_version: Schema version hash
        add_row_hash: Whether to add _row_hash column for deduplication
        row_hash_columns: Columns to include in row hash (None = all business columns)
    
    Returns:
        DataFrame with added metadata columns
    """
    df = df.copy()
    
    # Add standard bronze metadata
    df["_ingestion_timestamp"] = datetime.now()
    df["_source_system"] = source_system
    df["_source_entity"] = source_entity
    
    if ingestion_id:
        df["_ingestion_id"] = ingestion_id
    
    if schema_version:
        df["_schema_version"] = schema_version
    
    # Add row hash if requested
    if add_row_hash:
        logger.debug(f"Computing row hashes for {len(df)} rows...")
        df["_row_hash"] = df.apply(
            lambda row: compute_row_hash(row, columns=row_hash_columns),
            axis=1
        )
    
    logger.debug(f"Added bronze metadata columns: {['_ingestion_timestamp', '_source_system', '_source_entity']}")
    
    return df


def compare_schemas(old_schema: SchemaInfo, new_schema: SchemaInfo) -> SchemaComparison:
    """
    Compare two schemas and detect changes
    
    Args:
        old_schema: Previous schema
        new_schema: New schema
    
    Returns:
        SchemaComparison object with differences
    """
    old_cols = set(old_schema.columns.keys())
    new_cols = set(new_schema.columns.keys())
    
    # Detect added/removed columns
    added_columns = sorted(new_cols - old_cols)
    removed_columns = sorted(old_cols - new_cols)
    
    # Detect type changes
    common_cols = old_cols & new_cols
    type_changes = {}
    for col in common_cols:
        old_type = old_schema.columns[col]
        new_type = new_schema.columns[col]
        if old_type != new_type:
            type_changes[col] = (old_type, new_type)
    
    # Determine compatibility (only additive changes are compatible)
    is_compatible = (
        not removed_columns and  # No columns removed
        not type_changes        # No type changes
    )
    
    comparison = SchemaComparison(
        added_columns=added_columns,
        removed_columns=removed_columns,
        type_changes=type_changes,
        is_compatible=is_compatible
    )
    
    if comparison.has_changes():
        logger.info(f"Schema changes detected: {comparison}")
    
    return comparison


def extract_metadata_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Extract comprehensive metadata statistics from DataFrame
    
    Args:
        df: Input DataFrame
    
    Returns:
        Dictionary with metadata statistics
    """
    stats = {
        "row_count": len(df),
        "column_count": len(df.columns),
        "memory_usage_mb": df.memory_usage(deep=True).sum() / (1024 * 1024),
        "null_counts": df.isnull().sum().to_dict(),
        "null_percentages": (df.isnull().sum() / len(df) * 100).to_dict(),
        "dtypes": df.dtypes.astype(str).to_dict()
    }
    
    # Add numeric column statistics
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        numeric_stats = df[numeric_cols].describe().to_dict()
        stats["numeric_summary"] = numeric_stats
    
    # Add string column statistics
    string_cols = df.select_dtypes(include=['object', 'string']).columns
    if len(string_cols) > 0:
        string_stats = {}
        for col in string_cols:
            string_stats[col] = {
                "unique_count": df[col].nunique(),
                "max_length": df[col].astype(str).str.len().max() if df[col].notna().any() else 0
            }
        stats["string_summary"] = string_stats
    
    return stats


def generate_metadata_json(
    source_system: str,
    source_entity: str,
    schema_info: SchemaInfo,
    ingestion_stats: Dict[str, Any],
    file_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Generate comprehensive metadata JSON for a dataset
    
    Args:
        source_system: Source system name
        source_entity: Entity name
        schema_info: Schema information
        ingestion_stats: Ingestion statistics
        file_path: Optional path to data file
    
    Returns:
        Metadata dictionary (ready for JSON serialization)
    """
    metadata = {
        "version": "1.0",
        "generated_at": datetime.now().isoformat(),
        "source": {
            "system": source_system,
            "entity": source_entity
        },
        "schema": schema_info.to_dict(),
        "schema_hash": compute_schema_hash(schema_info),
        "ingestion": ingestion_stats
    }
    
    if file_path:
        metadata["file_path"] = file_path
    
    return metadata


def partition_key_from_timestamp(timestamp_col: str = "_ingestion_timestamp") -> List[str]:
    """
    Generate partition key columns from timestamp
    
    Args:
        timestamp_col: Name of timestamp column
    
    Returns:
        List of partition column names
    """
    return [
        f"{timestamp_col}_year",
        f"{timestamp_col}_month",
        f"{timestamp_col}_day"
    ]


def add_partition_columns(
    df: pd.DataFrame,
    timestamp_col: str = "_ingestion_timestamp"
) -> pd.DataFrame:
    """
    Add partitioning columns derived from timestamp
    
    Args:
        df: Input DataFrame
        timestamp_col: Timestamp column to derive partitions from
    
    Returns:
        DataFrame with partition columns added
    """
    df = df.copy()
    
    if timestamp_col not in df.columns:
        raise ValueError(f"Timestamp column '{timestamp_col}' not found in DataFrame")
    
    # Ensure timestamp column is datetime
    df[timestamp_col] = pd.to_datetime(df[timestamp_col])
    
    # Add partition columns
    df[f"{timestamp_col}_year"] = df[timestamp_col].dt.year
    df[f"{timestamp_col}_month"] = df[timestamp_col].dt.month
    df[f"{timestamp_col}_day"] = df[timestamp_col].dt.day
    
    logger.debug(f"Added partition columns: year, month, day")
    
    return df


def deduplicate_by_hash(
    df: pd.DataFrame,
    hash_column: str = "_row_hash",
    keep: str = "last"
) -> Tuple[pd.DataFrame, int]:
    """
    Deduplicate DataFrame based on row hash
    
    Args:
        df: Input DataFrame with hash column
        hash_column: Name of hash column
        keep: Which duplicate to keep ('first', 'last', False)
    
    Returns:
        Tuple of (deduplicated DataFrame, number of duplicates removed)
    """
    if hash_column not in df.columns:
        raise ValueError(f"Hash column '{hash_column}' not found in DataFrame")
    
    original_count = len(df)
    df_deduped = df.drop_duplicates(subset=[hash_column], keep=keep)
    duplicates_removed = original_count - len(df_deduped)
    
    if duplicates_removed > 0:
        logger.info(f"Removed {duplicates_removed} duplicate rows based on {hash_column}")
    
    return df_deduped, duplicates_removed


if __name__ == "__main__":
    # Demo usage
    print("\n" + "="*60)
    print("Metadata Utilities Demo")
    print("="*60)
    
    # Create sample DataFrame
    data = {
        "customer_id": [1, 2, 3, 4, 5],
        "customer_name": ["Alice", "Bob", "Charlie", None, "Eve"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com", None, "eve@example.com"],
        "age": [25, 30, 35, 40, 28],
        "premium": [1200.50, 1500.75, 2000.00, 1800.25, 1350.00],
        "is_active": [True, True, False, True, True],
        "signup_date": pd.date_range("2024-01-01", periods=5)
    }
    df = pd.DataFrame(data)
    
    print("\n1. Original DataFrame:")
    print(df)
    
    # Schema detection
    print("\n2. Schema Detection:")
    schema = detect_schema(df)
    print(f"   Columns: {schema.columns}")
    print(f"   Nullable: {schema.nullable_columns}")
    print(f"   Schema Hash: {compute_schema_hash(schema)[:16]}...")
    
    # Add bronze metadata
    print("\n3. Add Bronze Metadata:")
    df_bronze = add_bronze_metadata(
        df,
        source_system="postgres_prod",
        source_entity="customers",
        ingestion_id="ing_20260218_001",
        schema_version=compute_schema_hash(schema),
        add_row_hash=True
    )
    print(f"   Added columns: {[col for col in df_bronze.columns if col.startswith('_')]}")
    print(f"   Sample row hash: {df_bronze['_row_hash'].iloc[0][:16]}...")
    
    # Add partition columns
    print("\n4. Add Partition Columns:")
    df_partitioned = add_partition_columns(df_bronze, "_ingestion_timestamp")
    partition_cols = [col for col in df_partitioned.columns if "year" in col or "month" in col or "day" in col]
    print(f"   Partition columns: {partition_cols}")
    
    # Extract metadata stats
    print("\n5. Metadata Statistics:")
    stats = extract_metadata_stats(df)
    print(f"   Row count: {stats['row_count']}")
    print(f"   Column count: {stats['column_count']}")
    print(f"   Memory usage: {stats['memory_usage_mb']:.2f} MB")
    print(f"   Null counts: {stats['null_counts']}")
    
    # Schema comparison
    print("\n6. Schema Evolution:")
    # Simulate schema change
    df_new = df.copy()
    df_new["loyalty_tier"] = ["Gold", "Silver", "Bronze", "Gold", "Silver"]
    df_new = df_new.drop(columns=["is_active"])
    
    old_schema = detect_schema(df)
    new_schema = detect_schema(df_new)
    comparison = compare_schemas(old_schema, new_schema)
    
    print(f"   Added columns: {comparison.added_columns}")
    print(f"   Removed columns: {comparison.removed_columns}")
    print(f"   Compatible: {comparison.is_compatible}")
    print(f"   Summary: {comparison}")
    
    # Generate metadata JSON
    print("\n7. Generate Metadata JSON:")
    metadata_json = generate_metadata_json(
        source_system="postgres_prod",
        source_entity="customers",
        schema_info=schema,
        ingestion_stats={
            "batch_count": 1,
            "elapsed_time": 5.2,
            "rows_per_second": len(df) / 5.2
        },
        file_path="bronze/postgres_prod/customers/2026-02-18/data_001.parquet"
    )
    print(f"   Metadata keys: {list(metadata_json.keys())}")
    print(f"   Schema hash: {metadata_json['schema_hash'][:16]}...")
    
    # Deduplication
    print("\n8. Deduplication:")
    # Add duplicate row
    df_with_dups = pd.concat([df_bronze, df_bronze.iloc[[0]]], ignore_index=True)
    print(f"   Before dedup: {len(df_with_dups)} rows")
    df_deduped, dup_count = deduplicate_by_hash(df_with_dups)
    print(f"   After dedup: {len(df_deduped)} rows")
    print(f"   Duplicates removed: {dup_count}")
    
    print("\n" + "="*60)
    print("Demo complete!")
    print("="*60)
