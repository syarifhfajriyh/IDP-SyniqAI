"""
spark_s3_batch_consumer.py
===========================
SYINIQ – Part 2 Batch Path: Spark Batch Consumer for AWS S3

What this does
--------------
Reads the batched S3 file records that the Airflow DAG published to Kafka
(topics: ``airflow.s3.<prefix_slug>``) and writes them into the
**Iceberg Bronze layer** as properly typed, partitioned Parquet files.

Mirrors spark_mongodb_batch_consumer.py exactly – same Spark session setup,
same parse/validate/write pattern – only the envelope schema and table name
differ.

Pipeline inside this file
--------------------------
  Kafka (airflow.s3.*)
         │
         ▼  read_from_kafka()
  Raw DataFrame (binary Kafka values)
         │
         ▼  parse_messages()
  Exploded rows  (one row per S3 file record)
         │
         ▼  validate_schema()
  Validated + typed DataFrame
         │
         ▼  write_to_iceberg_bronze()
  Iceberg table: local.bronze.s3_imports
         │
         ▼
  Parquet files in MinIO  s3a://bronze/iceberg/s3_imports/

Iceberg table schema (bronze.s3_imports)
-----------------------------------------
    batch_id        STRING     – Airflow run / batch_id
    bucket          STRING     – Source S3 bucket name
    prefix          STRING     – Source S3 prefix (e.g. "sales/")
    file_key        STRING     – Full S3 object key
    file_type       STRING     – csv / json / parquet / binary
    watermark_from  TIMESTAMP  – Start of extraction window
    watermark_to    TIMESTAMP  – End of extraction window
    extracted_at    TIMESTAMP  – When the record was extracted
    record_id       STRING     – Row index within the file (stringified)
    raw_document    STRING     – Full record as JSON string
    ingested_at     TIMESTAMP  – When Spark wrote the record (now())
    year            INT        – Partition column
    month           INT        – Partition column
    day             INT        – Partition column

Usage
-----
    # Run against all airflow.s3.* topics
    python spark_s3_batch_consumer.py

    # Run against specific prefixes only
    python spark_s3_batch_consumer.py --prefixes sales/ inventory/

    # Dry-run: print schema + row count without writing to Iceberg
    python spark_s3_batch_consumer.py --dry-run

    # Process only messages from last 2 hours
    python spark_s3_batch_consumer.py --hours-back 2
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Load .env
# ---------------------------------------------------------------------------
_THIS_DIR = Path(__file__).resolve().parent
try:
    from dotenv import load_dotenv
    _env_path = _THIS_DIR / ".env"
    if _env_path.exists():
        load_dotenv(_env_path)
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
)
logger = logging.getLogger("spark_s3_batch_consumer")

# ---------------------------------------------------------------------------
# PySpark imports
# ---------------------------------------------------------------------------
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, TimestampType, IntegerType, LongType, ArrayType,
    )
    _SPARK_AVAILABLE = True
except ImportError:
    _SPARK_AVAILABLE = False
    logger.warning("PySpark not installed – Spark functionality unavailable")


# ============================================================================
# Configuration
# ============================================================================

KAFKA_SERVERS: str  = os.getenv("KAFKA_BOOTSTRAP_SERVERS",         "localhost:9092")
TOPIC_PREFIX: str   = os.getenv("KAFKA_TOPIC_AIRFLOW_S3_PREFIX",   "airflow.s3")
PREFIXES: List[str] = [
    p.strip()
    for p in os.getenv("S3_PREFIXES", "metadata/,data/").split(",")
    if p.strip()
]

# MinIO / S3  – where Iceberg stores Parquet files
MINIO_ENDPOINT:    str = os.getenv("MINIO_ENDPOINT",    "localhost:9000")
MINIO_ACCESS_KEY:  str = os.getenv("MINIO_ACCESS_KEY",  "")
MINIO_SECRET_KEY:  str = os.getenv("MINIO_SECRET_KEY",  "")
WAREHOUSE_PATH:    str = os.getenv("ICEBERG_WAREHOUSE",  "s3a://bronze/iceberg")
CHECKPOINT_DIR:    str = os.getenv(
    "SPARK_S3_CHECKPOINT_DIR",
    str(_THIS_DIR / "checkpoints" / "s3_batch"),
)

BRONZE_TABLE: str = os.getenv("ICEBERG_BRONZE_S3_TABLE", "local.bronze.s3_imports")

_SPARK_KAFKA_JAR:   str = os.getenv("SPARK_KAFKA_JAR",       "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
_SPARK_ICEBERG_JAR: str = os.getenv("SPARK_ICEBERG_JAR",     "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2")
_SPARK_HADOOP_JAR:  str = os.getenv("SPARK_HADOOP_AWS_JAR",  "org.apache.hadoop:hadoop-aws:3.3.4")


# ============================================================================
# Schema for the Kafka message envelope (produced by s3_batch_extractor)
# ============================================================================

def get_envelope_schema() -> "StructType":
    """
    The Kafka message envelope produced by S3BatchExtractor:

        {
            "source":         "s3",
            "bucket":         "my-bucket",
            "prefix":         "sales/",
            "file_key":       "sales/2026/03/11/data.csv",
            "file_type":      "csv",
            "batch_id":       "abc-123",
            "batch_number":   1,
            "total_batches":  3,
            "extracted_at":   "2026-03-11T08:00:00+00:00",
            "watermark_from": "2026-03-11T06:00:00+00:00",
            "watermark_to":   "2026-03-11T08:00:00+00:00",
            "record_count":   500,
            "records":        [ {...}, ... ]
        }
    """
    return StructType([
        StructField("source",         StringType(),               True),
        StructField("bucket",         StringType(),               True),
        StructField("prefix",         StringType(),               True),
        StructField("file_key",       StringType(),               True),
        StructField("file_type",      StringType(),               True),
        StructField("batch_id",       StringType(),               True),
        StructField("batch_number",   IntegerType(),              True),
        StructField("total_batches",  IntegerType(),              True),
        StructField("extracted_at",   StringType(),               True),
        StructField("watermark_from", StringType(),               True),
        StructField("watermark_to",   StringType(),               True),
        StructField("record_count",   IntegerType(),              True),
        StructField("records",        ArrayType(StringType(), True), True),
    ])


# ============================================================================
# Spark session factory
# ============================================================================

def create_spark_session() -> "SparkSession":
    logger.info("Creating Spark session …")
    spark = (
        SparkSession.builder
        .appName("SYINIQ-S3-Batch-Consumer")
        .config("spark.jars.packages", f"{_SPARK_KAFKA_JAR},{_SPARK_ICEBERG_JAR},{_SPARK_HADOOP_JAR}")
        # Iceberg catalog
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type",      "hadoop")
        .config("spark.sql.catalog.local.warehouse",  WAREHOUSE_PATH)
        # MinIO / S3A config
        .config("spark.hadoop.fs.s3a.endpoint",              f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key",            MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",            MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",     "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Performance tuning
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.adaptive.enabled",   "true")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark session ready  (version {spark.version})")
    return spark


# ============================================================================
# Core read / transform / write functions
# ============================================================================

def _prefix_to_topic_slug(prefix: str) -> str:
    """sales/  →  sales    data/2026/raw/  →  data_2026_raw"""
    slug = prefix.strip("/").replace("/", "_").replace(" ", "_")
    return re.sub(r"[^a-zA-Z0-9_.-]", "_", slug) or "root"


def read_from_kafka(
    spark: "SparkSession",
    prefixes: List[str],
    hours_back: Optional[int] = None,
) -> "DataFrame":
    """
    Read Kafka topics for the given S3 prefixes in batch mode.
    Returns raw Kafka rows (binary value column).
    """
    topics = ",".join(f"{TOPIC_PREFIX}.{_prefix_to_topic_slug(p)}" for p in prefixes)
    logger.info(f"Reading Kafka topics: {topics}")

    reader = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("subscribe", topics) \
        .option("startingOffsets", "earliest")

    # If hours_back is set, only read messages from last N hours
    if hours_back is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)
        cutoff_ms = int(cutoff.timestamp() * 1000)
        reader = reader.option("startingOffsets", f'{{"*":{{"*":{cutoff_ms}}}}}')

    raw_df = reader.load()
    logger.info(f"Read {raw_df.count()} raw Kafka messages")
    return raw_df


def parse_messages(raw_df: "DataFrame") -> "DataFrame":
    """
    Parse Kafka binary value → JSON envelope → explode records array.
    Returns one row per S3 file record.
    """
    logger.info("Parsing Kafka messages ...")
    
    # Decode binary value to string
    decoded = raw_df.selectExpr("CAST(value AS STRING) AS json_str")
    
    # Parse JSON envelope
    envelope_schema = get_envelope_schema()
    parsed = decoded.select(
        F.from_json(F.col("json_str"), envelope_schema).alias("envelope")
    ).select("envelope.*")
    
    # Convert records array (JSON strings) to actual structs
    # Each element in records is a JSON string, so we need to parse it
    exploded = parsed.select(
        F.col("batch_id"),
        F.col("bucket"),
        F.col("prefix"),
        F.col("file_key"),
        F.col("file_type"),
        F.col("watermark_from"),
        F.col("watermark_to"),
        F.col("extracted_at"),
        F.posexplode(F.col("records")).alias("record_idx", "record_json")
    )
    
    # Parse each record JSON string
    final = exploded.select(
        F.col("batch_id"),
        F.col("bucket"),
        F.col("prefix"),
        F.col("file_key"),
        F.col("file_type"),
        F.col("watermark_from"),
        F.col("watermark_to"),
        F.col("extracted_at"),
        F.col("record_idx").cast("string").alias("record_id"),
        F.col("record_json").alias("raw_document")
    )
    
    logger.info(f"Exploded to {final.count()} individual records")
    return final


def validate_schema(df: "DataFrame") -> "DataFrame":
    """
    Ensure all required columns exist and cast timestamps properly.
    Add partition columns (year, month, day) based on extracted_at.
    """
    logger.info("Validating schema and adding partition columns ...")
    
    validated = df \
        .withColumn("extracted_at_ts", F.to_timestamp(F.col("extracted_at"))) \
        .withColumn("watermark_from_ts", F.to_timestamp(F.col("watermark_from"))) \
        .withColumn("watermark_to_ts", F.to_timestamp(F.col("watermark_to"))) \
        .withColumn("ingested_at", F.current_timestamp()) \
        .withColumn("year", F.year(F.col("extracted_at_ts"))) \
        .withColumn("month", F.month(F.col("extracted_at_ts"))) \
        .withColumn("day", F.dayofmonth(F.col("extracted_at_ts"))) \
        .select(
            "batch_id",
            "bucket",
            "prefix",
            "file_key",
            "file_type",
            F.col("watermark_from_ts").alias("watermark_from"),
            F.col("watermark_to_ts").alias("watermark_to"),
            F.col("extracted_at_ts").alias("extracted_at"),
            "record_id",
            "raw_document",
            "ingested_at",
            "year",
            "month",
            "day"
        )
    
    logger.info("Schema validation complete")
    return validated


def ensure_iceberg_table(spark: "SparkSession") -> None:
    """
    Create the Iceberg bronze table if it doesn't exist.
    """
    logger.info(f"Ensuring Iceberg table exists: {BRONZE_TABLE}")
    
    # Check if table exists
    try:
        spark.sql(f"DESCRIBE TABLE {BRONZE_TABLE}")
        logger.info(f"Table {BRONZE_TABLE} already exists")
        return
    except Exception:
        pass
    
    # Create table with schema and partitioning
    create_ddl = f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
        batch_id        STRING,
        bucket          STRING,
        prefix          STRING,
        file_key        STRING,
        file_type       STRING,
        watermark_from  TIMESTAMP,
        watermark_to    TIMESTAMP,
        extracted_at    TIMESTAMP,
        record_id       STRING,
        raw_document    STRING,
        ingested_at     TIMESTAMP,
        year            INT,
        month           INT,
        day             INT
    )
    USING iceberg
    PARTITIONED BY (year, month, day)
    """
    
    spark.sql(create_ddl)
    logger.info(f"Created Iceberg table: {BRONZE_TABLE}")


def write_to_iceberg_bronze(df: "DataFrame", dry_run: bool = False) -> int:
    """
    Write the validated DataFrame to Iceberg bronze table.
    Returns the number of rows written.
    """
    row_count = df.count()
    logger.info(f"Writing {row_count} rows to Iceberg table: {BRONZE_TABLE}")
    
    if dry_run:
        logger.info("[DRY-RUN] Would write to Iceberg table")
        df.show(10, truncate=False)
        df.printSchema()
        return row_count
    
    df.writeTo(BRONZE_TABLE) \
        .append()
    
    logger.info(f"Successfully wrote {row_count} rows to {BRONZE_TABLE}")
    return row_count


# ============================================================================
# Main pipeline
# ============================================================================

def run_pipeline(
    prefixes: Optional[List[str]] = None,
    hours_back: Optional[int] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Run the full S3 batch consumer pipeline.
    
    1. Create Spark session
    2. Read from Kafka
    3. Parse messages
    4. Validate schema
    5. Write to Iceberg
    
    Returns stats dict.
    """
    if not _SPARK_AVAILABLE:
        raise RuntimeError("PySpark not installed")
    
    if prefixes is None:
        prefixes = PREFIXES
    
    logger.info(f"=== Starting S3 Batch Consumer Pipeline ===")
    logger.info(f"Prefixes: {prefixes}")
    logger.info(f"Hours back: {hours_back}")
    logger.info(f"Dry run: {dry_run}")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Ensure bronze table exists
        if not dry_run:
            ensure_iceberg_table(spark)
        
        # Read from Kafka
        raw_df = read_from_kafka(spark, prefixes, hours_back)
        
        if raw_df.count() == 0:
            logger.info("No messages to process")
            return {
                "status": "success",
                "message": "No messages to process",
                "rows_written": 0
            }
        
        # Parse and validate
        parsed_df = parse_messages(raw_df)
        validated_df = validate_schema(parsed_df)
        
        # Write to Iceberg
        rows_written = write_to_iceberg_bronze(validated_df, dry_run)
        
        logger.info("=== Pipeline Complete ===")
        return {
            "status": "success",
            "rows_written": rows_written,
            "prefixes": prefixes,
            "bronze_table": BRONZE_TABLE
        }
        
    except Exception as exc:
        logger.error(f"Pipeline failed: {exc}", exc_info=True)
        return {
            "status": "error",
            "error": str(exc)
        }
    
    finally:
        spark.stop()


# ============================================================================
# CLI entry point
# ============================================================================

def _main() -> None:
    parser = argparse.ArgumentParser(
        description="SYINIQ S3 Batch Consumer – read from Kafka and write to Iceberg"
    )
    parser.add_argument(
        "--prefixes",
        nargs="+",
        help="S3 prefixes to process (overrides S3_PREFIXES env var)",
    )
    parser.add_argument(
        "--hours-back",
        type=int,
        help="Only process messages from last N hours",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print schema and sample data without writing to Iceberg",
    )
    args = parser.parse_args()
    
    # Override prefixes if provided
    prefixes = args.prefixes if args.prefixes else None
    
    # Run pipeline
    result = run_pipeline(
        prefixes=prefixes,
        hours_back=args.hours_back,
        dry_run=args.dry_run
    )
    
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    _main()
