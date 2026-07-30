"""
Spark Streaming CDC Consumer
=============================
Consumes CDC events from Kafka and writes to Iceberg Bronze tables.

This consumer:
1. Reads CDC events from Debezium Kafka topics
2. Processes change events (INSERT, UPDATE, DELETE)
3. Writes to Iceberg Bronze tables with ACID guarantees
4. Handles schema evolution automatically
5. Provides exactly-once semantics with Kafka offsets

Architecture:
  Kafka Topics → Spark Structured Streaming → Iceberg Bronze Tables

Topics consumed:
  - cdc.syniq_postgres.public.* (PostgreSQL tables)
  - cdc.syniq_mariadb.mysql.* (MariaDB tables)
  - cdc.mongodb.* (MongoDB collections)
  - cdc.s3.* (S3 file events)

Usage:
    # Run all CDC consumers
    python spark_cdc_consumer.py

    # Run specific source
    python spark_cdc_consumer.py --source postgres
    python spark_cdc_consumer.py --source mariadb

    # Custom config
    python spark_cdc_consumer.py --kafka-servers localhost:9092
"""

# CRITICAL: Set HADOOP_HOME before any Spark imports (for Windows)
import os
import sys
from pathlib import Path

if os.name == 'nt':
    if not os.getenv('HADOOP_HOME'):
        # Create hadoop_home if it doesn't exist
        hadoop_home = Path.home() / ".hadoop_home"
        hadoop_bin = hadoop_home / "bin"
        hadoop_home.mkdir(exist_ok=True)
        hadoop_bin.mkdir(exist_ok=True)
        
        os.environ['HADOOP_HOME'] = str(hadoop_home)
        print(f"[CDC] Set HADOOP_HOME to: {hadoop_home}")
        
        # Check if winutils.exe exists
        winutils = hadoop_bin / "winutils.exe"
        if not winutils.exists():
            print(f"[CDC] WARNING: {winutils} not found!")
            print("[CDC] Run: python setup_hadoop_windows.py to download it")
    else:
        print(f"[CDC] HADOOP_HOME already set: {os.getenv('HADOOP_HOME')}")

import argparse
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp,
    lit, expr, when, coalesce, struct, to_json
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, TimestampType, BooleanType, MapType
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SparkCDCConsumer:
    """
    Spark Structured Streaming consumer for CDC events.
    """

    # Per-table primary key registry: "source_db.source_table" -> [pk columns].
    # Tables not listed fall back to DEFAULT_PK_COLUMNS. Composite keys are
    # supported by listing more than one column.
    PK_REGISTRY: Dict[str, List[str]] = {
        # "syniq_postgres.order_items": ["order_id", "product_id"],
    }
    DEFAULT_PK_COLUMNS = ["id"]

    def __init__(
        self,
        kafka_bootstrap_servers: str = "localhost:9092",
        checkpoint_location: str = "./checkpoints",
        warehouse_path: str = "s3a://bronze/iceberg",
        minio_endpoint: str = "http://localhost:9000",
        minio_access_key: str = "minioadmin",
        minio_secret_key: str = "minioadmin"
    ):
        """
        Initialize Spark CDC Consumer.
        
        Args:
            kafka_bootstrap_servers: Kafka broker addresses
            checkpoint_location: Directory for Spark checkpoints
            warehouse_path: Iceberg warehouse location
            minio_endpoint: MinIO endpoint URL
            minio_access_key: MinIO access key
            minio_secret_key: MinIO secret key
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.checkpoint_location = checkpoint_location
        self.warehouse_path = warehouse_path
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        
        self.spark = None
        self.active_streams = []
        
        logger.info("SparkCDCConsumer initialized")
    
    
    def create_spark_session(self) -> SparkSession:
        """
        Create Spark session with Iceberg and Kafka support.
        """
        logger.info("Creating Spark session with Iceberg + Kafka support...")
        
        # Download Kafka and Iceberg JARs if needed
        kafka_jar = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        iceberg_runtime = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
        aws_bundle = "org.apache.hadoop:hadoop-aws:3.3.4"
        
        spark = (SparkSession.builder
            .appName("SYINIQ-CDC-Consumer")
            .config("spark.jars.packages", f"{kafka_jar},{iceberg_runtime},{aws_bundle}")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", self.warehouse_path)
            
            # MinIO / S3 configuration
            .config("spark.hadoop.fs.s3a.endpoint", self.minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", self.minio_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.minio_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            
            # Windows Hadoop compatibility - disable native libraries
            .config("spark.hadoop.io.native.lib.available", "false")
            .config("spark.sql.streaming.fileSource.cleaner.numThreads", "1")
            
            # Streaming configuration
            .config("spark.sql.streaming.checkpointLocation", self.checkpoint_location)
            .config("spark.sql.streaming.schemaInference", "true")
            .config("spark.sql.adaptive.enabled", "true")
            
            # Performance tuning
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.streaming.kafka.maxRatePerPartition", "1000")
            
            .master("local[*]")
            .getOrCreate()
        )
        
        logger.info(f"Spark session created: {spark.version}")
        self.spark = spark
        return spark
    
    
    def get_cdc_schema(self) -> StructType:
        """
        Define schema for CDC messages from Debezium.
        
        After ExtractNewRecordState transformation, messages look like:
        {
            "id": 123,
            "name": "John",
            "email": "john@example.com",
            "__op": "c",  # c=create, u=update, d=delete, r=read
            "__source_ts_ms": 1234567890,
            "__source_db": "postgres",
            "__source_table": "users"
        }
        """
        return StructType([
            # CDC metadata (added by ExtractNewRecordState transformation)
            StructField("__op", StringType(), True),
            StructField("__source_ts_ms", LongType(), True),
            StructField("__source_db", StringType(), True),
            StructField("__source_table", StringType(), True),
            
            # Actual data fields are dynamic, so we'll handle them separately
        ])
    
    
    def consume_postgres_cdc(self, table_pattern: str = "public.*") -> None:
        """
        Consume PostgreSQL CDC events and write to Iceberg.
        
        Args:
            table_pattern: Pattern for table names to consume (e.g., "public.*")
        """
        logger.info(f"Starting PostgreSQL CDC consumer for tables: {table_pattern}")
        
        # Topic pattern for PostgreSQL
        topic_pattern = f"cdc\\.syniq_postgres\\.{table_pattern}"
        
        # Read from Kafka
        df = (self.spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
            .option("subscribePattern", topic_pattern)
            .option("startingOffsets", "earliest")  # Start from beginning on first run
            .option("maxOffsetsPerTrigger", "10000")  # Rate limiting
            .option("failOnDataLoss", "false")
            .load()
        )
        
        # Parse JSON value
        # Note: Schema will be inferred dynamically since each table has different columns
        df_parsed = (df
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp")
            .selectExpr(
                "key",
                "get_json_object(value, '$.__op') as op",
                "CAST(get_json_object(value, '$.__source_ts_ms') AS LONG) as source_ts_ms",
                "get_json_object(value, '$.__source_db') as source_db",
                "get_json_object(value, '$.__source_table') as source_table",
                "value as full_record",
                "topic",
                "partition",
                "offset",
                "timestamp as kafka_timestamp"
            )
            .withColumn("ingestion_timestamp", current_timestamp())
        )
        
        # Write to Iceberg (using foreachBatch for dynamic table routing)
        iceberg_query = (df_parsed
            .writeStream
            .foreachBatch(self._write_to_iceberg_bronze)
            .option("checkpointLocation", f"{self.checkpoint_location}/postgres_cdc")
            .start()
        )

        self.active_streams.append(iceberg_query)
        logger.info("PostgreSQL CDC consumer started")
    
    
    def consume_mariadb_cdc(self, table_pattern: str = "*") -> None:
        """
        Consume MariaDB CDC events and write to Iceberg.
        
        Args:
            table_pattern: Pattern for table names to consume
        """
        logger.info(f"Starting MariaDB CDC consumer for tables: {table_pattern}")
        
        # Topic pattern for MariaDB
        topic_pattern = f"cdc\\.syniq_mariadb\\.mysql\\.{table_pattern}"
        
        # Read from Kafka
        df = (self.spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
            .option("subscribePattern", topic_pattern)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", "10000")
            .option("failOnDataLoss", "false")
            .load()
        )
        
        # Parse JSON value
        df_parsed = (df
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp")
            .selectExpr(
                "key",
                "get_json_object(value, '$.__op') as op",
                "CAST(get_json_object(value, '$.__source_ts_ms') AS LONG) as source_ts_ms",
                "get_json_object(value, '$.__source_db') as source_db",
                "get_json_object(value, '$.__source_table') as source_table",
                "value as full_record",
                "topic",
                "partition",
                "offset",
                "timestamp as kafka_timestamp"
            )
            .withColumn("ingestion_timestamp", current_timestamp())
        )
        
        # Write to Iceberg
        iceberg_query = (df_parsed
            .writeStream
            .foreachBatch(self._write_to_iceberg_bronze)
            .option("checkpointLocation", f"{self.checkpoint_location}/mariadb_cdc")
            .start()
        )

        self.active_streams.append(iceberg_query)
        logger.info("MariaDB CDC consumer started")
    
    
    def _write_to_iceberg_bronze(self, batch_df: DataFrame, batch_id: int):
        """
        Write a micro-batch to Iceberg Bronze tables.
        
        This function is called by foreachBatch and handles:
        1. Parsing the full JSON record
        2. Routing to appropriate Iceberg table
        3. Handling INSERT/UPDATE/DELETE operations
        4. Creating tables if they don't exist
        
        Args:
            batch_df: Micro-batch DataFrame
            batch_id: Batch identifier
        """
        if batch_df.isEmpty():
            return
        
        logger.info(f"Processing batch {batch_id} with {batch_df.count()} records")
        
        # Group by source and table for efficient processing
        tables = batch_df.select("source_db", "source_table").distinct().collect()
        
        for row in tables:
            source_db = row.source_db
            source_table = row.source_table
            
            # Filter records for this table
            table_df = batch_df.filter(
                (col("source_db") == source_db) & (col("source_table") == source_table)
            )
            
            # Parse full record JSON into separate columns
            # We need to infer schema from the first record
            sample_record = table_df.select("full_record").first()
            if sample_record:
                import json
                try:
                    record_dict = json.loads(sample_record.full_record)
                except (json.JSONDecodeError, TypeError) as e:
                    logger.error(
                        f"Malformed CDC record JSON for {source_db}.{source_table}, "
                        f"routing batch to DLQ instead of crashing: {e}"
                    )
                    self._write_to_error_table(table_df, f"JSON parse error: {e}")
                    continue

                # Remove metadata fields to get actual data columns
                data_columns = {k: v for k, v in record_dict.items() if not k.startswith("__")}
                
                # Extract all fields from JSON
                parsed_df = table_df
                for col_name in data_columns.keys():
                    parsed_df = parsed_df.withColumn(
                        col_name,
                        expr(f"get_json_object(full_record, '$.{col_name}')")
                    )
                
                # Add metadata columns
                parsed_df = (parsed_df
                    .withColumn("_cdc_operation", col("op"))
                    .withColumn("_cdc_timestamp", (col("source_ts_ms") / 1000).cast("timestamp"))
                    .withColumn("_ingestion_timestamp", col("ingestion_timestamp"))
                    .withColumn("_kafka_offset", col("offset"))
                    .drop("full_record", "key", "topic", "partition", "offset", "kafka_timestamp", 
                          "op", "source_ts_ms", "source_db", "source_table", "ingestion_timestamp")
                )
                
                # Define Iceberg table name
                iceberg_table = f"local.bronze.{source_db}_{source_table}"
                
                try:
                    # Check if table exists
                    if self._iceberg_table_exists(iceberg_table):
                        # Merge into existing table
                        self._merge_into_iceberg(iceberg_table, parsed_df, source_db, source_table)
                    else:
                        # Create new table
                        logger.info(f"Creating new Iceberg table: {iceberg_table}")
                        parsed_df.writeTo(iceberg_table).using("iceberg").createOrReplace()
                    
                    logger.info(f"Successfully wrote {parsed_df.count()} records to {iceberg_table}")
                    
                except Exception as e:
                    logger.error(f"Failed to write to {iceberg_table}: {e}")
                    # Write to DLQ or error table
                    self._write_to_error_table(parsed_df, str(e))
    
    
    def _iceberg_table_exists(self, table_name: str) -> bool:
        """Check if Iceberg table exists."""
        try:
            self.spark.table(table_name)
            return True
        except:
            return False
    
    
    def _get_pk_columns(self, source_db: str, source_table: str) -> List[str]:
        """Look up the configured primary key column(s) for a source table."""
        return self.PK_REGISTRY.get(f"{source_db}.{source_table}", self.DEFAULT_PK_COLUMNS)

    def _merge_into_iceberg(self, table_name: str, df: DataFrame, source_db: str, source_table: str):
        """
        Merge CDC records into existing Iceberg table.

        Handles:
        - INSERT: _cdc_operation = 'c' or 'r'
        - UPDATE: _cdc_operation = 'u' (only if the incoming record is newer
          than what's already in the table, guarding against out-of-order
          Kafka delivery overwriting newer state with stale data)
        - DELETE: _cdc_operation = 'd'

        On failure this raises rather than silently falling back to a blind
        append — the caller routes failed batches to the DLQ instead
        (append-on-failure would duplicate/corrupt data).
        """
        # Register temp view for merge
        df.createOrReplaceTempView("cdc_updates")

        pk_columns = self._get_pk_columns(source_db, source_table)
        on_clause = " AND ".join(f"target.{c} = source.{c}" for c in pk_columns)

        # Perform merge operation
        merge_query = f"""
        MERGE INTO {table_name} target
        USING cdc_updates source
        ON {on_clause}
        WHEN MATCHED AND source._cdc_operation = 'd' THEN DELETE
        WHEN MATCHED AND source._cdc_operation IN ('u', 'r') AND source._cdc_timestamp > target._cdc_timestamp THEN UPDATE SET *
        WHEN NOT MATCHED AND source._cdc_operation IN ('c', 'r') THEN INSERT *
        """

        self.spark.sql(merge_query)
        logger.info(f"Merged CDC updates into {table_name}")
    
    
    def _write_to_error_table(self, df: DataFrame, error_msg: str):
        """Write failed records to error/DLQ table."""
        error_table = "local.bronze._cdc_errors"
        
        error_df = df.withColumn("_error_message", lit(error_msg))
        
        try:
            error_df.writeTo(error_table).using("iceberg").append()
            logger.info(f"Wrote {df.count()} error records to {error_table}")
        except:
            logger.error("Failed to write to error table")
    
    
    def start(self, source: str = "all"):
        """
        Start CDC consumers for specified source.
        
        Args:
            source: Source to consume from ('postgres', 'mariadb', 'all')
        """
        if not self.spark:
            self.create_spark_session()
        
        logger.info("=" * 60)
        logger.info("Starting Spark CDC Consumers")
        logger.info("=" * 60)
        
        if source in ["postgres", "all"]:
            self.consume_postgres_cdc()
        
        if source in ["mariadb", "all"]:
            self.consume_mariadb_cdc()
        
        logger.info(f"Started {len(self.active_streams)} streaming queries")
        logger.info("Waiting for termination... (Ctrl+C to stop)")
        
        # Wait for all streams to finish
        try:
            for query in self.active_streams:
                query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
            self.stop()
    
    
    def stop(self):
        """Stop all active streams and Spark session."""
        logger.info("Stopping all streaming queries...")
        
        for query in self.active_streams:
            try:
                query.stop()
            except:
                pass
        
        if self.spark:
            self.spark.stop()
        
        logger.info("All streams stopped")


def main():
    # HADOOP_HOME already set at top of file
    
    # Write PID file for backend monitoring
    pid_file = Path(__file__).parent / "cdc_consumer.pid"
    try:
        pid_file.write_text(str(os.getpid()))
        logger.info(f"PID {os.getpid()} written to {pid_file}")
    except Exception as e:
        logger.warning(f"Failed to write PID file: {e}")
    
    # Ensure PID file is cleaned up on exit
    import atexit
    def cleanup_pid():
        try:
            if pid_file.exists():
                pid_file.unlink()
                logger.info("PID file removed")
        except Exception as e:
            logger.warning(f"Failed to remove PID file: {e}")
    
    atexit.register(cleanup_pid)
    
    # Get defaults from environment variables (for Docker compatibility)
    kafka_servers_default = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    minio_endpoint_default = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
    minio_access_key_default = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    minio_secret_key_default = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
    
    parser = argparse.ArgumentParser(description="Spark CDC Consumer for Debezium")
    parser.add_argument(
        '--source',
        choices=['postgres', 'mariadb', 'all'],
        default='all',
        help='CDC source to consume (default: all)'
    )
    parser.add_argument(
        '--kafka-servers',
        default=kafka_servers_default,
        help=f'Kafka bootstrap servers (default: {kafka_servers_default})'
    )
    parser.add_argument(
        '--checkpoint',
        default='./checkpoints',
        help='Checkpoint directory (default: ./checkpoints)'
    )
    parser.add_argument(
        '--warehouse',
        default='s3a://bronze/iceberg',
        help='Iceberg warehouse path (default: s3a://bronze/iceberg)'
    )
    
    args = parser.parse_args()
    
    # Create consumer with environment-aware config
    consumer = SparkCDCConsumer(
        kafka_bootstrap_servers=args.kafka_servers,
        checkpoint_location=args.checkpoint,
        warehouse_path=args.warehouse,
        minio_endpoint=minio_endpoint_default,
        minio_access_key=minio_access_key_default,
        minio_secret_key=minio_secret_key_default
    )
    
    # Start consuming
    try:
        consumer.start(source=args.source)
    except Exception as e:
        logger.error(f"Consumer failed: {e}")
        raise
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
