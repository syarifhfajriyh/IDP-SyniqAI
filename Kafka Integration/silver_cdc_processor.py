"""
Silver CDC Processor - Real-time Streaming Transformation
=========================================================

Extends CDC pipeline to process Bronze → Silver in real-time.

Architecture:
  Bronze CDC Tables → Spark Streaming → Silver Transformations → Silver Tables
  
Features:
- Real-time quality validation on CDC streams
- Incremental Silver transformations (15-30s latency)
- CDC operation handling (INSERT/UPDATE/DELETE)
- Automatic quality scoring
- Quarantine management for failed records
- Metrics tracking and monitoring

Usage:
    # Start all CDC Silver processors
    python silver_cdc_processor.py
    
    # Process specific source
    python silver_cdc_processor.py --source postgres --table users
    
    # Custom configuration
    python silver_cdc_processor.py --checkpoint-interval 10 --max-offsets-per-trigger 10000
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, when, lit, current_timestamp,
    coalesce, regexp_replace, length, isnan, isnull,
    count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    md5, concat_ws, year, month, dayofmonth, hour,
    row_number, rank, dense_rank, lag, lead, expr
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType, BooleanType, LongType
)

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "data lakehouse" / "syniq_project"))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SilverCDCProcessor:
    """
    Real-time Silver transformation processor for CDC streams.
    
    Monitors Bronze CDC tables and applies Silver transformations in streaming mode.
    """
    
    def __init__(
        self,
        minio_endpoint: str = "http://localhost:9000",
        minio_access_key: str = "admin",
        minio_secret_key: str = "password123",
        checkpoint_dir: str = "/tmp/silver_cdc_checkpoints",
        checkpoint_interval: int = 30,  # seconds
        max_offsets_per_trigger: int = 10000
    ):
        """
        Initialize Silver CDC Processor.
        
        Args:
            minio_endpoint: MinIO endpoint URL
            minio_access_key: MinIO access key
            minio_secret_key: MinIO secret key
            checkpoint_dir: Directory for Spark checkpoints
            checkpoint_interval: Checkpoint interval in seconds
            max_offsets_per_trigger: Max records per micro-batch
        """
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.checkpoint_dir = checkpoint_dir
        self.checkpoint_interval = checkpoint_interval
        self.max_offsets_per_trigger = max_offsets_per_trigger
        
        # Iceberg configuration
        self.catalog_name = "syniq_iceberg"
        self.bronze_namespace = "bronze"
        self.silver_namespace = "silver"
        
        # Quality thresholds
        self.quality_thresholds = {
            'completeness_min': 0.95,  # 95% non-null
            'validity_min': 0.90,       # 90% valid format
            'uniqueness_min': 0.99      # 99% unique (for ID columns)
        }
        
        # Initialize Spark
        self.spark = self._create_spark_session()
        
        logger.info("✅ SilverCDCProcessor initialized")
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Iceberg and streaming configuration"""
        try:
            warehouse_path = f"{self.minio_endpoint}/syniqai-warehouse"
            
            spark = SparkSession.builder \
                .appName("SilverCDCProcessor") \
                .config("spark.jars.packages", 
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,"
                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                    "software.amazon.awssdk:bundle:2.20.18,"
                    "software.amazon.awssdk:url-connection-client:2.20.18"
                ) \
                .config("spark.sql.extensions", 
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config("spark.sql.catalog.syniq_iceberg", 
                    "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.syniq_iceberg.type", "hadoop") \
                .config("spark.sql.catalog.syniq_iceberg.warehouse", warehouse_path) \
                .config("spark.hadoop.fs.s3a.endpoint", self.minio_endpoint) \
                .config("spark.hadoop.fs.s3a.access.key", self.minio_access_key) \
                .config("spark.hadoop.fs.s3a.secret.key", self.minio_secret_key) \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                .config("spark.sql.streaming.checkpointLocation", self.checkpoint_dir) \
                .config("spark.sql.streaming.schemaInference", "true") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.streaming.stopGracefullyOnShutdown", "true") \
                .getOrCreate()
            
            logger.info(f"✅ Spark session created: {spark.version}")
            return spark
        
        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            raise
    
    def process_table_stream(
        self,
        source: str,
        table_name: str,
        transformation_config: Optional[Dict[str, Any]] = None
    ):
        """
        Start streaming Silver processing for a specific Bronze CDC table.
        
        Args:
            source: Source system (postgres, mariadb)
            table_name: Table name
            transformation_config: Custom transformation rules
        """
        try:
            bronze_table = f"{self.catalog_name}.{self.bronze_namespace}.{source}_{table_name}"
            silver_table = f"{self.catalog_name}.{self.silver_namespace}.{source}_{table_name}"
            checkpoint_path = f"{self.checkpoint_dir}/{source}/{table_name}"
            
            logger.info(f"🔄 Starting Silver CDC stream: {bronze_table} → {silver_table}")
            
            # Ensure Silver table exists
            self._ensure_silver_table(source, table_name)
            
            # Read Bronze stream
            bronze_stream = self.spark.readStream \
                .format("iceberg") \
                .table(bronze_table) \
                .option("stream-from-timestamp", 
                        str(int(datetime.now().timestamp() * 1000))) \
                .option("max-files-per-trigger", self.max_offsets_per_trigger)
            
            logger.info(f"📖 Reading from Bronze: {bronze_table}")
            
            # Apply Silver transformations
            silver_stream = self._apply_transformations(
                bronze_stream, 
                source, 
                table_name,
                transformation_config
            )
            
            # Write to Silver with quality checks
            query = silver_stream.writeStream \
                .format("iceberg") \
                .outputMode("append") \
                .option("checkpointLocation", checkpoint_path) \
                .option("fanout-enabled", "true") \
                .trigger(processingTime=f"{self.checkpoint_interval} seconds") \
                .foreachBatch(lambda batch_df, batch_id: 
                    self._write_with_quality_checks(
                        batch_df, batch_id, silver_table, source, table_name
                    )) \
                .start()
            
            logger.info(f"✅ Silver CDC stream started: {silver_table}")
            logger.info(f"   Checkpoint: {checkpoint_path}")
            logger.info(f"   Trigger interval: {self.checkpoint_interval}s")
            
            return query
        
        except Exception as e:
            logger.error(f"Failed to start Silver stream for {source}.{table_name}: {e}")
            raise
    
    def _ensure_silver_table(self, source: str, table_name: str):
        """Ensure Silver Iceberg table exists (create if not)"""
        try:
            silver_table = f"{self.catalog_name}.{self.silver_namespace}.{source}_{table_name}"
            bronze_table = f"{self.catalog_name}.{self.bronze_namespace}.{source}_{table_name}"
            
            # Check if table exists
            tables = self.spark.sql(
                f"SHOW TABLES IN {self.catalog_name}.{self.silver_namespace}"
            ).collect()
            
            table_exists = any(row.tableName == f"{source}_{table_name}" for row in tables)
            
            if not table_exists:
                logger.info(f"Creating Silver table: {silver_table}")
                
                # Get Bronze schema
                bronze_df = self.spark.read.format("iceberg").table(bronze_table).limit(1)
                
                # Create Silver table with additional metadata columns
                create_sql = f"""
                CREATE TABLE {silver_table}
                USING iceberg
                PARTITIONED BY (days(silver_processed_at))
                TBLPROPERTIES (
                    'write.format.default' = 'parquet',
                    'write.parquet.compression-codec' = 'snappy',
                    'write.metadata.compression-codec' = 'gzip',
                    'format-version' = '2'
                )
                AS SELECT 
                    *,
                    CAST(NULL AS TIMESTAMP) as silver_processed_at,
                    CAST(NULL AS DOUBLE) as silver_quality_score,
                    CAST(NULL AS STRING) as silver_validation_status
                FROM {bronze_table}
                WHERE 1=0
                """
                
                self.spark.sql(create_sql)
                logger.info(f"✅ Created Silver table: {silver_table}")
            else:
                logger.info(f"Silver table already exists: {silver_table}")
        
        except Exception as e:
            logger.error(f"Failed to ensure Silver table: {e}")
            raise
    
    def _apply_transformations(
        self,
        df: DataFrame,
        source: str,
        table_name: str,
        config: Optional[Dict[str, Any]] = None
    ) -> DataFrame:
        """
        Apply Silver layer transformations to streaming DataFrame.
        
        Args:
            df: Input Bronze DataFrame
            source: Source system
            table_name: Table name
            config: Custom transformation configuration
        
        Returns:
            Transformed Silver DataFrame
        """
        try:
            logger.info(f"Applying transformations to {source}.{table_name}")
            
            # 1. Basic Cleaning
            df_clean = self._basic_cleaning(df)
            
            # 2. Type Casting & Validation
            df_typed = self._type_casting(df_clean, table_name)
            
            # 3. Deduplication (based on primary key from CDC)
            df_dedup = self._deduplication(df_typed, source, table_name)
            
            # 4. Data Quality Validation
            df_validated = self._quality_validation(df_dedup, source, table_name)
            
            # 5. Add Silver metadata
            df_final = df_validated \
                .withColumn("silver_processed_at", current_timestamp()) \
                .withColumn("silver_quality_score", lit(None).cast("double")) \
                .withColumn("silver_validation_status", lit("pending"))
            
            logger.info(f"✅ Transformations applied to {source}.{table_name}")
            
            return df_final
        
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            raise
    
    def _basic_cleaning(self, df: DataFrame) -> DataFrame:
        """Apply basic data cleaning transformations"""
        try:
            # Trim whitespace from all string columns
            string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
            
            for col_name in string_cols:
                df = df.withColumn(col_name, trim(col(col_name)))
            
            logger.debug(f"Cleaned {len(string_cols)} string columns")
            return df
        
        except Exception as e:
            logger.error(f"Basic cleaning failed: {e}")
            return df
    
    def _type_casting(self, df: DataFrame, table_name: str) -> DataFrame:
        """Apply type casting based on table schema"""
        try:
            # Table-specific type casting rules
            if 'transaction' in table_name.lower():
                if 'amount' in df.columns:
                    df = df.withColumn('amount', 
                        coalesce(col('amount').cast('double'), lit(0.0)))
                
                if 'transaction_date' in df.columns:
                    df = df.withColumn('transaction_date',
                        col('transaction_date').cast('timestamp'))
            
            if 'user' in table_name.lower():
                if 'email' in df.columns:
                    df = df.withColumn('email', lower(trim(col('email'))))
                
                if 'created_at' in df.columns:
                    df = df.withColumn('created_at',
                        col('created_at').cast('timestamp'))
            
            logger.debug(f"Type casting applied to {table_name}")
            return df
        
        except Exception as e:
            logger.error(f"Type casting failed: {e}")
            return df
    
    def _deduplication(self, df: DataFrame, source: str, table_name: str) -> DataFrame:
        """Remove duplicates based on CDC metadata"""
        try:
            # Use CDC timestamp and operation type for deduplication
            if '_cdc_timestamp' in df.columns and '_cdc_lsn' in df.columns:
                # Window by primary key, order by CDC timestamp DESC
                # Keep the most recent change for each key
                window_spec = Window.partitionBy('_cdc_key').orderBy(
                    col('_cdc_timestamp').desc(),
                    col('_cdc_lsn').desc()
                )
                
                df_dedup = df.withColumn('_row_num', row_number().over(window_spec)) \
                    .filter(col('_row_num') == 1) \
                    .drop('_row_num')
                
                logger.debug(f"Deduplication applied using CDC metadata")
                return df_dedup
            else:
                logger.warning("CDC metadata not found, skipping deduplication")
                return df
        
        except Exception as e:
            logger.error(f"Deduplication failed: {e}")
            return df
    
    def _quality_validation(self, df: DataFrame, source: str, table_name: str) -> DataFrame:
        """Apply data quality validation rules"""
        try:
            # Completeness checks - mark records with critical NULL values
            critical_columns = self._get_critical_columns(table_name)
            
            for col_name in critical_columns:
                if col_name in df.columns:
                    df = df.withColumn(
                        f"_dq_null_{col_name}",
                        when(col(col_name).isNull(), True).otherwise(False)
                    )
            
            # Validity checks
            if 'email' in df.columns:
                df = df.withColumn(
                    "_dq_invalid_email",
                    when(
                        col('email').rlike(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
                        False
                    ).otherwise(True)
                )
            
            if 'amount' in df.columns:
                df = df.withColumn(
                    "_dq_negative_amount",
                    when(col('amount') < 0, True).otherwise(False)
                )
            
            logger.debug(f"Quality validation applied to {table_name}")
            return df
        
        except Exception as e:
            logger.error(f"Quality validation failed: {e}")
            return df
    
    def _get_critical_columns(self, table_name: str) -> List[str]:
        """Get list of critical columns that should not be NULL"""
        critical_columns_map = {
            'users': ['user_id', 'email'],
            'transactions': ['transaction_id', 'amount', 'transaction_date'],
            'orders': ['order_id', 'customer_id', 'order_date'],
            'products': ['product_id', 'name', 'price']
        }
        
        for key, columns in critical_columns_map.items():
            if key in table_name.lower():
                return columns
        
        return []
    
    def _write_with_quality_checks(
        self,
        batch_df: DataFrame,
        batch_id: int,
        silver_table: str,
        source: str,
        table_name: str
    ):
        """
        Write batch to Silver with quality checks.
        
        Separates clean records (to Silver) from failed records (to Quarantine).
        """
        try:
            if batch_df.isEmpty():
                logger.debug(f"Batch {batch_id} is empty, skipping")
                return
            
            logger.info(f"Processing batch {batch_id} for {silver_table}")
            
            # Calculate quality score
            total_rows = batch_df.count()
            
            # Identify failed records (any DQ flag = True)
            dq_columns = [c for c in batch_df.columns if c.startswith('_dq_')]
            
            if dq_columns:
                # Create condition: any DQ flag is True
                failed_condition = None
                for dq_col in dq_columns:
                    if failed_condition is None:
                        failed_condition = col(dq_col) == True
                    else:
                        failed_condition = failed_condition | (col(dq_col) == True)
                
                # Split into clean and failed
                failed_df = batch_df.filter(failed_condition)
                clean_df = batch_df.filter(~failed_condition)
                
                failed_count = failed_df.count()
                clean_count = clean_df.count()
            else:
                # No DQ columns, all records are clean
                clean_df = batch_df
                failed_df = self.spark.createDataFrame([], batch_df.schema)
                failed_count = 0
                clean_count = total_rows
            
            # Calculate quality score
            quality_score = (clean_count / total_rows * 100) if total_rows > 0 else 100.0
            
            # Update quality score in clean records
            clean_df_final = clean_df \
                .withColumn("silver_quality_score", lit(quality_score)) \
                .withColumn("silver_validation_status", lit("passed"))
            
            # Remove DQ columns before writing to Silver
            for dq_col in dq_columns:
                clean_df_final = clean_df_final.drop(dq_col)
            
            # Write clean records to Silver
            if clean_count > 0:
                clean_df_final.writeTo(silver_table).append()
                logger.info(f"✅ Batch {batch_id}: Wrote {clean_count} clean records to Silver")
            
            # Write failed records to Quarantine
            if failed_count > 0:
                quarantine_table = f"{self.catalog_name}.quarantine.{source}_{table_name}_silver"
                self._write_to_quarantine(failed_df, quarantine_table, batch_id)
                logger.warning(f"⚠️ Batch {batch_id}: {failed_count} records sent to Quarantine")
            
            # Log metrics
            logger.info(f"📊 Batch {batch_id} Quality: {quality_score:.2f}% "
                       f"({clean_count}/{total_rows} passed)")
        
        except Exception as e:
            logger.error(f"Failed to write batch {batch_id}: {e}")
            raise
    
    def _write_to_quarantine(self, df: DataFrame, quarantine_table: str, batch_id: int):
        """Write failed records to quarantine table"""
        try:
            # Ensure quarantine table exists
            quarantine_namespace = "quarantine"
            tables = self.spark.sql(
                f"SHOW TABLES IN {self.catalog_name}.{quarantine_namespace}"
            ).collect()
            
            table_name = quarantine_table.split('.')[-1]
            table_exists = any(row.tableName == table_name for row in tables)
            
            if not table_exists:
                # Create quarantine table
                df.writeTo(quarantine_table) \
                    .tableProperty("write.format.default", "parquet") \
                    .create()
                logger.info(f"Created quarantine table: {quarantine_table}")
            
            # Append to quarantine
            df.withColumn("_quarantine_batch_id", lit(batch_id)) \
                .withColumn("_quarantine_timestamp", current_timestamp()) \
                .writeTo(quarantine_table) \
                .append()
            
            logger.info(f"Wrote {df.count()} records to quarantine: {quarantine_table}")
        
        except Exception as e:
            logger.error(f"Failed to write to quarantine: {e}")
    
    def start_all_sources(self, sources: List[str] = None):
        """
        Start Silver CDC processing for all configured sources.
        
        Args:
            sources: List of sources to process (default: ['postgres', 'mariadb'])
        """
        if sources is None:
            sources = ['postgres', 'mariadb']
        
        queries = []
        
        for source in sources:
            try:
                # Get list of Bronze CDC tables for this source
                bronze_tables = self._get_bronze_cdc_tables(source)
                
                logger.info(f"Found {len(bronze_tables)} Bronze CDC tables for {source}")
                
                for table_name in bronze_tables:
                    try:
                        query = self.process_table_stream(source, table_name)
                        queries.append(query)
                    except Exception as e:
                        logger.error(f"Failed to start stream for {source}.{table_name}: {e}")
            
            except Exception as e:
                logger.error(f"Failed to process source {source}: {e}")
        
        if queries:
            logger.info(f"✅ Started {len(queries)} Silver CDC streams")
            logger.info("Waiting for streaming queries to terminate...")
            
            try:
                # Wait for all queries
                for query in queries:
                    query.awaitTermination()
            except KeyboardInterrupt:
                logger.info("Stopping all queries...")
                for query in queries:
                    query.stop()
                logger.info("All queries stopped")
        else:
            logger.warning("No Silver CDC streams started")
    
    def _get_bronze_cdc_tables(self, source: str) -> List[str]:
        """Get list of Bronze CDC tables for a source"""
        try:
            tables = self.spark.sql(
                f"SHOW TABLES IN {self.catalog_name}.{self.bronze_namespace}"
            ).collect()
            
            # Filter tables that start with source prefix
            bronze_tables = [
                row.tableName.replace(f"{source}_", "")
                for row in tables
                if row.tableName.startswith(f"{source}_")
            ]
            
            return bronze_tables
        
        except Exception as e:
            logger.error(f"Failed to get Bronze tables for {source}: {e}")
            return []
    
    def get_metrics(self, source: str, table_name: str) -> Dict[str, Any]:
        """
        Get real-time metrics for a Silver CDC stream.
        
        Args:
            source: Source system
            table_name: Table name
        
        Returns:
            Dictionary with metrics
        """
        try:
            silver_table = f"{self.catalog_name}.{self.silver_namespace}.{source}_{table_name}"
            
            # Read Silver table
            df = self.spark.read.format("iceberg").table(silver_table)
            
            # Calculate metrics
            total_rows = df.count()
            avg_quality_score = df.agg(avg('silver_quality_score')).collect()[0][0]
            
            # Recent processing stats (last hour)
            recent_df = df.filter(
                col('silver_processed_at') >= current_timestamp() - expr('INTERVAL 1 HOUR')
            )
            recent_rows = recent_df.count()
            
            metrics = {
                'table': f"{source}.{table_name}",
                'total_rows': total_rows,
                'avg_quality_score': round(avg_quality_score, 2) if avg_quality_score else None,
                'recent_rows_1h': recent_rows,
                'last_updated': df.agg(spark_max('silver_processed_at')).collect()[0][0]
            }
            
            return metrics
        
        except Exception as e:
            logger.error(f"Failed to get metrics: {e}")
            return {}
    
    def cleanup(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Silver CDC Processor")
    parser.add_argument('--source', type=str, help='Source system (postgres, mariadb)')
    parser.add_argument('--table', type=str, help='Specific table to process')
    parser.add_argument('--minio-endpoint', type=str, default='http://localhost:9000')
    parser.add_argument('--checkpoint-interval', type=int, default=30, 
                       help='Checkpoint interval in seconds')
    parser.add_argument('--max-offsets', type=int, default=10000,
                       help='Max records per micro-batch')
    
    args = parser.parse_args()
    
    # Initialize processor
    processor = SilverCDCProcessor(
        minio_endpoint=args.minio_endpoint,
        checkpoint_interval=args.checkpoint_interval,
        max_offsets_per_trigger=args.max_offsets
    )
    
    try:
        if args.source and args.table:
            # Process specific table
            logger.info(f"Starting Silver CDC for {args.source}.{args.table}")
            query = processor.process_table_stream(args.source, args.table)
            query.awaitTermination()
        else:
            # Process all sources
            sources = [args.source] if args.source else None
            processor.start_all_sources(sources)
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
    finally:
        processor.cleanup()


if __name__ == "__main__":
    main()
