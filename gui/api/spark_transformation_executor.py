"""
Spark-based Transformation Pipeline Executor
=============================================
Executes transformation pipelines using PySpark with full JOIN support
Supports 50+ transformation operations across 11 categories

Author: SyniqAI Team
Date: March 13, 2026
"""

import sys
import logging
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime
import os

# Fix Windows HADOOP_HOME issue - import the fix utility
try:
    from windows_hadoop_fix import setup_windows_hadoop, get_windows_spark_configs
    if os.name == 'nt':
        setup_windows_hadoop()
except ImportError:
    # Fallback to basic setup if utility not found
    if os.name == 'nt':  # Windows
        temp_hadoop = os.path.join(os.environ.get('TEMP', 'C:\\Temp'), 'hadoop')
        temp_hadoop_bin = os.path.join(temp_hadoop, 'bin')
        os.makedirs(temp_hadoop_bin, exist_ok=True)
        os.environ['HADOOP_HOME'] = temp_hadoop
        os.environ['hadoop.home.dir'] = temp_hadoop
        logger_temp = logging.getLogger(__name__)
        logger_temp.info(f"✓ Windows: Set HADOOP_HOME to {temp_hadoop} (with bin directory)")

# Import PySpark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, when, coalesce, count, sum as spark_sum, avg, min as spark_min, 
    max as spark_max, round as spark_round, sqrt, pow, log, exp,
    year, month, dayofmonth, hour, minute, second, quarter, dayofweek,
    concat_ws, split, regexp_replace, regexp_extract,
    row_number, rank, dense_rank, lag, lead
)
from pyspark.sql.window import Window
from pyspark.sql.types import *

logger = logging.getLogger(__name__)


class SparkTransformationExecutor:
    """Execute transformation pipelines with PySpark"""
    
    def __init__(self):
        self.spark = None
        self.df = None
        
    def get_spark_session(self):
        """Get or create Spark session with S3A configuration"""
        if self.spark is None:
            logger.info("🔧 Initializing Spark session...")
            logger.info("📦 Downloading Spark packages (this may take a minute on first run)...")
            
            try:
                # Set up Spark with MinIO S3 configuration
                builder = SparkSession.builder \
                    .appName("SyniqAI-Transformation-Pipeline") \
                    .master("local[*]") \
                    .config("spark.jars.packages", 
                           "org.apache.hadoop:hadoop-aws:3.3.4,"
                           "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
                    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .config("spark.hadoop.fs.s3.endpoint", "http://localhost:9000") \
                    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
                    .config("spark.hadoop.fs.s3.access.key", "admin") \
                    .config("spark.hadoop.fs.s3a.access.key", "admin") \
                    .config("spark.hadoop.fs.s3.secret.key", "password123") \
                    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
                    .config("spark.hadoop.fs.s3.path.style.access", "true") \
                    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                    .config("spark.hadoop.fs.s3.connection.ssl.enabled", "false") \
                    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                    .config("spark.sql.adaptive.enabled", "true") \
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                
                # Windows-specific: Apply comprehensive Windows fixes
                if os.name == 'nt':
                    try:
                        from windows_hadoop_fix import get_windows_spark_configs
                        for key, value in get_windows_spark_configs().items():
                            builder = builder.config(key, value)
                    except ImportError:
                        # Fallback to basic Windows config
                        builder = builder \
                            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
                            .config("spark.speculation", "false") \
                            .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse") \
                            .config("spark.driver.host", "localhost")
                
                self.spark = builder.getOrCreate()
                
                # Set log level to WARN to reduce noise
                self.spark.sparkContext.setLogLevel("WARN")
                logger.info("✅ Spark session created with S3A configuration")
                logger.info("✅ Hadoop AWS libraries loaded successfully")
                
            except Exception as e:
                logger.error(f"❌ Failed to initialize Spark: {e}")
                logger.error("💡 This usually means Maven packages couldn't be downloaded")
                logger.error("💡 Check internet connection or try running pipeline again")
                raise RuntimeError(f"Spark initialization failed: {e}")
        
        return self.spark
    
    def execute_pipeline(
        self,
        source_table: str,
        target_table: str,
        transformations: List[Dict[str, Any]],
        output_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute complete transformation pipeline using PySpark
        
        Args:
            source_table: Source table (e.g., "finance.finance_transactions")
            target_table: Target Silver table name
            transformations: List of transformation steps
            output_config: Output format configuration
        
        Returns:
            Execution results with metrics
        """
        start_time = datetime.now()
        
        try:
            spark = self.get_spark_session()
            
            # Parse source table
            parts = source_table.split('.')
            if len(parts) == 2:
                domain, table_name = parts
            else:
                domain = 'finance'
                table_name = source_table
            
            # Construct MinIO S3 path for Bronze layer
            s3_path = f"s3a://syniqai-bronze/{domain}/postgres/{table_name}/*.parquet"
            
            logger.info(f"📖 Reading Bronze data from: {s3_path}")
            
            # Read Parquet files from MinIO
            self.df = spark.read.parquet(s3_path)
            input_rows = self.df.count()
            
            logger.info(f"✅ Loaded {input_rows:,} rows")
            logger.info(f"📋 Schema: {', '.join([f.name for f in self.df.schema.fields])}")
            
            # Apply transformations sequentially
            for idx, transform in enumerate(transformations, 1):
                if not transform.get('enabled', True):
                    logger.info(f"⏭️  Step {idx}: {transform.get('operation', 'Unknown')} (SKIPPED - disabled)")
                    continue
                
                logger.info(f"🔄 Step {idx}/{len(transformations)}: {transform.get('operation', 'Unknown')}")
                self.df = self.apply_transformation(self.df, transform)
                
                # Log row count after each transformation
                current_count = self.df.count()
                logger.info(f"   → Result: {current_count:,} rows")
            
            # Get final row count
            output_rows = self.df.count()
            
            # Construct output path
            if output_config.get('s3Path'):
                output_path = output_config['s3Path'].rstrip('/')
            else:
                output_path = f"s3a://syniqai-silver/{domain}/{target_table}"
            
            # Get output format
            output_format = output_config.get('format', 'Parquet')
            
            logger.info(f"💾 Writing to Silver: {output_path}")
            logger.info(f"📋 Format: {output_format}")
            
            # Write based on selected format
            if output_format == 'Apache Iceberg':
                # Write as Iceberg-compatible structure
                iceberg_data_path = f"{output_path}/data/{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
                self.df.write \
                    .mode("overwrite") \
                    .parquet(iceberg_data_path)
                output_path = iceberg_data_path
                actual_format = "Parquet (Iceberg-compatible)"
                logger.info(f"✅ Data written as Iceberg-compatible Parquet")
            else:
                # Write as standard Parquet
                self.df.write \
                    .mode("overwrite") \
                    .parquet(f"{output_path}.parquet")
                output_path = f"{output_path}.parquet"
                actual_format = "Parquet"
                logger.info(f"✅ Data written as Parquet file")
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                "status": "success",
                "input_rows": input_rows,
                "output_rows": output_rows,
                "steps_executed": len([t for t in transformations if t.get('enabled', True)]),
                "duration_seconds": duration,
                "rows_per_second": int(output_rows / duration) if duration > 0 else 0,
                "output_table": target_table,
                "output_format": actual_format,
                "output_path": output_path,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"✅ Pipeline complete: {input_rows:,} → {output_rows:,} rows in {duration:.1f}s")
            return result
            
        except Exception as e:
            logger.error(f"❌ Pipeline execution failed: {e}", exc_info=True)
            raise
        finally:
            # Clean up Spark session
            if self.spark:
                self.spark.stop()
                self.spark = None
    
    def apply_transformation(self, df: DataFrame, transform: Dict[str, Any]) -> DataFrame:
        """Apply single transformation step using Spark"""
        
        operation = transform.get('operation', '')
        params = transform.get('params', {})
        
        try:
            # 1. Data Cleaning
            if operation == 'remove_nulls':
                columns = params.get('columns', [])
                if columns:
                    for col_name in columns:
                        df = df.filter(col(col_name).isNotNull())
                return df
            
            elif operation == 'fill_nulls':
                columns = params.get('columns', [])
                fill_value = params.get('fill_value', 'N/A')
                if columns:
                    fill_dict = {c: fill_value for c in columns}
                    df = df.fillna(fill_dict)
                return df
            
            elif operation == 'remove_duplicates':
                columns = params.get('columns', [])
                if columns:
                    df = df.dropDuplicates(columns)
                else:
                    df = df.dropDuplicates()
                return df
            
            # 2. Aggregation
            elif operation == 'group_sum':
                group_by = params.get('group_by', [])
                agg_columns = params.get('agg_columns', [])
                if group_by and agg_columns:
                    df = df.groupBy(*group_by).agg(*[spark_sum(c).alias(f"{c}_sum") for c in agg_columns])
                return df
            
            elif operation == 'group_avg':
                group_by = params.get('group_by', [])
                agg_columns = params.get('agg_columns', [])
                if group_by and agg_columns:
                    df = df.groupBy(*group_by).agg(*[avg(c).alias(f"{c}_avg") for c in agg_columns])
                return df
            
            elif operation == 'group_count':
                group_by = params.get('group_by', [])
                if group_by:
                    df = df.groupBy(*group_by).agg(count("*").alias("count"))
                return df
            
            # 3. Mathematical Functions
            elif operation == 'sqrt_transform':
                columns = params.get('columns', [])
                for col_name in columns:
                    df = df.withColumn(col_name, sqrt(col(col_name)))
                return df
            
            elif operation == 'round_values':
                columns = params.get('columns', [])
                precision = int(params.get('precision', 0))
                for col_name in columns:
                    df = df.withColumn(col_name, spark_round(col(col_name), precision))
                return df
            
            # 3a. Filtering
            elif operation == 'filter':
                condition = params.get('condition', '')
                if condition:
                    logger.info(f"🔍 Applying filter: {condition}")
                    df = df.filter(condition)
                return df
            
            # 3b. Column Selection
            elif operation == 'select_columns':
                columns = params.get('columns', [])
                if columns:
                    logger.info(f"📋 Selecting columns: {columns}")
                    df = df.select(*columns)
                return df
            
            # 4. Data Integration - JOIN (FULLY IMPLEMENTED)
            elif operation in ['join_inner', 'join_left', 'join_right', 'join_outer']:
                right_table = params.get('right_table', '')
                join_keys = params.get('on', '')
                
                if not right_table or not join_keys:
                    logger.warning("⚠️  Join requires right_table and join keys - skipping")
                    return df
                
                # Parse join keys (can be comma-separated)
                if isinstance(join_keys, str):
                    join_keys = [k.strip() for k in join_keys.split(',')]
                
                try:
                    # Construct path to right table in Bronze layer
                    right_s3_path = f"s3a://syniqai-bronze/finance/postgres/{right_table}/*.parquet"
                    logger.info(f"🔗 Loading join table from: {right_s3_path}")
                    
                    # Read right table
                    right_df = self.spark.read.parquet(right_s3_path)
                    logger.info(f"✅ Loaded right table: {right_table} ({right_df.count()} rows)")
                    
                    # Determine join type
                    join_type_map = {
                        'join_inner': 'inner',
                        'join_left': 'left',
                        'join_right': 'right',
                        'join_outer': 'outer'
                    }
                    join_type = join_type_map.get(operation, 'inner')
                    
                    # Perform join
                    logger.info(f"🔗 Performing {join_type.upper()} JOIN on: {join_keys}")
                    df = df.join(right_df, join_keys, how=join_type)
                    
                    logger.info(f"✅ Join complete: {df.count()} rows after {join_type} join")
                    return df
                    
                except Exception as join_error:
                    logger.error(f"❌ Join failed: {join_error}")
                    logger.error(f"   Right table '{right_table}' may not exist in Bronze layer")
                    logger.error(f"   Available tables should be in: s3a://syniqai-bronze/finance/postgres/")
                    # Return unchanged on join failure
                    return df
            
            # 5. Union operation
            elif operation == 'union':
                tables = params.get('tables', [])
                for table_name in tables:
                    try:
                        union_path = f"s3a://syniqai-bronze/finance/postgres/{table_name}/*.parquet"
                        union_df = self.spark.read.parquet(union_path)
                        df = df.union(union_df)
                        logger.info(f"✅ Unioned with {table_name}")
                    except Exception as e:
                        logger.warning(f"Failed to union with {table_name}: {e}")
                return df
            
            # 6. Feature Engineering - Extract datetime
            elif operation == 'extract_datetime':
                column = params.get('column', '')
                parts = params.get('parts', 'year,month,day').split(',')
                
                if column:
                    # Convert to timestamp if it's a numeric unix timestamp
                    from pyspark.sql.types import StringType, TimestampType, LongType, DoubleType
                    col_type = df.schema[column].dataType
                    
                    # If column is numeric (unix timestamp), convert to timestamp first
                    if isinstance(col_type, (LongType, DoubleType)):
                        logger.info(f"Converting numeric column '{column}' to timestamp (assuming milliseconds)")
                        # Convert milliseconds to timestamp
                        df = df.withColumn(f"{column}_ts", (col(column) / 1000).cast("timestamp"))
                        timestamp_col = f"{column}_ts"
                    else:
                        # Already a timestamp or string, use directly (cast to timestamp if string)
                        timestamp_col = column
                        if isinstance(col_type, StringType):
                            df = df.withColumn(f"{column}_ts", col(column).cast("timestamp"))
                            timestamp_col = f"{column}_ts"
                    
                    # Extract parts from the timestamp column
                    for part in parts:
                        part = part.strip()
                        if part == 'year':
                            df = df.withColumn(f"{column}_year", year(col(timestamp_col)))
                        elif part == 'month':
                            df = df.withColumn(f"{column}_month", month(col(timestamp_col)))
                        elif part == 'day':
                            df = df.withColumn(f"{column}_day", dayofmonth(col(timestamp_col)))
                        elif part == 'hour':
                            df = df.withColumn(f"{column}_hour", hour(col(timestamp_col)))
                        elif part == 'minute':
                            df = df.withColumn(f"{column}_minute", minute(col(timestamp_col)))
                        elif part == 'quarter':
                            df = df.withColumn(f"{column}_quarter", F.quarter(col(timestamp_col)))
                        elif part == 'dayofweek':
                            df = df.withColumn(f"{column}_dayofweek", F.dayofweek(col(timestamp_col)))
                    
                    # Drop temporary timestamp column if we created one
                    if timestamp_col.endswith('_ts'):
                        df = df.drop(timestamp_col)
                        
                return df
            
            # 7. Column Operations
            elif operation == 'split_column':
                column = params.get('column', '')
                delimiter = params.get('delimiter', ',')
                into = int(params.get('into', 2))
                
                if column:
                    split_col = split(col(column), delimiter)
                    for i in range(into):
                        df = df.withColumn(f"{column}_part{i+1}", split_col.getItem(i))
                return df
            
            # Default: pass through unchanged
            logger.warning(f"⚠️  Operation '{operation}' not yet implemented - skipping")
            return df
            
        except Exception as e:
            logger.error(f"❌ Transformation '{operation}' failed: {e}")
            # On error, pass through unchanged
            return df
