"""
Gold Transformation Service - Backend API Integration
Handles Gold layer transformations with PySpark and Iceberg
"""

import os
import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

# Import job tracker for monitoring
from silver_job_tracker import job_tracker

# Fix Windows HADOOP_HOME issue
try:
    from windows_hadoop_fix import setup_windows_hadoop, get_windows_spark_configs
    if os.name == 'nt':
        setup_windows_hadoop()
except ImportError:
    if os.name == 'nt':  # Windows
        temp_hadoop = os.path.join(os.environ.get('TEMP', 'C:\\Temp'), 'hadoop')
        temp_hadoop_bin = os.path.join(temp_hadoop, 'bin')
        os.makedirs(temp_hadoop_bin, exist_ok=True)
        os.environ['HADOOP_HOME'] = temp_hadoop
        os.environ['hadoop.home.dir'] = temp_hadoop

from pyspark.sql import SparkSession

# Add project paths
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from transformations.gold_engine import GoldTransformationEngine

logger = logging.getLogger(__name__)


class GoldTransformationService:
    """Service for managing Gold transformations via API"""
    
    def __init__(self):
        self.spark = None
        self.engine = None
        self._initialize_spark()
    
    def _initialize_spark(self):
        """Initialize Spark session with Iceberg + S3A support"""
        try:
            logger.info("🔧 Initializing Spark session for Gold service...")
            logger.info("📦 Downloading Spark packages if needed...")
            
            builder = SparkSession.builder \
                .appName("GoldTransformationService") \
                .master("local[*]") \
                .config("spark.jars.packages", 
                       "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
                       "org.apache.hadoop:hadoop-aws:3.3.4") \
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config("spark.sql.catalog.syniqai_silver", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.syniqai_silver.type", "hadoop") \
                .config("spark.sql.catalog.syniqai_silver.warehouse", "s3a://syniqai-silver") \
                .config("spark.sql.catalog.syniqai_gold", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.syniqai_gold.type", "hadoop") \
                .config("spark.sql.catalog.syniqai_gold.warehouse", "s3a://syniqai-gold") \
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
            
            # Windows-specific configs
            if os.name == 'nt':
                try:
                    from windows_hadoop_fix import get_windows_spark_configs
                    for key, value in get_windows_spark_configs().items():
                        builder = builder.config(key, value)
                except ImportError:
                    builder = builder \
                        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
                        .config("spark.speculation", "false") \
                        .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse") \
                        .config("spark.driver.host", "localhost")
            
            self.spark = builder.getOrCreate()
            self.spark.sparkContext.setLogLevel("WARN")
            self.engine = GoldTransformationEngine(self.spark)
            logger.info("✅ Spark session initialized for Gold transformations")
            logger.info("✅ Iceberg + Hadoop AWS libraries loaded successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Spark: {e}")
            logger.error("💡 Check internet connection for Maven package downloads")
            raise
    
    def get_silver_tables(self) -> List[Dict]:
        """List all available Silver Iceberg tables"""
        try:
            tables = []
            
            # Query Iceberg catalog for Silver tables
            try:
                # List namespaces (domains) in Silver catalog
                namespaces = self.spark.sql("SHOW NAMESPACES IN syniqai_silver").collect()
                
                for ns_row in namespaces:
                    namespace = ns_row['namespace']
                    
                    # List tables in this namespace
                    try:
                        tables_in_ns = self.spark.sql(f"SHOW TABLES IN syniqai_silver.{namespace}").collect()
                        
                        for table_row in tables_in_ns:
                            table_name = table_row['tableName']
                            full_name = f"syniqai_silver.{namespace}.{table_name}"
                            
                            # Get row count
                            try:
                                count_df = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {full_name}")
                                row_count = count_df.collect()[0]['cnt']
                            except:
                                row_count = 0
                            
                            tables.append({
                                "name": table_name,
                                "full_name": full_name,
                                "domain": namespace,
                                "row_count": row_count
                            })
                    except Exception as e:
                        logger.warning(f"Failed to list tables in {namespace}: {e}")
            
            except Exception as e:
                logger.warning(f"Failed to list Silver namespaces: {e}")
                # Fallback: return sample tables
                tables = [
                    {
                        "name": "march_user_transactions",
                        "full_name": "syniqai_silver.finance.march_user_transactions",
                        "domain": "finance",
                        "row_count": 0
                    },
                    {
                        "name": "finance_transactions",
                        "full_name": "syniqai_silver.finance.finance_transactions",
                        "domain": "finance",
                        "row_count": 0
                    }
                ]
            
            return tables
            
        except Exception as e:
            logger.error(f"Failed to list Silver tables: {e}")
            return []
    
    def execute_aggregation(self, 
                           silver_table: str,
                           gold_table: str,
                           group_by_cols: List[str],
                           aggregations: List[Dict],
                           filters: Optional[List[Dict]] = None) -> Dict:
        """Execute aggregation transformation"""
        try:
            result = self.engine.execute_aggregation(
                silver_table=silver_table,
                gold_table=gold_table,
                group_by_cols=group_by_cols,
                aggregations=aggregations,
                filters=filters
            )
            return result
        except Exception as e:
            logger.error(f"Aggregation failed: {e}")
            raise
    
    def execute_join(self,
                     tables: List[Dict],
                     join_conditions: List[Dict],
                     select_columns: List[str],
                     gold_table: str) -> Dict:
        """Execute multi-table join transformation"""
        try:
            result = self.engine.execute_join(
                tables=tables,
                join_conditions=join_conditions,
                select_columns=select_columns,
                gold_table=gold_table
            )
            return result
        except Exception as e:
            logger.error(f"Join failed: {e}")
            raise
    
    def execute_customer_segmentation(self,
                                     silver_table: str,
                                     gold_table: str) -> Dict:
        """Execute RFM customer segmentation"""
        try:
            result = self.engine.execute_customer_segmentation(
                silver_table=silver_table,
                gold_table=gold_table
            )
            return result
        except Exception as e:
            logger.error(f"Segmentation failed: {e}")
            raise


# Singleton instance
_gold_service_instance = None

def get_gold_service() -> GoldTransformationService:
    """Get or create Gold transformation service instance"""
    global _gold_service_instance
    
    if _gold_service_instance is None:
        _gold_service_instance = GoldTransformationService()
    
    return _gold_service_instance
