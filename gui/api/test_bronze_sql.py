"""
Test SQL query with correct Bronze table paths
"""
import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Import the fix utility
from windows_hadoop_fix import setup_windows_hadoop, get_windows_spark_configs

if os.name == 'nt':
    setup_windows_hadoop()

from pyspark.sql import SparkSession

def test_bronze_table_access():
    """Test accessing Bronze tables with correct MinIO paths"""
    try:
        logger.info("\n" + "="*70)
        logger.info("Testing Bronze Table Access from MinIO")
        logger.info("="*70)
        
        # Build Spark session with Windows configs and S3A
        builder = SparkSession.builder \
            .appName("BronzeTableTest") \
            .master("local[1]") \
            .config("spark.jars.packages", 
                   "org.apache.hadoop:hadoop-aws:3.3.4,"
                   "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "admin") \
            .config("spark.hadoop.fs.s3a.secret.key", "password123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        
        # Apply Windows configurations
        if os.name == 'nt':
            for key, value in get_windows_spark_configs().items():
                builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info("✅ Spark session created")
        
        # Test accessing finance_transactions with correct path
        logger.info("\n🔍 Testing Bronze table access...")
        s3_path = "s3a://syniqai-bronze/finance/postgres/finance_transactions/"
        
        logger.info(f"📂 Reading from: {s3_path}")
        
        df = spark.read.parquet(s3_path)
        
        logger.info(f"✅ Successfully read table!")
        logger.info(f"   Rows: {df.count():,}")
        logger.info(f"   Columns: {', '.join(df.columns)}")
        
        # Register as temp view
        df.createOrReplaceTempView("bronze.finance_transactions")
        logger.info("✅ Registered as 'bronze.finance_transactions'")
        
        # Test SQL query
        logger.info("\n🔧 Testing SQL query...")
        query = """
            SELECT 
                transaction_id, 
                user_id, 
                amount, 
                currency_code, 
                transaction_date
            FROM bronze.finance_transactions
            LIMIT 5
        """
        
        result = spark.sql(query)
        
        logger.info("✅ SQL query executed successfully!")
        logger.info("\n📊 Sample results:")
        result.show(5, truncate=False)
        
        # Test aggregation
        logger.info("🔧 Testing aggregation query...")
        agg_query = """
            SELECT 
                currency_code,
                COUNT(*) as transaction_count,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount
            FROM bronze.finance_transactions
            GROUP BY currency_code
            ORDER BY transaction_count DESC
        """
        
        agg_result = spark.sql(agg_query)
        
        logger.info("✅ Aggregation query executed!")
        logger.info("\n📊 Aggregation results:")
        agg_result.show(truncate=False)
        
        # Clean up
        spark.stop()
        
        logger.info("\n" + "="*70)
        logger.info("✅ ALL TESTS PASSED! Bronze tables are accessible!")
        logger.info("="*70)
        return True
        
    except Exception as e:
        logger.error(f"\n❌ TEST FAILED: {e}")
        logger.error("\nFull error traceback:", exc_info=True)
        return False

if __name__ == "__main__":
    success = test_bronze_table_access()
    sys.exit(0 if success else 1)
