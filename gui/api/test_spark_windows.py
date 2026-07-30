"""
Test Spark initialization on Windows with winutils fix
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

def test_spark_initialization():
    """Test basic Spark initialization"""
    try:
        logger.info("\n" + "="*60)
        logger.info("Testing Spark Initialization on Windows")
        logger.info("="*60)
        
        # Build Spark session with Windows configs
        builder = SparkSession.builder \
            .appName("WindowsSparkTest") \
            .master("local[1]")
        
        # Apply Windows configurations
        if os.name == 'nt':
            for key, value in get_windows_spark_configs().items():
                builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info("✅ Spark session created successfully!")
        logger.info(f"   Spark Version: {spark.version}")
        logger.info(f"   Spark App Name: {spark.sparkContext.appName}")
        
        # Test simple DataFrame operation
        logger.info("\n🔧 Testing DataFrame operations...")
        df = spark.createDataFrame([
            (1, "Alice", 100),
            (2, "Bob", 200),
            (3, "Charlie", 300)
        ], ["id", "name", "amount"])
        
        logger.info("✅ Created test DataFrame:")
        df.show()
        
        # Test SQL query
        logger.info("🔧 Testing SQL query...")
        df.createOrReplaceTempView("test_table")
        result = spark.sql("SELECT name, amount * 2 as doubled FROM test_table WHERE amount > 150")
        
        logger.info("✅ SQL query executed successfully:")
        result.show()
        
        # Test aggregation
        logger.info("🔧 Testing aggregation...")
        agg_result = df.groupBy().sum("amount")
        logger.info("✅ Aggregation successful:")
        agg_result.show()
        
        # Clean up
        spark.stop()
        
        logger.info("\n" + "="*60)
        logger.info("✅ ALL TESTS PASSED! Spark is working on Windows!")
        logger.info("="*60)
        return True
        
    except Exception as e:
        logger.error(f"\n❌ TEST FAILED: {e}")
        logger.error("\nFull error traceback:", exc_info=True)
        return False

if __name__ == "__main__":
    success = test_spark_initialization()
    sys.exit(0 if success else 1)
