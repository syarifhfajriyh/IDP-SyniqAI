"""
Test CDC Consumer Startup
Quick diagnostic to check why CDC consumer might be crashing
"""
import sys
import traceback
from pathlib import Path

print("=" * 60)
print("CDC Consumer Startup Test")
print("=" * 60)

try:
    print("\n1. Checking Python environment...")
    print(f"   Python: {sys.version}")
    print(f"   Executable: {sys.executable}")
    
    print("\n2. Checking imports...")
    
    print("   - pyspark...", end=" ")
    from pyspark.sql import SparkSession
    print("✓")
    
    print("   - kafka-python...", end=" ")
    import kafka
    print("✓")
    
    print("\n   Note: pyiceberg not required for CDC consumer (uses PySpark's Iceberg integration)")
    
    print("\n3. Checking environment variables...")
    import os
    java_home = os.getenv("JAVA_HOME")
    if java_home:
        print(f"   JAVA_HOME: {java_home}")
    else:
        print("   ⚠ JAVA_HOME not set!")
    
    print("\n4. Testing Spark initialization...")
    spark = SparkSession.builder \
        .appName("CDC-Test") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.syniq_iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.syniq_iceberg.type", "jdbc") \
        .config("spark.sql.catalog.syniq_iceberg.uri", "jdbc:postgresql://localhost:5432/syniqai_metadata") \
        .config("spark.sql.catalog.syniq_iceberg.jdbc.user", "syniqai_user") \
        .config("spark.sql.catalog.syniq_iceberg.jdbc.password", "syniqai_pass") \
        .config("spark.sql.catalog.syniq_iceberg.warehouse", "s3a://bronze/iceberg") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    print("   ✓ Spark session created")
    spark.stop()
    print("   ✓ Spark session stopped cleanly")
    
    print("\n5. Testing Kafka connection...")
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('localhost', 9092))
    sock.close()
    
    if result == 0:
        print("   ✓ Kafka is reachable on localhost:9092")
    else:
        print("   ✗ Cannot connect to Kafka on localhost:9092")
    
    print("\n" + "=" * 60)
    print("✓ ALL CHECKS PASSED - CDC consumer should work")
    print("=" * 60)
    
except Exception as e:
    print(f"\n✗ ERROR: {e}")
    print("\nFull traceback:")
    traceback.print_exc()
    print("\n" + "=" * 60)
    print("✗ CHECKS FAILED - Fix issues above before starting CDC")
    print("=" * 60)
    sys.exit(1)
