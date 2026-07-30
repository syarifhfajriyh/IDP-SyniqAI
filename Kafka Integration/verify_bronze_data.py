#!/usr/bin/env python3
"""
Query Iceberg Bronze table to verify CDC data
"""
from pyspark.sql import SparkSession

# Initialize Spark with Iceberg and S3
spark = SparkSession.builder \
    .appName("VerifyBronzeData") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "s3a://bronze/iceberg") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

try:
    # Read from Iceberg table
    table_name = "local.bronze.postgres_loan_applications"
    
    print(f"\nReading from Iceberg table: {table_name}")
    print("=" * 60)
    
    df = spark.read.format("iceberg").load(table_name)
    
    print(f"\nTotal rows: {df.count()}")
    print(f"Columns: {', '.join(df.columns)}")
    
    # Show recent records
    print(f"\nRecent CDC records:")
    print("-" * 60)
    df.select("loan_id", "applicant_name", "loan_amount", "application_status", "op", "source_ts_ms") \
      .orderBy("source_ts_ms", ascending=False) \
      .show(10, truncate=False)
    
    print("\nCDC Pipeline verified successfully!")
    print("   PostgreSQL -> Debezium -> Kafka -> Spark -> Iceberg (MinIO)\n")
    
except Exception as e:
    print(f"\nError reading Iceberg table: {e}\n")
    
finally:
    spark.stop()
