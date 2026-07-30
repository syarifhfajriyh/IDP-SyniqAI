"""
Spark + Iceberg configuration utilities for SyniqAI Lakehouse
Phase 4: Apache Iceberg Integration
"""
import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession


def load_iceberg_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load Iceberg configuration from YAML file
    
    Args:
        config_path: Path to iceberg_config.yaml (optional)
    
    Returns:
        Configuration dictionary
    """
    if config_path is None:
        config_path = Path(__file__).parent.parent / "config" / "iceberg_config.yaml"
    
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def create_spark_session_with_iceberg(
    app_name: str = "SyniqAI_Lakehouse",
    master: str = "local[*]",
    minio_config: Optional[Dict[str, Any]] = None
) -> SparkSession:
    """
    Create Spark session configured for Iceberg + MinIO
    Reuses existing session if available to prevent JVM crashes
    
    Args:
        app_name: Spark application name
        master: Spark master URL (default: local[*])
        minio_config: MinIO configuration dict (optional, will load from config if not provided)
    
    Returns:
        Configured SparkSession
    """
    # Try to get existing active session first
    try:
        existing_spark = SparkSession.getActiveSession()
        if existing_spark is not None:
            print(f"✓ Reusing existing Spark session: {existing_spark.version}")
            return existing_spark
    except Exception as e:
        print(f"No existing session found: {e}")
    
    # Configure Java (required for Spark/PySpark)
    # FORCE Java 17 (required for Spark 3.5+)
    if os.name == 'nt':  # Windows
        # Trust JAVA_HOME if already set correctly
        java_17_path = os.environ.get('JAVA_HOME', '')
        if not java_17_path or not os.path.isfile(os.path.join(java_17_path, 'bin', 'java.exe')):
            # Search common install locations for any JDK-17 folder
            import glob
            search_bases = [
                os.path.expandvars(r"%LOCALAPPDATA%\Programs\Eclipse Adoptium"),
                r"C:\Program Files\Eclipse Adoptium",
                r"C:\Program Files\Java",
                r"C:\Program Files\OpenJDK",
                r"C:\Program Files\Microsoft",
            ]
            java_17_path = ''
            for base in search_bases:
                for pattern in ("jdk-17*", "jdk17*"):
                    for path in glob.glob(os.path.join(base, pattern)):
                        if os.path.isfile(os.path.join(path, 'bin', 'java.exe')):
                            java_17_path = path
                            break
                    if java_17_path:
                        break
                if java_17_path:
                    break

        if java_17_path and os.path.isfile(os.path.join(java_17_path, 'bin', 'java.exe')):
            os.environ['JAVA_HOME'] = java_17_path
            os.environ['PATH'] = f"{java_17_path}\\bin;{os.environ.get('PATH', '')}"
            print(f"✓ Detected Java 17 at: {java_17_path}")
        else:
            raise RuntimeError(
                "JAVA_HOME not set and could not find Java 17.\n"
                "Please install Java 17 from https://adoptium.net/temurin/releases/?version=17"
            )
    else:
        if 'JAVA_HOME' not in os.environ:
            raise RuntimeError(
                "JAVA_HOME not set. Please install Java 17 and set JAVA_HOME environment variable."
            )
    
    # Configure Hadoop for Windows (required for Spark on Windows)
    if os.name == 'nt':  # Windows
        hadoop_home = Path(__file__).parent.parent / "hadoop"
        os.environ['HADOOP_HOME'] = str(hadoop_home)
        os.environ['PATH'] = f"{hadoop_home / 'bin'};{os.environ.get('PATH', '')}"
    
    # Load Iceberg config
    iceberg_cfg = load_iceberg_config()
    spark_cfg = iceberg_cfg['spark']
    catalog_cfg = iceberg_cfg['catalog']
    
    # Load MinIO credentials if not provided
    if minio_config is None:
        from utils.config_loader import load_config
        minio_config_path = Path(__file__).parent.parent / "config" / "minio_config.yaml"
        minio_config = load_config(str(minio_config_path))['minio']
    
    # Construct JAR paths
    iceberg_version = spark_cfg['iceberg_version']
    spark_version = spark_cfg['spark_version']
    scala_version = spark_cfg['scala_version']
    
    jar_filename = f"iceberg-spark-runtime-{spark_version}_{scala_version}-{iceberg_version}.jar"
    jar_path = Path(__file__).parent.parent / "jars" / jar_filename
    
    # Hadoop AWS JARs for S3A support
    hadoop_aws_jar = Path(__file__).parent.parent / "jars" / "hadoop-aws-3.3.4.jar"
    aws_sdk_jar = Path(__file__).parent.parent / "jars" / "aws-java-sdk-bundle-1.12.262.jar"
    
    # Ensure Iceberg JAR exists
    if not jar_path.exists():
        raise FileNotFoundError(
            f"Iceberg Spark runtime JAR not found: {jar_path}\n"
            f"Download from: https://repo1.maven.org/maven2/org/apache/iceberg/"
            f"iceberg-spark-runtime-{spark_version}_{scala_version}/{iceberg_version}/"
            f"{jar_filename}"
        )
    
    # Collect all JARs
    jars_list = [str(jar_path)]
    if hadoop_aws_jar.exists():
        jars_list.append(str(hadoop_aws_jar))
    else:
        print(f"⚠️  Warning: Hadoop AWS JAR not found: {hadoop_aws_jar}")
        print(f"   S3/MinIO access may not work. Download from:")
        print(f"   https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar")
    
    if aws_sdk_jar.exists():
        jars_list.append(str(aws_sdk_jar))
    else:
        print(f"⚠️  Warning: AWS SDK JAR not found: {aws_sdk_jar}")
        print(f"   S3/MinIO access may not work. Download from:")
        print(f"   https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar")
    
    # Build Spark session
    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master)
    
    # Add all JARs
    builder = builder.config("spark.jars", ",".join(jars_list))
    
    # Iceberg Spark extensions
    builder = builder.config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    
    # Configure Iceberg catalog (Silver/Gold — from iceberg_config.yaml)
    catalog_name = catalog_cfg['name']
    builder = builder.config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
    builder = builder.config(f"spark.sql.catalog.{catalog_name}.type", catalog_cfg['type'])
    builder = builder.config(f"spark.sql.catalog.{catalog_name}.warehouse", catalog_cfg['warehouse'])

    # Bronze catalog — must match the catalog name/warehouse spark_cdc_consumer.py
    # actually writes to (registered as "local" there, launched with
    # --warehouse s3a://syniqai-bronze/iceberg by start_platform.ps1). Without this,
    # any query against "local.bronze.*" here (e.g. CDC health/metrics endpoints in
    # backend.py) resolves against an unregistered/empty catalog.
    bronze_catalog_name = os.getenv("BRONZE_ICEBERG_CATALOG_NAME", "local")
    bronze_catalog_warehouse = os.getenv("BRONZE_ICEBERG_WAREHOUSE", "s3a://syniqai-bronze/iceberg")
    builder = builder.config(f"spark.sql.catalog.{bronze_catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
    builder = builder.config(f"spark.sql.catalog.{bronze_catalog_name}.type", "hadoop")
    builder = builder.config(f"spark.sql.catalog.{bronze_catalog_name}.warehouse", bronze_catalog_warehouse)
    
    # S3 (MinIO) configuration
    builder = builder.config("spark.hadoop.fs.s3a.endpoint", catalog_cfg['s3']['endpoint'])
    builder = builder.config("spark.hadoop.fs.s3a.access.key", minio_config['access_key'])
    builder = builder.config("spark.hadoop.fs.s3a.secret.key", minio_config['secret_key'])
    builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
    builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    builder = builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    
    # AWS Credentials Provider - use SimpleAWSCredentialsProvider (part of hadoop-aws)
    builder = builder.config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    # S3A timeout configurations (ALL must be in milliseconds for Java 17 compatibility)
    # Override ALL default duration strings like "60s" with numeric milliseconds
    builder = builder.config("spark.hadoop.fs.s3a.connection.timeout", "200000")  # 200 seconds
    builder = builder.config("spark.hadoop.fs.s3a.connection.establish.timeout", "50000")  # 50 seconds
    builder = builder.config("spark.hadoop.fs.s3a.socket.send.buffer", "8192")
    builder = builder.config("spark.hadoop.fs.s3a.socket.recv.buffer", "8192")
    builder = builder.config("spark.hadoop.fs.s3a.attempts.maximum", "10")
    builder = builder.config("spark.hadoop.fs.s3a.retry.interval", "500")  # 500ms
    builder = builder.config("spark.hadoop.fs.s3a.retry.limit", "10")
    builder = builder.config("spark.hadoop.fs.s3a.timeout.duration", "200000")  # 200 seconds
    builder = builder.config("spark.hadoop.fs.s3a.readahead.range", "65536")  # 64KB
    
    # Thread pool configurations (MUST be numeric, not duration strings)
    builder = builder.config("spark.hadoop.fs.s3a.threads.max", "10")
    builder = builder.config("spark.hadoop.fs.s3a.threads.core", "5")
    builder = builder.config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")  # 60 seconds in milliseconds
    builder = builder.config("spark.hadoop.fs.s3a.max.total.tasks", "10")
    builder = builder.config("spark.hadoop.fs.s3a.connection.maximum", "15")
    
    # Disable fs.s3a configs that use duration format
    builder = builder.config("spark.hadoop.fs.s3a.impl.disable.cache", "true")
    builder = builder.config("spark.hadoop.fs.s3a.fast.upload", "true")
    # CRITICAL for Windows: Use bytebuffer instead of disk to avoid winutils.exe dependency
    builder = builder.config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
    builder = builder.config("spark.hadoop.fs.s3a.fast.upload.active.blocks", "4")  # Limit memory usage
    builder = builder.config("spark.hadoop.fs.s3a.multipart.size", "104857600")  # 100MB
    builder = builder.config("spark.hadoop.fs.s3a.multipart.threshold", "104857600")  # 100MB
    builder = builder.config("spark.hadoop.fs.s3a.multipart.purge", "false")  # Disable purge to avoid duration config
    builder = builder.config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")  # 24 hours in milliseconds
    
    # Memory configuration
    builder = builder.config("spark.driver.memory", spark_cfg['driver_memory'])
    builder = builder.config("spark.executor.memory", spark_cfg['executor_memory'])
    
    # Additional Spark configs
    for key, value in spark_cfg['configs'].items():
        builder = builder.config(key, str(value))
    
    # Add stability configs to prevent JVM crashes
    builder = builder.config("spark.python.worker.reuse", "true")
    builder = builder.config("spark.python.worker.memory", "512m")
    builder = builder.config("spark.driver.maxResultSize", "1g")
    builder = builder.config("spark.network.timeout", "800s")
    builder = builder.config("spark.executor.heartbeatInterval", "60s")
    
    # Create session with retry logic
    max_retries = 3
    for attempt in range(max_retries):
        try:
            print(f"Creating Spark session (attempt {attempt + 1}/{max_retries})...")
            spark = builder.getOrCreate()
            
            # Set log level to reduce noise
            spark.sparkContext.setLogLevel("WARN")
            
            print(f"✓ Spark session created successfully: {spark.version}")
            return spark
            
        except Exception as e:
            print(f"✗ Failed to create Spark session (attempt {attempt + 1}): {e}")
            
            if attempt < max_retries - 1:
                # Try to cleanup before retry
                try:
                    from pyspark import SparkContext
                    if SparkContext._active_spark_context:
                        print("Stopping stale SparkContext...")
                        SparkContext._active_spark_context.stop()
                except Exception as cleanup_error:
                    print(f"Cleanup error: {cleanup_error}")
                
                import time
                time.sleep(2)  # Wait before retry
            else:
                raise RuntimeError(
                    f"Failed to create Spark session after {max_retries} attempts. "
                    f"Last error: {e}\n"
                    f"Possible causes:\n"
                    f"1. Java/Spark configuration issues\n"
                    f"2. Insufficient memory (try closing other applications)\n"
                    f"3. Port conflicts (check if another Spark job is running)\n"
                    f"4. Restart the backend service to clear stale sessions"
                )


def stop_spark_session(spark: Optional[SparkSession] = None):
    """
    Safely stop a Spark session
    
    Args:
        spark: SparkSession to stop (if None, stops active session)
    """
    try:
        if spark is None:
            spark = SparkSession.getActiveSession()
        
        if spark is not None:
            print("Stopping Spark session...")
            spark.stop()
            print("✓ Spark session stopped")
            
            # Also cleanup SparkContext if still active
            from pyspark import SparkContext
            if SparkContext._active_spark_context:
                SparkContext._active_spark_context.stop()
                print("✓ SparkContext stopped")
    except Exception as e:
        print(f"Error stopping Spark session: {e}")


def test_spark_iceberg_setup():
    """
    Test Spark + Iceberg configuration
    """
    print("🔍 Testing Spark + Iceberg setup...\n")
    
    try:
        # Create Spark session
        spark = create_spark_session_with_iceberg(app_name="Test_Iceberg")
        
        print(f"✅ Spark session created successfully!")
        print(f"   Spark version: {spark.version}")
        print(f"   Master: {spark.sparkContext.master}")
        print(f"   App name: {spark.sparkContext.appName}\n")
        
        # Test DataFrame creation
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        df = spark.createDataFrame(data, ["name", "age"])
        
        print("📊 Test DataFrame:")
        df.show()
        
        # Test catalog listing
        print("📂 Available catalogs:")
        spark.sql("SHOW CATALOGS").show()
        
        spark.stop()
        print("\n✅ All tests passed!")
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    test_spark_iceberg_setup()
