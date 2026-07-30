"""
Test Spark + Iceberg installation
Run this after Java 11 is installed
"""
import sys
import os
from pathlib import Path

def test_installations():
    """Test if all required packages are installed"""
    print("🔍 Testing Spark + Iceberg Installation\n")
    print("=" * 60)
    
    # Configure Hadoop for Windows (if needed)
    if os.name == 'nt':  # Windows
        hadoop_home = Path(__file__).parent / "hadoop"
        os.environ['HADOOP_HOME'] = str(hadoop_home)
        os.environ['PATH'] = f"{hadoop_home / 'bin'};{os.environ.get('PATH', '')}"
        print(f"🔧 Set HADOOP_HOME: {hadoop_home}\n")
    
    # Test 1: Check Python packages
    print("📦 Step 1: Checking Python packages...")
    try:
        import pyspark
        print(f"   ✅ PySpark {pyspark.__version__} installed")
    except ImportError as e:
        print(f"   ❌ PySpark not installed: {e}")
        return False
    
    try:
        import pyiceberg
        print(f"   ✅ PyIceberg {pyiceberg.__version__} installed")
    except ImportError as e:
        print(f"   ❌ PyIceberg not installed: {e}")
        return False
    
    try:
        import s3fs
        print(f"   ✅ S3FS installed (for MinIO connectivity)")
    except ImportError as e:
        print(f"   ❌ S3FS not installed: {e}")
        return False
    
    try:
        import pyarrow
        print(f"   ✅ PyArrow {pyarrow.__version__} installed")
    except ImportError as e:
        print(f"   ❌ PyArrow not installed: {e}")
        return False
    
    # Test 2: Check JAR file
    print("\n📦 Step 2: Checking Iceberg Spark runtime JAR...")
    jar_path = Path(__file__).parent / "jars" / "iceberg-spark-runtime-3.5_2.12-1.10.1.jar"
    if jar_path.exists():
        size_mb = jar_path.stat().st_size / (1024 * 1024)
        print(f"   ✅ JAR file found: {jar_path.name} ({size_mb:.2f} MB)")
    else:
        print(f"   ❌ JAR file not found: {jar_path}")
        return False
    
    # Test 3: Check Java installation
    print("\n📦 Step 3: Checking Java installation...")
    import subprocess
    try:
        result = subprocess.run(['java', '-version'], 
                              capture_output=True, 
                              text=True, 
                              timeout=5)
        # Java outputs version to stderr
        version_output = result.stderr if result.stderr else result.stdout
        if 'openjdk' in version_output.lower() or 'java' in version_output.lower():
            version_line = version_output.split('\n')[0]
            print(f"   ✅ {version_line}")
        else:
            print(f"   ⚠️  Java found but version unclear: {version_output[:100]}")
    except FileNotFoundError:
        print("   ❌ Java not found. Please install Java 11:")
        print("      Download: https://adoptium.net/temurin/releases/?version=11")
        print("      Or run: winget install --id EclipseAdoptium.Temurin.11.JDK -e")
        return False
    except Exception as e:
        print(f"   ❌ Error checking Java: {e}")
        return False
    
    # Test 4: Try to create a Spark session
    print("\n📦 Step 4: Testing Spark session creation...")
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("Iceberg_Installation_Test") \
            .master("local[1]") \
            .config("spark.jars", str(jar_path)) \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.test_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.test_catalog.type", "hadoop") \
            .config("spark.sql.catalog.test_catalog.warehouse", "file:///tmp/iceberg-test") \
            .getOrCreate()
        
        print(f"   ✅ Spark session created successfully")
        print(f"      - Spark version: {spark.version}")
        print(f"      - Master: {spark.sparkContext.master}")
        
        # Test DataFrame
        data = [("Alice", 25), ("Bob", 30)]
        df = spark.createDataFrame(data, ["name", "age"])
        
        print(f"      - Test DataFrame created with {df.count()} rows")
        
        spark.stop()
        print(f"   ✅ Spark session stopped successfully")
        
    except Exception as e:
        print(f"   ❌ Failed to create Spark session: {e}")
        print("\n   Troubleshooting:")
        print("   - Ensure Java 11 is installed and in PATH")
        print("   - Restart PowerShell after installing Java")
        print("   - Check that JAVA_HOME is set correctly")
        return False
    
    # Success!
    print("\n" + "=" * 60)
    print("🎉 All tests passed! Spark + Iceberg is ready to use")
    print("=" * 60)
    print("\n📚 Next steps:")
    print("   1. Read: SPARK_ICEBERG_INSTALLATION.md")
    print("   2. Configure: config/iceberg_config.yaml")
    print("   3. Test utility: python utils/spark_iceberg_config.py")
    print("   4. Start building: Implement silver_transformer_spark.py")
    
    return True


if __name__ == "__main__":
    success = test_installations()
    sys.exit(0 if success else 1)
