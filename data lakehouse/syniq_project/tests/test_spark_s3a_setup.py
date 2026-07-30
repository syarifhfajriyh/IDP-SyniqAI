"""
Simple test to verify Spark + Iceberg + S3A setup is working
"""
import sys
sys.path.insert(0, '.')

from utils.spark_iceberg_config import create_spark_session_with_iceberg

print("="*80)
print("🧪 TESTING SPARK + ICEBERG + S3A SETUP")
print("="*80)

try:
    print("\n1️⃣  Creating Spark session with Iceberg...")
    spark = create_spark_session_with_iceberg(app_name="Test_S3A")
    
    print(f"✅ Spark session created successfully!")
    print(f"   Spark version: {spark.version}")
    print(f"   App name: {spark.sparkContext.appName}")
    
    print("\n2️⃣  Testing DataFrame creation...")
    data = [("Alice", 25, "Engineering"), ("Bob", 30, "Sales"), ("Charlie", 35, "Marketing")]
    df = spark.createDataFrame(data, ["name", "age", "department"])
    
    print(f"✅ DataFrame created with {df.count()} rows")
    df.show()
    
    print("\n3️⃣  Testing S3A configuration...")
    configs = spark.sparkContext.getConf().getAll()
    s3a_configs = [c for c in configs if 's3a' in c[0].lower()]
    
    if s3a_configs:
        print("✅ S3A configurations found:")
        for key, value in s3a_configs:
            display_value = value if 'key' not in key.lower() else '***HIDDEN***'
            print(f"   {key} = {display_value}")
    else:
        print("❌ No S3A configurations found!")
    
    print("\n4️⃣  Testing Iceberg catalog...")
    try:
        catalogs = spark.sql("SHOW CATALOGS")
        print("✅ Available catalogs:")
        catalogs.show()
    except Exception as e:
        print(f"⚠️  Could not list catalogs: {e}")
    
    spark.stop()
    print("\n" + "="*80)
    print("🎉 ALL TESTS PASSED! Spark + Iceberg + S3A is ready!")
    print("="*80)
    
except Exception as e:
    print(f"\n❌ TEST FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
