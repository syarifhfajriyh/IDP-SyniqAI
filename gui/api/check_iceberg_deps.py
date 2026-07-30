"""Test Iceberg Write Capability"""

import sys

print("🔍 Checking Iceberg dependencies...\n")

# Check PyArrow
try:
    import pyarrow as pa
    print(f"✅ PyArrow: {pa.__version__}")
except ImportError:
    print("❌ PyArrow not installed")
    print("   Install: pip install pyarrow")
    sys.exit(1)

# Check PyIceberg
try:
    import pyiceberg
    print(f"✅ PyIceberg: {pyiceberg.__version__}")
except ImportError:
    print("❌ PyIceberg not installed")
    print("   Install: pip install pyiceberg")
    sys.exit(1)

# Check DuckDB
try:
    import duckdb
    print(f"✅ DuckDB: {duckdb.__version__}")
except ImportError:
    print("❌ DuckDB not installed")
    print("   Install: pip install duckdb")
    sys.exit(1)

print("\n✅ All dependencies available!")
print("\n📋 Next steps:")
print("   1. Ensure Iceberg REST catalog is running (http://localhost:8181)")
print("   2. Or configure catalog in transformation_pipeline_executor.py")
print("   3. Select 'Apache Iceberg' format in UI")
print("   4. Execute transformation")
