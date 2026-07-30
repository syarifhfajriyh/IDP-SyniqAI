"""
Quick Test Script - Verify Syniq Data Lakehouse Setup

This script tests MinIO connectivity without requiring database setup.
Use this to verify your installation is working correctly.

Usage:
    python test_setup.py
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from utils import get_logger, load_config

logger = get_logger(__name__)


def test_minio_connection():
    """Test MinIO connection."""
    print("\n" + "="*70)
    print("TEST 1: MinIO Connection")
    print("="*70)
    
    try:
        from minio import Minio
        
        # Load MinIO config
        minio_config = load_config("config/minio_config.yaml")["minio"]
        
        print(f"\n📦 Connecting to MinIO...")
        print(f"   Endpoint: {minio_config['endpoint']}")
        print(f"   Access Key: {minio_config['access_key']}")
        
        # Create client
        client = Minio(
            minio_config["endpoint"],
            access_key=minio_config["access_key"],
            secret_key=minio_config["secret_key"],
            secure=minio_config.get("secure", False)
        )
        
        # List buckets
        buckets = list(client.list_buckets())
        
        print(f"\n✅ Connected successfully!")
        print(f"   Buckets found: {len(buckets)}")
        
        if buckets:
            for bucket in buckets:
                print(f"     - {bucket.name}")
        else:
            print(f"\n⚠️  No buckets found. Run: python main.py setup")
        
        return True
    
    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        print(f"\nTroubleshooting:")
        print(f"  1. Check MinIO is running: docker ps")
        print(f"  2. Check MinIO console: http://localhost:9001")
        print(f"  3. Verify credentials in config/minio_config.yaml")
        print(f"  4. See MINIO_TROUBLESHOOTING.md for help")
        return False


def test_config_files():
    """Test configuration files."""
    print("\n" + "="*70)
    print("TEST 2: Configuration Files")
    print("="*70)
    
    config_files = {
        "db_config.yaml": "config/db_config.yaml",
        "ingestion_config.yaml": "config/ingestion_config.yaml",
        "minio_config.yaml": "config/minio_config.yaml"
    }
    
    all_ok = True
    
    for name, path in config_files.items():
        try:
            config = load_config(path)
            print(f"\n✅ {name}")
            print(f"   Keys: {', '.join(config.keys())}")
        except Exception as e:
            print(f"\n❌ {name}: {e}")
            all_ok = False
    
    return all_ok


def test_imports():
    """Test critical imports."""
    print("\n" + "="*70)
    print("TEST 3: Module Imports")
    print("="*70)
    
    modules = [
        ("pandas", "Data processing"),
        ("pyarrow", "Parquet format"),
        ("minio", "MinIO client"),
        ("loguru", "Logging"),
        ("yaml", "YAML parsing"),
    ]
    
    all_ok = True
    
    for module_name, description in modules:
        try:
            __import__(module_name)
            print(f"✅ {module_name:20} - {description}")
        except ImportError as e:
            print(f"❌ {module_name:20} - Missing: {e}")
            all_ok = False
    
    return all_ok


def test_project_structure():
    """Test directory structure."""
    print("\n" + "="*70)
    print("TEST 4: Project Structure")
    print("="*70)
    
    required_dirs = [
        "config",
        "connectors",
        "utils",
        "ingestion",
        "logs",
        "examples"
    ]
    
    all_ok = True
    
    for dir_name in required_dirs:
        dir_path = Path(dir_name)
        if dir_path.exists():
            print(f"✅ {dir_name}/")
        else:
            print(f"❌ {dir_name}/ - Missing")
            all_ok = False
    
    return all_ok


def create_sample_data():
    """Create sample data for testing."""
    print("\n" + "="*70)
    print("TEST 5: Sample Data Generation")
    print("="*70)
    
    try:
        import pandas as pd
        from datetime import datetime, timedelta
        import random
        
        # Generate sample customers
        n_customers = 100
        
        customers = pd.DataFrame({
            'customer_id': range(1, n_customers + 1),
            'name': [f'Customer {i}' for i in range(1, n_customers + 1)],
            'email': [f'customer{i}@example.com' for i in range(1, n_customers + 1)],
            'age': [random.randint(18, 80) for _ in range(n_customers)],
            'created_at': [
                datetime.now() - timedelta(days=random.randint(0, 365))
                for _ in range(n_customers)
            ]
        })
        
        print(f"\n✅ Generated sample data:")
        print(f"   Customers: {len(customers)} rows")
        print(f"\n   Sample:")
        print(customers.head())
        
        return customers
    
    except Exception as e:
        print(f"\n❌ Failed to generate sample data: {e}")
        return None


def test_bronze_ingestor():
    """Test bronze ingestor with sample data."""
    print("\n" + "="*70)
    print("TEST 6: Bronze Ingestor (Sample Data)")
    print("="*70)
    
    try:
        from ingestion import BronzeIngestor
        from utils import load_config
        
        # Load MinIO config
        minio_config = load_config("config/minio_config.yaml")["minio"]
        
        # Create sample data
        import pandas as pd
        sample_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Test A', 'Test B', 'Test C'],
            'value': [100, 200, 300]
        })
        
        print(f"\n📊 Testing with sample data:")
        print(sample_data)
        
        # Create ingestor
        print(f"\n📦 Creating bronze ingestor...")
        ingestor = BronzeIngestor(
            minio_config=minio_config,
            bronze_bucket=minio_config["buckets"]["bronze"],
            enable_validation=False,
            enable_partitioning=True
        )
        
        print(f"✅ Bronze ingestor created")
        
        # Test ingestion
        print(f"\n📤 Ingesting sample data to MinIO...")
        result = ingestor.ingest_batch(
            df=sample_data,
            source_system="test_system",
            source_entity="test_table"
        )
        
        if result["success"]:
            print(f"\n✅ Ingestion successful!")
            print(f"   Rows written: {result['rows_written']}")
            print(f"   File path: {result['file_path']}")
            print(f"   File size: {result['file_size_bytes']} bytes")
            return True
        else:
            print(f"\n❌ Ingestion failed: {result.get('error', 'Unknown error')}")
            return False
    
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("\n")
    print("╔" + "="*68 + "╗")
    print("║" + " "*20 + "SYNIQ LAKEHOUSE - SETUP TEST" + " "*20 + "║")
    print("╚" + "="*68 + "╝")
    
    results = {}
    
    # Run tests
    results["imports"] = test_imports()
    results["structure"] = test_project_structure()
    results["configs"] = test_config_files()
    results["minio"] = test_minio_connection()
    
    # Only test ingestion if MinIO is working
    if results["minio"]:
        print(f"\n⚠️  Testing bronze ingestion with sample data...")
        print(f"   This will create a test file in MinIO bronze bucket")
        
        try:
            # Check if bronze bucket exists
            from minio import Minio
            minio_config = load_config("config/minio_config.yaml")["minio"]
            client = Minio(
                minio_config["endpoint"],
                access_key=minio_config["access_key"],
                secret_key=minio_config["secret_key"],
                secure=minio_config.get("secure", False)
            )
            
            bronze_bucket = minio_config["buckets"]["bronze"]
            if not client.bucket_exists(bronze_bucket):
                print(f"\n⚠️  Bronze bucket doesn't exist. Run: python main.py setup")
                results["ingestion"] = False
            else:
                results["ingestion"] = test_bronze_ingestor()
        except Exception as e:
            print(f"\n⚠️  Skipping ingestion test: {e}")
            results["ingestion"] = False
    else:
        print(f"\n⚠️  Skipping ingestion test (MinIO not connected)")
        results["ingestion"] = None
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    for test_name, result in results.items():
        if result is None:
            status = "⏭️  SKIPPED"
        elif result:
            status = "✅ PASSED"
        else:
            status = "❌ FAILED"
        
        print(f"{status:12} - {test_name.upper()}")
    
    # Overall result
    passed = sum(1 for r in results.values() if r is True)
    failed = sum(1 for r in results.values() if r is False)
    skipped = sum(1 for r in results.values() if r is None)
    
    print("\n" + "="*70)
    print(f"Results: {passed} passed, {failed} failed, {skipped} skipped")
    print("="*70)
    
    if failed == 0:
        print("\n🎉 All tests passed! Your lakehouse is ready.")
        print("\nNext steps:")
        print("  1. Configure your data sources in config/db_config.yaml")
        print("  2. Configure ingestion jobs in config/ingestion_config.yaml")
        print("  3. Run: python main.py test")
        print("  4. Run: python main.py ingest")
    else:
        print("\n⚠️  Some tests failed. Please fix the issues above.")
        print("\nSee GETTING_STARTED.md for help")
    
    print("\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Tests interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n\n❌ Tests failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
