"""
MinIO Setup and Verification Script

This script helps you:
1. Test MinIO connection
2. Create required buckets (bronze, silver, gold)
3. Verify bucket accessibility

Usage:
    python setup_minio.py
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from minio import Minio
from minio.error import S3Error
from utils import load_config, get_logger

logger = get_logger(__name__)


def test_minio_connection(config: dict) -> bool:
    """Test connection to MinIO."""
    try:
        logger.info("Testing MinIO connection...")
        logger.info(f"  Endpoint: {config['endpoint']}")
        
        client = Minio(
            config["endpoint"],
            access_key=config["access_key"],
            secret_key=config["secret_key"],
            secure=config.get("secure", False)
        )
        
        # Try to list buckets
        buckets = list(client.list_buckets())
        
        logger.info(f"✅ Connected successfully!")
        logger.info(f"   Existing buckets: {len(buckets)}")
        
        for bucket in buckets:
            logger.info(f"     - {bucket.name}")
        
        return True
    
    except S3Error as e:
        logger.error(f"❌ MinIO S3 Error: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Connection failed: {e}")
        return False


def create_buckets(config: dict, buckets: dict) -> dict:
    """Create MinIO buckets if they don't exist."""
    results = {}
    
    try:
        logger.info("\nCreating MinIO buckets...")
        
        client = Minio(
            config["endpoint"],
            access_key=config["access_key"],
            secret_key=config["secret_key"],
            secure=config.get("secure", False)
        )
        
        for bucket_type, bucket_name in buckets.items():
            try:
                if client.bucket_exists(bucket_name):
                    logger.info(f"  ✅ {bucket_name:15} (already exists)")
                    results[bucket_name] = "exists"
                else:
                    client.make_bucket(bucket_name)
                    logger.info(f"  ✅ {bucket_name:15} (created)")
                    results[bucket_name] = "created"
            
            except S3Error as e:
                logger.error(f"  ❌ {bucket_name:15} (failed: {e})")
                results[bucket_name] = f"failed: {e}"
            except Exception as e:
                logger.error(f"  ❌ {bucket_name:15} (error: {e})")
                results[bucket_name] = f"error: {e}"
        
        return results
    
    except Exception as e:
        logger.error(f"Failed to create buckets: {e}")
        return {}


def verify_bucket_access(config: dict, bucket_name: str) -> bool:
    """Verify read/write access to a bucket."""
    try:
        import io
        from datetime import datetime
        
        client = Minio(
            config["endpoint"],
            access_key=config["access_key"],
            secret_key=config["secret_key"],
            secure=config.get("secure", False)
        )
        
        # Create test object
        test_data = f"Test write at {datetime.now()}".encode('utf-8')
        test_object = "_test/access_test.txt"
        
        # Write test
        client.put_object(
            bucket_name,
            test_object,
            io.BytesIO(test_data),
            len(test_data)
        )
        
        # Read test
        response = client.get_object(bucket_name, test_object)
        read_data = response.read()
        response.close()
        response.release_conn()
        
        # Cleanup
        client.remove_object(bucket_name, test_object)
        
        return read_data == test_data
    
    except Exception as e:
        logger.error(f"Access verification failed: {e}")
        return False


def main():
    """Main setup function."""
    logger.info("=" * 70)
    logger.info("SYNIQ DATA LAKEHOUSE - MINIO SETUP")
    logger.info("=" * 70)
    
    # Load MinIO configuration
    config_path = Path(__file__).parent / "config" / "minio_config.yaml"
    
    if not config_path.exists():
        logger.error(f"Configuration file not found: {config_path}")
        sys.exit(1)
    
    try:
        config = load_config(config_path)
        minio_config = config["minio"]
        buckets = minio_config["buckets"]
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        sys.exit(1)
    
    # Step 1: Test connection
    logger.info("\n" + "=" * 70)
    logger.info("STEP 1: Test MinIO Connection")
    logger.info("=" * 70)
    
    if not test_minio_connection(minio_config):
        logger.error("\n❌ MinIO connection test failed!")
        logger.info("\nPlease ensure:")
        logger.info("  1. MinIO is running (docker ps)")
        logger.info("  2. Endpoint is correct in config/minio_config.yaml")
        logger.info("  3. Credentials are correct")
        sys.exit(1)
    
    # Step 2: Create buckets
    logger.info("\n" + "=" * 70)
    logger.info("STEP 2: Create Required Buckets")
    logger.info("=" * 70)
    
    results = create_buckets(minio_config, buckets)
    
    if not results:
        logger.error("\n❌ Failed to create buckets!")
        sys.exit(1)
    
    # Step 3: Verify access
    logger.info("\n" + "=" * 70)
    logger.info("STEP 3: Verify Bucket Access")
    logger.info("=" * 70)
    
    all_accessible = True
    
    for bucket_type, bucket_name in buckets.items():
        if results.get(bucket_name) in ["exists", "created"]:
            logger.info(f"\nVerifying {bucket_name}...")
            if verify_bucket_access(minio_config, bucket_name):
                logger.info(f"  ✅ Read/Write access confirmed")
            else:
                logger.error(f"  ❌ Access verification failed")
                all_accessible = False
    
    # Final summary
    logger.info("\n" + "=" * 70)
    logger.info("SETUP SUMMARY")
    logger.info("=" * 70)
    
    logger.info("\n📦 Buckets:")
    for bucket_type, bucket_name in buckets.items():
        status = results.get(bucket_name, "unknown")
        icon = "✅" if status in ["exists", "created"] else "❌"
        logger.info(f"  {icon} {bucket_name:15} ({status})")
    
    logger.info(f"\n🔐 Access:")
    if all_accessible:
        logger.info(f"  ✅ All buckets are accessible")
    else:
        logger.info(f"  ⚠️  Some buckets have access issues")
    
    logger.info("\n" + "=" * 70)
    
    if all_accessible:
        logger.info("✅ MinIO setup completed successfully!")
        logger.info("\nYou can now run:")
        logger.info("  python main.py test      # Test all connections")
        logger.info("  python main.py ingest    # Run data ingestion")
        sys.exit(0)
    else:
        logger.error("❌ Setup completed with errors")
        sys.exit(1)


if __name__ == "__main__":
    main()
