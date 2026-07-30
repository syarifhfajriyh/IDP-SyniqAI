"""Direct MinIO connection test"""
from minio import Minio
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)

print(f"Loading config from: {env_path}")
print(f"\nTesting MinIO connection...")
print(f"Endpoint: {os.getenv('MINIO_ENDPOINT', 'localhost:9000')}")
print(f"Access Key: {os.getenv('MINIO_ACCESS_KEY', 'admin')}")

try:
    client = Minio(
        os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "admin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "password123"),
        secure=False
    )
    
    # List buckets
    buckets = list(client.list_buckets())
    print(f"\n✓ MinIO Connected Successfully!")
    print(f"✓ Found {len(buckets)} buckets:")
    for bucket in buckets:
        print(f"  - {bucket.name}")
    
    # Create required buckets if they don't exist
    required_buckets = ["syniqai-bronze", "syniqai-silver", "syniqai-gold", "syniqai-quarantine"]
    for bucket_name in required_buckets:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"✓ Created bucket: {bucket_name}")
        else:
            print(f"✓ Bucket exists: {bucket_name}")
    
    print("\n✓ MinIO is ready!")
    
except Exception as e:
    print(f"\n✗ MinIO connection failed: {e}")
    print("\nTroubleshooting:")
    print("1. Make sure MinIO Docker container is running")
    print("2. Verify credentials in .env match your Docker command")
    print("3. Check port 9000 is accessible")
