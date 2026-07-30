#!/usr/bin/env python3
"""
Check MinIO bucket and create if needed
"""
from minio import Minio
from minio.error import S3Error

# MinIO configuration
minio_client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="password123",
    secure=False
)

bucket_name = "bronze"

try:
    # Check if bucket exists
    if minio_client.bucket_exists(bucket_name):
        print(f"✓ Bucket '{bucket_name}' exists")
        
        # List objects in bucket
        objects = list(minio_client.list_objects(bucket_name, recursive=True))
        print(f"✓ Objects in bucket: {len(objects)}")
        
        if objects:
            print("\n📁 Recent objects:")
            for obj in objects[-5:]:
                print(f"   - {obj.object_name} ({obj.size} bytes)")
    else:
        print(f"✗ Bucket '{bucket_name}' does not exist")
        print(f"\n Creating bucket...")
        minio_client.make_bucket(bucket_name)
        print(f"✓ Bucket created: {bucket_name}")
        
        # Set public policy for testing
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                    "Resource": [f"arn:aws:s3:::{bucket_name}/*"]
                }
            ]
        }
        import json
        minio_client.set_bucket_policy(bucket_name, json.dumps(policy))
        print(f"✓ Bucket policy set")
        
except S3Error as e:
    print(f"✗ MinIO Error: {e}")
except Exception as e:
    print(f"✗ Error: {e}")
