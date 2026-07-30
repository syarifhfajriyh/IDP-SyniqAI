"""
Setup MinIO Domain Structure
Creates placeholder files to initialize the folder structure for each domain
"""
from minio import Minio
from minio.error import S3Error
import io
import json
from datetime import datetime

# MinIO Configuration
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"

def setup_minio_domains():
    """Create domain folder structure in MinIO buckets"""
    
    # Initialize MinIO client
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    buckets = ['syniqai-bronze', 'syniqai-silver', 'syniqai-gold']
    domains = ['finance', 'healthcare', 'general']
    
    print("Setting up MinIO domain structure...")
    print("=" * 60)
    
    for bucket in buckets:
        # Ensure bucket exists
        try:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                print(f"✓ Created bucket: {bucket}")
            else:
                print(f"✓ Bucket exists: {bucket}")
        except S3Error as e:
            print(f"✗ Error with bucket {bucket}: {e}")
            continue
        
        # Create domain folders with placeholder files
        for domain in domains:
            try:
                # Create a .domain_info file in each domain folder
                info = {
                    "domain": domain,
                    "layer": bucket.replace('syniqai-', ''),
                    "created_at": datetime.now().isoformat(),
                    "description": f"{domain.capitalize()} domain {bucket.replace('syniqai-', '')} layer"
                }
                
                info_json = json.dumps(info, indent=2)
                data = io.BytesIO(info_json.encode('utf-8'))
                
                object_path = f"{domain}/.domain_info"
                
                client.put_object(
                    bucket,
                    object_path,
                    data,
                    len(info_json),
                    content_type='application/json'
                )
                
                print(f"  ✓ Created {bucket}/{domain}/")
                
            except S3Error as e:
                print(f"  ✗ Error creating {bucket}/{domain}: {e}")
    
    print("=" * 60)
    print("✓ MinIO domain structure setup complete!")
    print("\nStructure created:")
    print("  syniqai-bronze/")
    print("    ├── finance/")
    print("    ├── healthcare/")
    print("    └── general/")
    print("  syniqai-silver/")
    print("    ├── finance/")
    print("    ├── healthcare/")
    print("    └── general/")
    print("  syniqai-gold/")
    print("    ├── finance/")
    print("    ├── healthcare/")
    print("    └── general/")
    print("\nYou can now start ingesting data through the UI!")

if __name__ == "__main__":
    try:
        setup_minio_domains()
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
