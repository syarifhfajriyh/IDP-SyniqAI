"""Check MinIO Silver Bucket Contents"""
import boto3
from botocore.client import Config

# Initialize MinIO client
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='admin',
    aws_secret_access_key='password123',
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

try:
    print("🔍 Checking MinIO Silver bucket contents...\n")
    
    # List objects in silver bucket
    response = s3.list_objects_v2(
        Bucket='syniqai-silver',
        Prefix='finance/',
        Delimiter='/'
    )
    
    if 'Contents' in response:
        print(f"📁 Found {len(response['Contents'])} objects:\n")
        for obj in response['Contents']:
            size = obj['Size']
            print(f"  📄 {obj['Key']} - {size} bytes")
            if size == 0:
                print(f"     ⚠️  WARNING: File is empty!")
    else:
        print("❌ No objects found in syniqai-silver/finance/")
    
    if 'CommonPrefixes' in response:
        print(f"\n📂 Subdirectories:")
        for prefix in response['CommonPrefixes']:
            print(f"  {prefix['Prefix']}")
            
            # Check contents of subdirectory
            sub_response = s3.list_objects_v2(
                Bucket='syniqai-silver',
                Prefix=prefix['Prefix']
            )
            if 'Contents' in sub_response:
                for obj in sub_response['Contents']:
                    size = obj['Size']
                    print(f"    📄 {obj['Key']} - {size} bytes")
                    if size == 0:
                        print(f"       ⚠️  WARNING: File is empty!")
    
except Exception as e:
    print(f"❌ Error: {e}")
