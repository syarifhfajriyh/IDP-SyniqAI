"""Test Transformation with Fixed Write Path"""

import requests
import json
import boto3
from botocore.client import Config

# Test payload: Simple remove_nulls transformation
payload = {
    "source_table": "finance.finance_transactions",
    "target_table": "finance_transactions_test",
    "transformations": [
        {
            "id": "t1",
            "name": "Remove Nulls",
            "operation": "remove_nulls",
            "enabled": True,
            "params": {
                "columns": ["amount"]
            }
        }
    ],
    "output_config": {
        "format": "Parquet",
        "s3Path": "s3://syniqai-silver/finance/test_output/"
    }
}

try:
    print("🧪 Testing transformation with new write logic...\n")
    
    response = requests.post(
        "http://localhost:8000/api/silver/execute-transformation",
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=30
    )
    
    print(f"📡 Response Status: {response.status_code}")
    result = response.json()
    print(f"📄 Response:\n{json.dumps(result, indent=2)}\n")
    
    if response.status_code == 200:
        print("✅ Transformation completed successfully!")
        
        # Check MinIO for the file
        print("\n🔍 Checking MinIO for output file...")
        s3 = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='admin',
            aws_secret_access_key='password123',
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        
        # List files in test_output
        response = s3.list_objects_v2(
            Bucket='syniqai-silver',
            Prefix='finance/test_output'
        )
        
        if 'Contents' in response:
            print(f"\n📁 Files created:")
            for obj in response['Contents']:
                size = obj['Size']
                size_kb = size / 1024
                print(f"   📄 {obj['Key']}")
                print(f"      Size: {size:,} bytes ({size_kb:.1f} KB)")
                if size > 0:
                    print(f"      ✅ File has data!")
                else:
                    print(f"      ❌ File is empty!")
        else:
            print("\n❌ No files found in output location")
    else:
        print(f"\n❌ Transformation failed: {result}")
        
except Exception as e:
    print(f"\n❌ Error: {e}")
