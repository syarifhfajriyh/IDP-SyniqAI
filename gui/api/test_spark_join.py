"""Test Spark JOIN Operations"""

import requests
import json

# Test payload with JOIN operation
payload = {
    "source_table": "finance.finance_transactions",
    "target_table": "finance_transactions_joined",
    "transformations": [
        {
            "id": "t1",
            "name": "Step 1: Inner Join with finance_transactions",
            "operation": "join_inner",
            "enabled": True,
            "params": {
                "right_table": "finance_transactions",
                "on": "user_id"  # Self-join example
            }
        },
        {
            "id": "t2",
            "name": "Step 2: Remove Nulls",
            "operation": "remove_nulls",
            "enabled": True,
            "params": {
                "columns": ["amount"]
            }
        }
    ],
    "output_config": {
        "format": "Parquet",
        "s3Path": "s3://syniqai-silver/finance/test_join_output/"
    }
}

try:
    print("🧪 Testing Spark JOIN operation...\n")
    print(f"📝 Test Setup:")
    print(f"   • Source: {payload['source_table']}")
    print(f"   • Target: {payload['target_table']}")
    print(f"   • Transformations: {len(payload['transformations'])}")
    print(f"   • Join Type: {payload['transformations'][0]['operation']}")
    print(f"   • Join On: {payload['transformations'][0]['params']['on']}")
    print(f"\n{'='*70}\n")
    
    response = requests.post(
        "http://localhost:8000/api/silver/execute-transformation",
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=120  # 2 minutes for Spark operations
    )
    
    print(f"📡 Response Status: {response.status_code}\n")
    
    if response.status_code == 200:
        result = response.json()
        print("✅ JOIN OPERATION SUCCESSFUL!\n")
        print("📊 Results:")
        print(f"   Input Rows:  {result['result']['input_rows']:,}")
        print(f"   Output Rows: {result['result']['output_rows']:,}")
        print(f"   Duration:    {result['result']['duration_seconds']:.2f}s")
        print(f"   Throughput:  {result['result']['rows_per_second']:,} rows/s")
        print(f"   Format:      {result['result']['output_format']}")
        print(f"   Output Path: {result['result']['output_path']}")
        
        print(f"\n{'='*70}")
        print("✅ ALL TESTS PASSED!")
        print("   • Spark session created successfully")
        print("   • JOIN operation completed")
        print("   • Data written to Silver layer")
        print(f"{'='*70}\n")
    else:
        print(f"❌ Test failed with status {response.status_code}")
        print(f"Response: {response.text}")
        
except requests.exceptions.Timeout:
    print("❌ Request timed out - Spark may be taking longer than expected")
    print("   Check backend logs for Spark session status")
except Exception as e:
    print(f"❌ Error: {e}")


# Additional test: List available tables for joins
print("\n" + "="*70)
print("📋 Available Tables in Bronze Layer:")
print("="*70)

try:
    import boto3
    from botocore.client import Config
    
    s3 = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='password123',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    response = s3.list_objects_v2(
        Bucket='syniqai-bronze',
        Prefix='finance/postgres/',
        Delimiter='/'
    )
    
    if 'CommonPrefixes' in response:
        tables = []
        for prefix in response['CommonPrefixes']:
            table_name = prefix['Prefix'].replace('finance/postgres/', '').rstrip('/')
            tables.append(table_name)
        
        print(f"\nFound {len(tables)} table(s):")
        for i, table in enumerate(sorted(tables), 1):
            print(f"   {i}. {table}")
        
        if len(tables) == 1:
            print("\n⚠️  Note: Only 1 table available for joins")
            print("   To test joins with different tables:")
            print("   1. Add more tables to Bronze layer via CDC")
            print("   2. Upload additional CSV/Parquet files")
            print("   3. Or use self-join (join table with itself)")
    else:
        print("\n❌ No tables found in Bronze layer")
        
except Exception as e:
    print(f"\n❌ Could not list Bronze tables: {e}")

print()
