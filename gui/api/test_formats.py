"""Test Both Parquet and Iceberg Formats"""

import requests
import json
import time

def test_format(format_name, output_path_prefix):
    """Test transformation with specified format"""
    
    payload = {
        "source_table": "finance.finance_transactions",
        "target_table": f"test_{format_name.lower().replace(' ', '_')}",
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
            "format": format_name,
            "s3Path": f"s3://syniqai-silver/finance/{output_path_prefix}"
        }
    }
    
    print(f"\n{'='*70}")
    print(f"🧪 Testing Format: {format_name}")
    print(f"{'='*70}\n")
    
    try:
        response = requests.post(
            "http://localhost:8000/api/silver/execute-transformation",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        result = response.json()
        
        if response.status_code == 200 and result.get('success'):
            print(f"✅ SUCCESS!")
            print(f"\n📊 Results:")
            print(f"   Input Rows:  {result['result']['input_rows']:,}")
            print(f"   Output Rows: {result['result']['output_rows']:,}")
            print(f"   Duration:    {result['result']['duration_seconds']:.2f}s")
            print(f"   Format:      {result['result']['output_format']}")
            print(f"   Output Path: {result['result']['output_path']}")
            return True
        else:
            print(f"❌ FAILED!")
            print(f"   Status: {response.status_code}")
            print(f"   Response: {json.dumps(result, indent=2)}")
            return False
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

# Run tests
print("\n" + "="*70)
print("🚀 Testing Transformation Formats")
print("="*70)

# Test 1: Parquet
parquet_success = test_format("Parquet", "test_parquet")
time.sleep(1)

# Test 2: Iceberg
iceberg_success = test_format("Apache Iceberg", "test_iceberg")

# Summary
print("\n" + "="*70)
print("📋 Test Summary")
print("="*70)
print(f"   Parquet:        {'✅ PASS' if parquet_success else '❌ FAIL'}")
print(f"   Apache Iceberg: {'✅ PASS' if iceberg_success else '❌ FAIL'}")
print("="*70 + "\n")

if parquet_success and iceberg_success:
    print("🎉 All formats working correctly!")
elif parquet_success:
    print("⚠️  Parquet works, but Iceberg needs attention")
    print("   Check backend logs for Iceberg error details")
else:
    print("❌ Tests failed - check backend logs")
