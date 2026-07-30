"""Test Transformation Pipeline Executor"""

import requests
import json

# Test payload: Simple remove_nulls transformation
payload = {
    "source_table": "finance.finance_transactions",
    "target_table": "finance_transactions_cleaned",
    "transformations": [
        {
            "id": "t1",
            "name": "Remove Nulls",
            "operation": "remove_nulls",
            "enabled": True,
            "params": {
                "columns": ["amount", "user_id"]
            }
        }
    ],
    "output_config": {
        "format": "Parquet",
        "s3Path": "s3://syniqai-silver/finance/finance_transactions_cleaned/"
    }
}

try:
    print("🧪 Testing transformation endpoint...")
    print(f"📝 Payload: {json.dumps(payload, indent=2)}")
    
    response = requests.post(
        "http://localhost:8000/api/silver/execute-transformation",
        json=payload,
        headers={"Content-Type": "application/json"}
    )
    
    print(f"\n📡 Response Status: {response.status_code}")
    print(f"📄 Response Body:\n{json.dumps(response.json(), indent=2)}")
    
    if response.status_code == 200:
        print("\n✅ Transformation executed successfully!")
    else:
        print(f"\n❌ Transformation failed with status {response.status_code}")
        
except Exception as e:
    print(f"\n❌ Error: {e}")
