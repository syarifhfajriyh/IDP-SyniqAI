"""Test SQL execute endpoint directly"""
import requests
import json

# Test data matching what frontend sends
payload = {
    "query": "SELECT * FROM bronze.finance_transactions LIMIT 10",
    "limit": 100,
    "tables": [
        {
            "name": "bronze.finance_transactions",
            "path": "s3://syniqai-bronze/finance/postgres/finance_transactions/*.parquet"
        }
    ]
}

print("Testing SQL execute endpoint...")
print(f"Payload: {json.dumps(payload, indent=2)}")
print()

try:
    response = requests.post(
        'http://localhost:8000/api/sql/execute',
        json=payload,
        headers={'Content-Type': 'application/json'},
        timeout=30
    )
    
    print(f"Status Code: {response.status_code}")
    print(f"Response Headers: {dict(response.headers)}")
    print(f"Response Text: {response.text[:500]}")
    
    if response.text:
        print(f"Response JSON: {json.dumps(response.json(), indent=2)}")
    else:
        print("❌ Empty response body")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
