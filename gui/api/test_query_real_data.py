"""Test Query Validation with Real MinIO Data"""
import requests
import json

# Test query execution
print("Testing Query Validation Endpoint with Real Data...")
print("=" * 60)

query_request = {
    "sql": "SELECT * FROM bronze.finance_transactions LIMIT 5",
    "table_name": "finance_transactions",
    "domain": "finance",
    "source": "postgres",
    "limit": 5
}

try:
    response = requests.post(
        'http://localhost:8000/api/query/execute-validation-query',
        json=query_request,
        timeout=10
    )
    
    print(f"\nStatus Code: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"✓ Success: {data.get('success')}")
        print(f"✓ Row Count: {data.get('row_count')}")
        print(f"✓ Execution Time: {data.get('execution_time_ms', 0):.2f}ms")
        print(f"✓ Query Type: {data.get('query_type')}")
        print(f"\nColumns: {data.get('columns', [])}")
        
        print("\n--- First Row ---")
        if data.get('rows'):
            first_row = data['rows'][0]
            for key, value in first_row.items():
                print(f"  {key}: {value}")
                
            # Check if this is real data (not sample)
            if 'merchant' in first_row and first_row.get('transaction_id', '').startswith('tx'):
                print("\n⚠ WARNING: Still returning SAMPLE DATA (has 'merchant' field and tx00X IDs)")
                print("Expected real data from MinIO!")
            else:
                print("\n✓ SUCCESS: Returning REAL DATA from MinIO!")
        else:
            print("No rows returned")
    else:
        print(f"✗ Error: {response.text}")
        
except requests.exceptions.ConnectionError:
    print("✗ ERROR: Cannot connect to backend at http://localhost:8000")
    print("  Make sure the backend server is running!")
except Exception as e:
    print(f"✗ ERROR: {e}")
