"""
Quick test to verify SQL endpoint receives tables parameter correctly
"""
import requests
import json

# Test 1: Check if /api/sql/tables endpoint works
print("=" * 70)
print("TEST 1: Fetching available tables")
print("=" * 70)

try:
    response = requests.get("http://localhost:8000/api/sql/tables")
    data = response.json()
    
    if data.get("success"):
        print(f"✅ Success! Found {len(data['tables'])} tables")
        for table in data['tables']:
            print(f"   - {table['name']}")
            print(f"     Path: {table['path']}")
            print(f"     Rows: {table.get('row_count', 'unknown'):,}")
    else:
        print(f"❌ Failed: {data}")
except Exception as e:
    print(f"❌ Error: {e}")

# Test 2: Execute query WITH tables parameter
print("\n" + "=" * 70)
print("TEST 2: Execute query WITH tables parameter")
print("=" * 70)

if data.get("success") and data['tables']:
    query_request = {
        "query": "SELECT * FROM bronze.finance_transactions LIMIT 5",
        "limit": 5,
        "tables": [
            {
                "name": table['name'],
                "path": table['path']
            }
            for table in data['tables']
        ]
    }
    
    print(f"Sending request with {len(query_request['tables'])} tables...")
    
    try:
        response = requests.post(
            "http://localhost:8000/api/sql/execute",
            json=query_request,
            headers={"Content-Type": "application/json"}
        )
        result = response.json()
        
        if result.get("success"):
            print(f"✅ Query executed successfully!")
            print(f"   Rows returned: {len(result['rows'])}")
            print(f"   Columns: {', '.join(result['columns'])}")
            print(f"   Execution time: {result['execution_time_ms']}ms")
        else:
            print(f"❌ Query failed: {result.get('error', 'Unknown error')}")
    except Exception as e:
        print(f"❌ Error: {e}")
else:
    print("⏭️  Skipping test 2 - no tables available")

# Test 3: Execute query WITHOUT tables parameter (should use auto-discovery)
print("\n" + "=" * 70)
print("TEST 3: Execute query WITHOUT tables parameter (auto-discovery)")
print("=" * 70)

query_request_no_tables = {
    "query": "SELECT * FROM bronze.finance_transactions LIMIT 5",
    "limit": 5
}

try:
    response = requests.post(
        "http://localhost:8000/api/sql/execute",
        json=query_request_no_tables,
        headers={"Content-Type": "application/json"}
    )
    result = response.json()
    
    if result.get("success"):
        print(f"✅ Query executed successfully with auto-discovery!")
        print(f"   Rows returned: {len(result['rows'])}")
    else:
        print(f"⚠️  Expected failure with auto-discovery (no tables specified)")
        print(f"   Error: {result.get('error', 'Unknown')[:100]}")
except Exception as e:
    print(f"❌ Error: {e}")

print("\n" + "=" * 70)
print("DIAGNOSIS COMPLETE")
print("=" * 70)
