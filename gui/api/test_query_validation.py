"""
Test Query Validation Interface - Quick Verification
=====================================================

Run this to verify the Query Validation feature is working.
"""
import requests
import json

BASE_URL = "http://localhost:8000"

def test_query_validation():
    """Test the query validation endpoint"""
    print("\n" + "="*70)
    print("🧪 Testing Query Validation Interface")
    print("="*70)
    
    # Test 1: Simple SELECT query
    print("\n📝 Test 1: Simple SELECT query")
    query_request = {
        "sql": "SELECT * FROM bronze.finance_transactions LIMIT 5",
        "table_name": "finance_transactions",
        "domain": "finance",
        "source": "postgres",
        "limit": 100
    }
    
    try:
        response = requests.post(
            f"{BASE_URL}/api/query/execute-validation-query",
            json=query_request
        )
        
        if response.ok:
            data = response.json()
            print(f"✅ Success!")
            print(f"   Rows returned: {data['row_count']}")
            print(f"   Execution time: {data['execution_time_ms']:.2f}ms")
            print(f"   Query type: {data['query_type']}")
            print(f"   Columns: {', '.join(data['columns'])}")
            
            if data['rows']:
                print(f"\n   First row sample:")
                for key, value in list(data['rows'][0].items())[:3]:
                    print(f"     {key}: {value}")
        else:
            print(f"❌ Failed: {response.status_code}")
            print(f"   {response.text}")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Test 2: Aggregation query
    print("\n📝 Test 2: Aggregation query")
    query_request = {
        "sql": "SELECT COUNT(*) as total_rows FROM bronze.finance_transactions",
        "table_name": "finance_transactions",
        "domain": "finance",
        "source": "postgres"
    }
    
    try:
        response = requests.post(
            f"{BASE_URL}/api/query/execute-validation-query",
            json=query_request
        )
        
        if response.ok:
            data = response.json()
            print(f"✅ Success!")
            print(f"   Query type: {data['query_type']}")
            print(f"   Result: {data['rows'][0] if data['rows'] else 'No rows'}")
        else:
            print(f"❌ Failed: {response.status_code}")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Test 3: Invalid query (should be blocked)
    print("\n📝 Test 3: Invalid query (should be rejected)")
    query_request = {
        "sql": "DROP TABLE finance_transactions",
        "table_name": "finance_transactions",
        "domain": "finance",
        "source": "postgres"
    }
    
    try:
        response = requests.post(
            f"{BASE_URL}/api/query/execute-validation-query",
            json=query_request
        )
        
        if response.status_code == 400:
            print(f"✅ Correctly blocked dangerous query")
            print(f"   Message: {response.json()['detail']}")
        else:
            print(f"⚠️  Unexpected response: {response.status_code}")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print("\n" + "="*70)
    print("✨ Test Complete!")
    print("="*70)
    print("\n📌 Next Steps:")
    print("1. Open UI: http://localhost:5173")
    print("2. Go to: Silver Dashboard → Data Catalog")
    print("3. Click any table → Query tab")
    print("4. You should see:")
    print("   • Step 1: Edit SQL with textarea")
    print("   • Step 2: Run Query button")
    print("   • Step 3: Results table")
    print("   • Quick query templates at bottom")
    print("\n5. Try running a query and see results appear!")

if __name__ == "__main__":
    test_query_validation()
