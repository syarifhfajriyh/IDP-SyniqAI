"""Test SQL Preview API endpoint"""
import requests
import json

API_BASE = "http://localhost:8000"

def test_database_tables():
    """Test listing available tables"""
    print("\n" + "="*60)
    print("TEST 1: List Available Tables")
    print("="*60)
    
    try:
        response = requests.get(f"{API_BASE}/api/silver/database-tables", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print(f"\n✅ Success!")
            print(f"   Storage: {data['storage']}")
            print(f"   Count: {data['count']} table(s)")
            print(f"\n   Tables:")
            for table in data['tables']:
                print(f"   • {table['name']} ({table['layer']} - {table['source']}/{table['entity']})")
            return data['tables']
        else:
            print(f"\n❌ Error {response.status_code}: {response.text}")
            return []
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return []

def test_sql_preview(sql_query, source="finance", entity="finance_transactions"):
    """Test executing SQL query"""
    print("\n" + "="*60)
    print(f"TEST 2: Execute SQL Query")
    print("="*60)
    print(f"\nSQL: {sql_query}")
    
    try:
        payload = {
            "sql": sql_query,
            "source": source,
            "entity": entity
        }
        
        response = requests.post(
            f"{API_BASE}/api/silver/sql-preview",
            json=payload,
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"\n✅ Success!")
            print(f"   Execution Time: {data['executionTime']}")
            print(f"   Rows Returned: {data['rowCount']}")
            print(f"   Columns: {', '.join(data['columns'])}")
            
            if data['rowCount'] > 0:
                print(f"\n   Sample data (first row):")
                for col, val in zip(data['columns'], data['rows'][0]):
                    print(f"   • {col}: {val}")
            
            if 'tablesQueried' in data:
                print(f"\n   Tables Queried: {', '.join(data['tablesQueried'])}")
            
            return True
        else:
            print(f"\n❌ Error {response.status_code}:")
            print(f"   {response.json().get('detail', response.text)}")
            return False
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return False

def main():
    print("\n" + "="*60)
    print("SQL PREVIEW API TEST")
    print("="*60)
    
    # Test 1: List tables
    tables = test_database_tables()
    
    if not tables:
        print("\n⚠️  No tables found. Please ingest data first.")
        return
    
    # Get first table name
    table_name = tables[0]['name']
    
    # Test 2: Count query
    test_sql_preview(f"SELECT COUNT(*) as total_rows FROM {table_name}")
    
    # Test 3: Sample data
    test_sql_preview(f"SELECT * FROM {table_name} LIMIT 5")
    
    # Test 4: Aggregation query
    if 'transaction' in table_name.lower():
        test_sql_preview(f"""
            SELECT 
                COUNT(*) as total_transactions,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount
            FROM {table_name}
        """)
    
    print("\n" + "="*60)
    print("✅ API TESTS COMPLETE")
    print("="*60)
    print("\nYou can now use SQL Preview in the UI!")
    print("1. Go to Silver layer")
    print("2. Find the SQL Editor section")
    print("3. Type your SQL query")
    print("4. Click 'Run Preview'")

if __name__ == "__main__":
    main()
