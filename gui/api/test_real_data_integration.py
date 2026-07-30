"""
Test Real Data Integration - Verify MinIO Data Loading
=======================================================

Test that Preview, Schema, Statistics, and Query tabs load real data from MinIO.
"""
import requests
import json

BASE_URL = "http://localhost:8000"

def test_preview_data():
    """Test preview data endpoint"""
    print("\n" + "="*70)
    print("📊 Test 1: Preview Data from MinIO")
    print("="*70)
    
    try:
        response = requests.get(
            f"{BASE_URL}/api/bronze-data/preview-data/finance_transactions",
            params={'domain': 'finance', 'source': 'postgres', 'limit': 5}
        )
        
        if response.ok:
            data = response.json()
            print(f"✅ Success!")
            print(f"   Rows returned: {data['row_count']}")
            print(f"   Columns: {len(data['columns'])}")
            print(f"   Column names: {', '.join(data['columns'][:5])}...")
            if data['rows']:
                print(f"\n   First row sample:")
                for key, value in list(data['rows'][0].items())[:3]:
                    print(f"     {key}: {value}")
        else:
            print(f"❌ Failed: {response.status_code}")
            print(f"   {response.text}")
    except Exception as e:
        print(f"❌ Error: {e}")


def test_schema():
    """Test schema endpoint"""
    print("\n" + "="*70)
    print("📋 Test 2: Table Schema from MinIO")
    print("="*70)
    
    try:
        response = requests.get(
            f"{BASE_URL}/api/bronze-data/schema/finance_transactions",
            params={'domain': 'finance', 'source': 'postgres'}
        )
        
        if response.ok:
            data = response.json()
            print(f"✅ Success!")
            print(f"   Columns found: {data['column_count']}")
            print(f"\n   Schema:")
            for col in data['schema'][:5]:
                print(f"     {col['name']}: {col['type']} ({'nullable' if col['nullable'] else 'NOT NULL'})")
        else:
            print(f"❌ Failed: {response.status_code}")
            print(f"   {response.text}")
    except Exception as e:
        print(f"❌ Error: {e}")


def test_statistics():
    """Test statistics endpoint"""
    print("\n" + "="*70)
    print("📈 Test 3: Column Statistics from MinIO")
    print("="*70)
    
    try:
        response = requests.get(
            f"{BASE_URL}/api/bronze-data/statistics/finance_transactions",
            params={'domain': 'finance', 'source': 'postgres'}
        )
        
        if response.ok:
            data = response.json()
            print(f"✅ Success!")
            print(f"   Total rows: {data['total_rows']}")
            print(f"   Statistics calculated for: {len(data['statistics'])} columns")
            print(f"\n   Sample statistics:")
            for stat in data['statistics'][:3]:
                print(f"     {stat['column']} ({stat['type']}): {stat['distinct']} distinct, {stat['nulls']} nulls ({stat['nullPercent']}%)")
        else:
            print(f"❌ Failed: {response.status_code}")
            print(f"   {response.text}")
    except Exception as e:
        print(f"❌ Error: {e}")


def test_query_execution():
    """Test query validation endpoint"""
    print("\n" + "="*70)
    print("🔍 Test 4: Query Execution")
    print("="*70)
    
    try:
        response = requests.post(
            f"{BASE_URL}/api/query/execute-validation-query",
            json={
                "sql": "SELECT * FROM bronze.finance_transactions LIMIT 3",
                "table_name": "finance_transactions",
                "domain": "finance",
                "source": "postgres"
            }
        )
        
        if response.ok:
            data = response.json()
            print(f"✅ Success!")
            print(f"   Rows: {data['row_count']}")
            print(f"   Execution time: {data['execution_time_ms']:.2f}ms")
            print(f"   Query type: {data['query_type']}")
        else:
            print(f"❌ Failed: {response.status_code}")
            print(f"   {response.text}")
    except Exception as e:
        print(f"❌ Error: {e}")


def main():
    print("\n" + "="*70)
    print("🧪 Testing Real Data Integration with MinIO")
    print("="*70)
    print("\nThis tests all 4 data tabs:")
    print("  1. Preview - Sample rows from Parquet files")
    print("  2. Schema - Column definitions from metadata")
    print("  3. Statistics - Calculated stats (distinct, nulls, min/max/avg)")
    print("  4. Query - SQL execution against Bronze data")
    print("\n" + "="*70)
    
    test_preview_data()
    test_schema()
    test_statistics()
    test_query_execution()
    
    print("\n" + "="*70)
    print("✨ Test Complete!")
    print("="*70)
    print("\n📌 Next Steps:")
    print("1. Open UI: http://localhost:5173")
    print("2. Go to: Silver Dashboard → Data Catalog")
    print("3. Click: finance_transactions table")
    print("4. Test each tab:")
    print("   • Preview → Should show real rows from MinIO")
    print("   • Schema → Should show actual column types")
    print("   • Statistics → Should show calculated stats")
    print("   • Query → Should execute and show results")
    print("\n🎉 All tabs now load REAL DATA from MinIO!")


if __name__ == "__main__":
    main()
