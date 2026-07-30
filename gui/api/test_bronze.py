"""Test Bronze Manager functions"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Import modules
import storage

def main():
    print("Initializing storage...")
    if storage.initialize_storage():
        print("✓ Storage initialized")
    else:
        print("✗ Storage initialization failed")
        return
    
    print(f"\n✓ bronze_manager type: {type(storage.bronze_manager)}")
    print(f"✓ bronze_manager is None: {storage.bronze_manager is None}")
    
    # Test list_tables
    print("\n=== Testing list_tables ===")
    try:
        tables = storage.bronze_manager.list_tables("finance")
        print(f"✓ Found {len(tables)} tables")
        for table in tables:
            print(f"  - {table}")
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
    
    # Test get_table_schema
    print("\n=== Testing get_table_schema ===")
    try:
        print("  Calling get_table_schema('finance', 'transactions')...")
        schema = storage.bronze_manager.get_table_schema("finance", "transactions")
        if schema:
            print(f"✓ Schema retrieved")
            print(f"  Columns: {len(schema.get('columns', []))}")
            print(f"  Row count: {schema.get('row_count', 0)}")
            print(f"  First 3 columns:")
            for col in schema.get('columns', [])[:3]:
                print(f"    - {col.get('name')}: {col.get('dtype')}")
        else:
            print("✗ Schema is None")
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
    
    # Test read_table_sample
    print("\n=== Testing read_table_sample ===")
    try:
        df = storage.bronze_manager.read_table_sample("finance", "transactions", max_rows=5)
        if df is not None:
            print(f"✓ Sample data retrieved: {len(df)} rows")
            print(f"  Columns: {list(df.columns)[:5]}")
        else:
            print("✗ DataFrame is None")
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
