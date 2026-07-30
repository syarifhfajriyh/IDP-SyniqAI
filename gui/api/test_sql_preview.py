"""Test SQL Preview with DuckDB on MinIO Parquet files"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from minio_utils import MinIOClient
import duckdb
import tempfile
import os
import shutil

def test_sql_preview():
    print("\n" + "="*60)
    print("SQL PREVIEW TEST - DuckDB + MinIO")
    print("="*60)
    
    try:
        # 1. Connect to MinIO
        print("\n1️⃣  Connecting to MinIO...")
        minio_client = MinIOClient()
        print("   ✅ Connected")
        
        # 2. List available tables
        print("\n2️⃣  Discovering tables...")
        bronze_tables = minio_client.list_tables('bronze')
        silver_tables = minio_client.list_tables('silver')
        
        all_tables = {}
        for table in bronze_tables:
            table_name = table['entity']
            all_tables[table_name] = {
                'layer': 'bronze',
                'source': table['source'],
                'entity': table['entity'],
                'bucket': 'syniqai-bronze',
                'object_path': f"{table['source']}/{table['entity']}/data.parquet"
            }
        
        for table in silver_tables:
            table_name = f"{table['entity']}_silver"
            all_tables[table_name] = {
                'layer': 'silver',
                'source': table['source'],
                'entity': table['entity'],
                'bucket': 'syniqai-silver',
                'object_path': f"{table['source']}/{table['entity']}/data.parquet"
            }
        
        if not all_tables:
            print("   ⚠️  No tables found!")
            return False
        
        print(f"   ✅ Found {len(all_tables)} table(s):")
        for name, info in all_tables.items():
            print(f"      • {name} ({info['layer']} - {info['source']}/{info['entity']})")
        
        # 3. Download Parquet files to temp directory
        print("\n3️⃣  Creating temporary workspace...")
        temp_dir = tempfile.mkdtemp(prefix="sql_preview_test_")
        print(f"   ✅ Created: {temp_dir}")
        
        try:
            # 4. Load tables into DuckDB
            print("\n4️⃣  Loading tables into DuckDB...")
            conn = duckdb.connect(':memory:')
            loaded_tables = []
            
            for table_name, info in all_tables.items():
                try:
                    # Download Parquet file
                    temp_file = os.path.join(temp_dir, f"{table_name}.parquet")
                    minio_client.client.fget_object(
                        info['bucket'],
                        info['object_path'],
                        temp_file
                    )
                    
                    # Create DuckDB view
                    conn.execute(f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{temp_file}')")
                    print(f"   ✅ Loaded: {table_name}")
                    loaded_tables.append(table_name)
                    
                except Exception as e:
                    print(f"   ⚠️  Skipped {table_name}: {str(e)[:80]}...")
            
            if not loaded_tables:
                print("\n   ❌ No tables could be loaded!")
                return False
            
            # 5. Run test queries
            print("\n5️⃣  Running test queries...")
            
            # Get first successfully loaded table
            first_table = loaded_tables[0]
            
            # Query 1: Row count
            print(f"\n   Query 1: SELECT COUNT(*) FROM {first_table}")
            result = conn.execute(f"SELECT COUNT(*) as row_count FROM {first_table}").fetchall()
            print(f"   ✅ Result: {result[0][0]} rows")
            
            # Query 2: Column info
            print(f"\n   Query 2: DESCRIBE {first_table}")
            result = conn.execute(f"DESCRIBE {first_table}").fetchall()
            print(f"   ✅ Columns:")
            for row in result[:5]:  # Show first 5 columns
                print(f"      • {row[0]}: {row[1]}")
            if len(result) > 5:
                print(f"      ... and {len(result) - 5} more columns")
            
            # Query 3: Sample data
            print(f"\n   Query 3: SELECT * FROM {first_table} LIMIT 3")
            result = conn.execute(f"SELECT * FROM {first_table} LIMIT 3").fetchall()
            columns = [desc[0] for desc in conn.description]
            print(f"   ✅ Sample data (showing first 3 columns):")
            for i, row in enumerate(result, 1):
                print(f"      Row {i}: {dict(list(zip(columns[:3], row[:3])))}")
            
            conn.close()
            print("\n" + "="*60)
            print("✅ ALL TESTS PASSED!")
            print("="*60)
            print("\nSQL Preview is ready to use!")
            print("You can now query Bronze and Silver tables in the UI.")
            return True
            
        finally:
            # Cleanup temp directory
            shutil.rmtree(temp_dir, ignore_errors=True)
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_sql_preview()
    sys.exit(0 if success else 1)
