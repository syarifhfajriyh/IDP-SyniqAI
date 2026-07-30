"""Debug the Bronze table details endpoint"""
import sys
from pathlib import Path
import traceback
import json

sys.path.insert(0, str(Path(__file__).parent))

# Import storage first
import storage

print("Initializing storage...")
storage.initialize_storage()
print(f"Storage initialized. bronze_manager: {type(storage.bronze_manager)}\n")

# Now test the actual endpoint logic
print("=== Testing endpoint logic manually ===")
domain = "finance"
table_name = "transactions"

try:
    print(f"\n1. Getting schema for {domain}/{table_name}...")
    schema = storage.bronze_manager.get_table_schema(domain, table_name)
    if schema:
        print(f"   SUCCESS: Schema has {len(schema.get('columns', []))} columns")
    else:
        print("   ERROR: Schema is None")
    
    print(f"\n2. Getting sample data for {domain}/{table_name}...")
    sample_df = storage.bronze_manager.read_table_sample(domain, table_name, max_rows=100)
    if sample_df is not None:
        print(f"   SUCCESS: Sample has {len(sample_df)} rows")
        
        print(f"\n3. Converting to dict (like endpoint does)...")
        sample_data = sample_df.head(10).to_dict(orient="records")
        print(f"   SUCCESS: Converted {len(sample_data)} records")
        
        print(f"\n4. Building response dict...")
        response = {
            "domain": domain,
            "table_name": table_name,
            "schema": schema,
            "sample_data": sample_data,
            "total_rows": len(sample_df)
        }
        print(f"   SUCCESS: Response dict created")
        
        print(f"\n5. Converting to JSON (FastAPI does this)...")
        json_str = json.dumps(response, default=str)
        print(f"   SUCCESS: JSON serialization worked ({len(json_str)} bytes)")
        print(f"\n✓ All steps passed! The endpoint logic should work.")
        
    else:
        print("   ERROR: sample_df is None")
        
except Exception as e:
    print(f"\n✗ ERROR at some step:")
    print(f"   {type(e).__name__}: {e}")
    print("\nFull traceback:")
    traceback.print_exc()
