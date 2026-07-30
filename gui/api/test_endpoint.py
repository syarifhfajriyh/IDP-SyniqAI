"""Test the backend endpoint function directly"""
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent))

# Import the backend module
print("Importing backend...")
from backend import app
print("✓ Backend imported")

# Import storage to initialize
import storage
print("\nInitializing storage...")
storage.initialize_storage()
print("✓ Storage initialized")
print(f"✓ storage.bronze_manager: {type(storage.bronze_manager)}\n")

# Try calling the endpoint function directly
print("=== Testing get_bronze_table_details ===")
try:
    from fastapi.testclient import TestClient
    client = TestClient(app)
    
    print("Making request to /api/bronze/table/finance/transactions...")
    response = client.get("/api/bronze/table/finance/transactions")
    
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"✓ Success!")
        print(f"  Domain: {data['domain']}")
        print(f"  Table: {data['table_name']}")
        print(f"  Total rows: {data['total_rows']}")
        print(f"  Schema columns: {len(data['schema']['columns'])}")
    else:
        print(f"✗ Error: {response.text}")
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
