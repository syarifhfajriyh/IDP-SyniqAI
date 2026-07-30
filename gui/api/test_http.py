"""Test endpoint with proper error handling"""
import urllib.request
import json

try:
    with urllib.request.urlopen("http://localhost:8000/api/bronze/table/finance/transactions") as response:
        status_code = response.status
        print(f"Status Code: {status_code}")
        
        body = response.read().decode('utf-8')
        data = json.loads(body)
        
        print(f"\n✓ SUCCESS!")
        print(f"Domain: {data['domain']}")
        print(f"Table: {data['table_name']}")
        print(f"Total rows: {data['total_rows']}")        
        print(f"Schema columns: {len(data['schema']['columns'])}")
        print(f"Sample data records: {len(data['sample_data'])}")
        
except urllib.error.HTTPError as e:
    print(f"\nHTTP Error {e.code}: {e.reason}")
    error_body = e.read().decode('utf-8')
    print(f"\nRaw Error Body:")
    print(error_body)
    print(f"\nTrying to parse as JSON:")
    try:
        error_data = json.loads(error_body)
        print(json.dumps(error_data, indent=2))
    except:
        print("Not valid JSON")
        
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
