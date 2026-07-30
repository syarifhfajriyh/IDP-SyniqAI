import requests

# Test query with finance_transactions
response = requests.post(
    'http://localhost:8000/api/query/execute-validation-query',
    json={
        'sql': 'SELECT * FROM finance_transactions LIMIT 3',
        'table_name': 'finance_transactions',
        'domain': 'finance',
        'source': 'postgres'
    }
)

data = response.json()
print("="*60)
print("QUERY TEST RESULTS")
print("="*60)
print(f"Success: {data['success']}")
print(f"Rows returned: {data['row_count']}")
print(f"Execution time: {data['execution_time_ms']:.2f}ms")
print(f"\nColumns ({len(data['columns'])}):")
print("  " + ", ".join(data['columns']))

if data['rows']:
    row = data['rows'][0]
    print(f"\nFirst Row Data:")
    print(f"  transaction_id: {row.get('transaction_id')}")
    print(f"  user_id: {row.get('user_id')}")  
    print(f"  full_name: {row.get('full_name', 'MISSING')}")
    print(f"  email: {row.get('email', 'MISSING')}")
    print(f"  merchant field exists?: {'merchant' in row}")
    
    print("\n" + "="*60)
    if 'merchant' in row:
        print("STATUS: STILL USING SAMPLE DATA (has merchant field)")
        print("EXPECTED: Real data with full_name, email, account_number")
    elif 'full_name' in row:
        print("STATUS: SUCCESS - REAL DATA FROM MINIO!")
        print("Confirmed by: full_name field present, no merchant field")
    print("="*60)
