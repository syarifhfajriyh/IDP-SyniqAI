import requests
import json

query = "SELECT transaction_id, user_id, full_name, amount, currency FROM finance_transactions LIMIT 5"

resp = requests.post(
    'http://localhost:8000/api/silver/sql-preview',
    json={
        'sql': query,
        'source': 'finance',
        'entity': 'finance_transactions'
    },
    timeout=30
)

print('Status:', resp.status_code)

if resp.status_code == 200:
    data = resp.json()
    print(f"\n✅ Query: {data['sql'][:70]}...")
    print(f"⚡ Execution Time: {data['executionTime']}")
    print(f"📊 Results: {data['rowCount']} row(s)\n")
    
    for i, row in enumerate(data['rows'], 1):
        print(f"Row {i}:")
        for col, val in zip(data['columns'], row):
            print(f"  {col}: {val}")
        print()
else:
    print(json.dumps(resp.json(), indent=2))
