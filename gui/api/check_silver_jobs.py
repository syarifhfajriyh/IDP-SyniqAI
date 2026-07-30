import requests

resp = requests.get('http://localhost:8000/api/silver/jobs', params={'limit': 5}, timeout=5)
print('Status:', resp.status_code)

jobs = resp.json()['jobs']
print(f'\n✅ Found {len(jobs)} Silver job(s)\n')

for job in jobs[:3]:
    print(f"  • {job['entity']}:")
    print(f"    - Status: {job['status']}")
    print(f"    - Rows: {job.get('row_count', 0):,}")
    print(f"    - Quality Score: {job.get('quality_score', 'N/A')}")
    print(f"    - Started: {job.get('started_at', 'N/A')}")
    print()
