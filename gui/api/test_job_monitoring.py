"""Test Job Monitoring API"""
import requests

try:
    print("🧪 Testing Job Monitoring API...")
    print("=" * 60)
    
    # Test 1: Get all jobs
    print("\n1. Fetching all jobs...")
    response = requests.get('http://localhost:8000/api/silver/jobs', params={'limit': 10}, timeout=5)
    
    print(f"   Status Code: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        jobs = data.get('jobs', [])
        total = data.get('total', 0)
        
        print(f"   ✅ API Working! Found {total} jobs")
        
        if jobs:
            print(f"\n   Recent Jobs:")
            for i, job in enumerate(jobs[:5], 1):
                print(f"      {i}. {job['job_id']}")
                print(f"         Status: {job['status']}")
                print(f"         Table: {job.get('table_name', 'N/A')}")
                print(f"         Started: {job.get('started_at', 'N/A')}")
                if job.get('completed_at'):
                    print(f"         Completed: {job['completed_at']}")
                print()
        else:
            print("\n   ℹ️  No jobs found yet")
            print("   💡 Run a transformation to create jobs!")
    else:
        print(f"   ❌ API Error: {response.status_code}")
        print(f"   Response: {response.text}")
    
    # Test 2: Get running jobs
    print("\n2. Checking for running jobs...")
    response = requests.get('http://localhost:8000/api/silver/jobs', params={'status': 'running', 'limit': 10}, timeout=5)
    
    if response.status_code == 200:
        data = response.json()
        running_jobs = data.get('jobs', [])
        print(f"   Running jobs: {len(running_jobs)}")
        
        if running_jobs:
            for job in running_jobs:
                print(f"      - {job['table_name']}: {job.get('progress', 0)}% complete")
    
    # Test 3: Get completed jobs
    print("\n3. Checking completed jobs...")
    response = requests.get('http://localhost:8000/api/silver/jobs', params={'status': 'completed', 'limit': 5}, timeout=5)
    
    if response.status_code == 200:
        data = response.json()
        completed_jobs = data.get('jobs', [])
        print(f"   Completed jobs: {len(completed_jobs)}")
        
        if completed_jobs:
            for job in completed_jobs:
                print(f"      - {job['table_name']}: {job.get('row_count', 'N/A')} rows")
    
    # Test 4: Get failed jobs
    print("\n4. Checking failed jobs...")
    response = requests.get('http://localhost:8000/api/silver/jobs', params={'status': 'failed', 'limit': 5}, timeout=5)
    
    if response.status_code == 200:
        data = response.json()
        failed_jobs = data.get('jobs', [])
        print(f"   Failed jobs: {len(failed_jobs)}")
        
        if failed_jobs:
            for job in failed_jobs:
                print(f"      - {job['table_name']}: {job.get('error_message', 'Unknown error')}")
    
    print("\n" + "=" * 60)
    print("✅ Job Monitoring API Test Complete!")
    print("\n📌 Next Steps:")
    print("   1. Open your browser: http://localhost:3000")
    print("   2. Navigate to: Silver Layer → Job Monitoring")
    print("   3. You should see the jobs listed above")
    print("   4. Try running a transformation to see live updates!")
    
except requests.exceptions.ConnectionError:
    print("❌ Backend not running!")
    print("   Start it with: python start_backend.py")
except Exception as e:
    print(f"❌ Test failed: {e}")
    import traceback
    traceback.print_exc()
