"""Test Silver with finance_transactions specifically"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "api"))

from minio_utils import MinIOClient
from silver_job_tracker import SilverJobTracker
from silver_service import SilverProcessingService
import json
import time

print("\n" + "="*60)
print("TEST: finance.finance_transactions")
print("="*60)

# Initialize
minio_client = MinIOClient()
job_tracker = SilverJobTracker()
silver_service = SilverProcessingService(minio_client, job_tracker)

# Test reading finance_transactions
print("\n1. Testing Bronze read for finance/finance_transactions...")
df = minio_client.read_parquet("bronze", "finance", "finance_transactions")

if df is not None and not df.empty:
    print(f"   ✅ Successfully read {len(df)} rows")
    print(f"   Columns: {', '.join(df.columns.tolist())}")
else:
    print("   ❌ Failed to read data")
    exit(1)

# Test Silver processing
print("\n2. Starting Silver job for finance.finance_transactions...")
job_id = silver_service.process_to_silver("finance", "finance_transactions", "postgres")
print(f"   ✅ Job started: {job_id}")

# Wait for completion
print("   ⏳ Waiting for job...")
for i in range(30):
    time.sleep(1)
    job = job_tracker.get_job(job_id)
    status = job['status']
    message = job.get('message', '')
    
    print(f"      [{i+1}s] Status: {status} | {message}")
    
    if status == 'completed':
        print("\n   ✅ JOB COMPLETED!")
        print(f"      Row count: {job.get('row_count')}")
        print(f"      Quality score: {job.get('quality_score')}")
        print(f"      Silver path: {job.get('silver_path')}")
        break
    elif status == 'failed':
        print("\n   ❌ JOB FAILED!")
        error = job.get('error_message', job.get('error', 'Unknown'))
        print(f"      Error: {error}")
        break

print("\n" + "="*60 + "\n")
