"""
Test Silver Backend - Diagnostic Script
Checks if Bronze data exists and tests the Silver service directly
"""

import sys
from pathlib import Path

# Add API path
sys.path.insert(0, str(Path(__file__).parent / "api"))

from minio_utils import MinIOClient
from silver_job_tracker import SilverJobTracker
from silver_service import SilverProcessingService
import json

def main():
    print("\n" + "="*60)
    print("SILVER BACKEND DIAGNOSTIC TEST")
    print("="*60)
    
    # Initialize MinIO client
    print("\n1. Initializing MinIO client...")
    try:
        minio_client = MinIOClient()
        print("   ✅ MinIO client initialized")
    except Exception as e:
        print(f"   ❌ Failed to initialize MinIO: {e}")
        return
    
    # List Bronze tables
    print("\n2. Checking Bronze tables...")
    try:
        bronze_tables = minio_client.list_tables("bronze")
        if not bronze_tables:
            print("   ⚠️  No Bronze tables found!")
            print("   You need to run Bronze ingestion first")
            return
        
        print(f"   ✅ Found {len(bronze_tables)} Bronze tables:")
        for table in bronze_tables:
            print(f"      - {table['source']}.{table['entity']}")
    except Exception as e:
        print(f"   ❌ Error listing Bronze tables: {e}")
        return
    
    # Check if finance.finance_transactions exists
    print("\n3. Checking finance.finance_transactions specifically...")
    finance_table = None
    for table in bronze_tables:
        if table['source'] == 'finance' and table['entity'] == 'finance_transactions':
            finance_table = table
            break
    
    if not finance_table:
        print("   ⚠️  finance.finance_transactions NOT found in Bronze!")
        print("   Available tables:")
        for table in bronze_tables:
            print(f"      - {table['source']}.{table['entity']}")
        
        # Use first available table for testing
        if bronze_tables:
            finance_table = bronze_tables[0]
            print(f"\n   📝 Will use {finance_table['source']}.{finance_table['entity']} for testing")
    else:
        print("   ✅ finance.finance_transactions found!")
    
    # Try reading the data
    print("\n4. Testing Bronze data read...")
    try:
        df = minio_client.read_parquet(
            "bronze",
            finance_table['source'],
            finance_table['entity']
        )
        
        if df is None or df.empty:
            print("   ❌ No data returned (empty or None)")
            return
        
        print(f"   ✅ Successfully read {len(df)} rows")
        print(f"   Columns: {', '.join(df.columns.tolist())}")
        print(f"\n   First 3 rows:")
        print(df.head(3).to_string())
    except Exception as e:
        print(f"   ❌ Error reading data: {e}")
        import traceback
        print(f"\n   Full error:\n{traceback.format_exc()}")
        return
    
    # Test Silver service
    print("\n5. Testing Silver service...")
    try:
        job_tracker = SilverJobTracker()
        silver_service = SilverProcessingService(minio_client, job_tracker)
        print("   ✅ Silver service initialized")
        
        # Check if transformer loaded
        if silver_service.transformer:
            print("   ✅ Silver transformer loaded")
        else:
            print("   ⚠️  Silver transformer not loaded (will use basic cleaning)")
    except Exception as e:
        print(f"   ❌ Error initializing Silver service: {e}")
        import traceback
        print(f"\n   Full error:\n{traceback.format_exc()}")
        return
    
    # Start a test job
    print(f"\n6. Starting test Silver job for {finance_table['source']}.{finance_table['entity']}...")
    try:
        job_id = silver_service.process_to_silver(
            finance_table['source'],
            finance_table['entity'],
            "postgres"
        )
        print(f"   ✅ Job started: {job_id}")
        
        # Wait for job to complete
        print("   ⏳ Waiting for job to complete...")
        import time
        for i in range(30):  # Wait up to 30 seconds
            time.sleep(1)
            job = job_tracker.get_job(job_id)
            
            status = job['status']
            message = job.get('message', '')
            progress = job.get('progress', 0)
            
            print(f"      [{i+1}s] Status: {status} | Progress: {progress}% | {message}")
            
            if status == 'completed':
                print("\n   ✅ JOB COMPLETED SUCCESSFULLY!")
                print(f"      Silver path: {job.get('silver_path')}")
                print(f"      Row count: {job.get('row_count')}")
                print(f"      Quality score: {job.get('quality_score')}")
                break
            elif status == 'failed':
                print("\n   ❌ JOB FAILED!")
                error = job.get('error_message', job.get('error', 'Unknown error'))
                print(f"      Error: {error}")
                
                # Show full job details
                print("\n   Full job details:")
                print(json.dumps(job, indent=2, default=str))
                break
        else:
            print("\n   ⏱️  Job still running after 30 seconds...")
            print("   Check logs for details")
    
    except Exception as e:
        print(f"   ❌ Error starting job: {e}")
        import traceback
        print(f"\n   Full error:\n{traceback.format_exc()}")
    
    print("\n" + "="*60)
    print("TEST COMPLETE")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()
