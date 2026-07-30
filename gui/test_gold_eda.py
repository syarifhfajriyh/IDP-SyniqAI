"""Test Gold EDA generation from Silver data"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "api"))

from minio_utils import MinIOClient
from gold_eda_service import GoldEDAService
import json

print("\n" + "="*60)
print("TEST: Gold EDA Generation")
print("="*60)

# Initialize
minio_client = MinIOClient()
gold_service = GoldEDAService(minio_client)

# Test: Generate EDA report for finance.finance_transactions
print("\n1. Checking Silver data exists...")
df = minio_client.read_parquet("silver", "finance", "finance_transactions")

if df is not None and not df.empty:
    print(f"   ✅ Silver data found: {len(df)} rows")
else:
    print("   ❌ No Silver data found")
    print("\n   Run Silver transformation first!")
    exit(1)

# Generate EDA report
print("\n2. Generating EDA report...")
result = gold_service.generate_eda_report("finance", "finance_transactions")

if result.get("success"):
    print("   ✅ EDA report generated successfully!")
    report = result["report"]
    print(f"\n   📊 Report Summary:")
    print(f"      Rows: {report.get('basic_info', {}).get('row_count', 'N/A')}")
    print(f"      Columns: {report.get('basic_info', {}).get('column_count', 'N/A')}")
    print(f"      Quality Score: {report.get('metrics', {}).get('quality_score', 'N/A')}")
    print(f"      Completeness: {report.get('metrics', {}).get('completeness', 'N/A')}%")
    
    # Check if saved to Gold
    if 'report_path' in result:
        print(f"\n   💾 Saved to: {result['report_path']}")
else:
    print("   ❌ EDA generation failed!")
    print(f"      Error: {result.get('error', 'Unknown')}")

# List available reports
print("\n3. Checking available EDA reports...")
report = gold_service.get_latest_report("finance", "finance_transactions")
if report:
    print("   ✅ Latest report available")
    print(f"      Generated: {report.get('generated_at', 'N/A')}")
else:
    print("   ⚠️  No report found")

print("\n" + "="*60 + "\n")
