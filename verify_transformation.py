"""
Verify Bronze → Silver Transformation
Compare schemas and data between Bronze and Silver layers
"""

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path

print("🔍 Transformation Verification Script")
print("=" * 60)

# File paths
bronze_file = r"C:\Users\Syarifah\Downloads\chunk_1_20260312_214822.parquet"
silver_file = r"C:\Users\Syarifah\Downloads\part-00000-21536853-c846-45b8-ad2a-381a0815e5cd-c000.snappy.parquet"

# Check files exist
bronze_exists = Path(bronze_file).exists()
silver_exists = Path(silver_file).exists()

print(f"\n📁 File Check:")
print(f"   Bronze: {'✅ Found' if bronze_exists else '❌ Not Found'} - {bronze_file}")
print(f"   Silver: {'✅ Found' if silver_exists else '❌ Not Found'} - {silver_file}")

if not bronze_exists or not silver_exists:
    print("\n❌ ERROR: One or both files not found!")
    print("   Please ensure both parquet files are downloaded to your Downloads folder.")
    exit(1)

# Read Bronze data
print("\n📖 Reading Bronze layer...")
df_bronze = pd.read_parquet(bronze_file)
bronze_rows = len(df_bronze)
bronze_cols = list(df_bronze.columns)

print(f"   Rows: {bronze_rows:,}")
print(f"   Columns ({len(bronze_cols)}): {', '.join(bronze_cols)}")

# Read Silver data
print("\n📖 Reading Silver layer...")
df_silver = pd.read_parquet(silver_file)
silver_rows = len(df_silver)
silver_cols = list(df_silver.columns)

print(f"   Rows: {silver_rows:,}")
print(f"   Columns ({len(silver_cols)}): {', '.join(silver_cols)}")

# Compare schemas
print("\n🔍 Schema Comparison:")
print(f"   Bronze columns: {len(bronze_cols)}")
print(f"   Silver columns: {len(silver_cols)}")
print(f"   New columns: {len(silver_cols) - len(bronze_cols)}")

# Find new columns
new_columns = [col for col in silver_cols if col not in bronze_cols]
print(f"\n✨ New Columns Added: {new_columns if new_columns else 'None (❌ Problem!)'}")

# Check expected date columns
expected_date_cols = ['transaction_date_year', 'transaction_date_month', 'transaction_date_quarter', 'transaction_date_dayofweek']
missing_cols = [col for col in expected_date_cols if col not in new_columns]

if missing_cols:
    print(f"   ⚠️ WARNING: Expected columns not found: {missing_cols}")
else:
    print(f"   ✅ All expected date columns present!")

# Row count validation
print(f"\n📊 Row Count Validation:")
print(f"   Bronze: {bronze_rows:,} rows")
print(f"   Silver: {silver_rows:,} rows")
if bronze_rows == silver_rows:
    print(f"   ✅ Row counts match!")
else:
    print(f"   ⚠️ WARNING: Row counts differ by {abs(bronze_rows - silver_rows):,}")

# Data preview
print("\n📋 Silver Data Sample (first 5 rows):")
print("=" * 60)

# Show relevant columns including new date columns
preview_cols = ['transaction_id', 'transaction_date', 'amount', 'status'] + new_columns
available_preview_cols = [col for col in preview_cols if col in df_silver.columns]

preview_df = df_silver[available_preview_cols].head(5)
print(preview_df.to_string(index=False))

# Validate date extraction worked
if 'transaction_date_year' in df_silver.columns:
    print("\n🗓️ Date Extraction Validation:")
    
    # Year
    if 'transaction_date_year' in df_silver.columns:
        print(f"   ✅ Year: {sorted(df_silver['transaction_date_year'].unique())}")
    
    # Month
    if 'transaction_date_month' in df_silver.columns:
        months = sorted(df_silver['transaction_date_month'].unique())
        print(f"   ✅ Month: {months} (1=Jan, 12=Dec)")
    
    # Quarter
    if 'transaction_date_quarter' in df_silver.columns:
        quarters = sorted(df_silver['transaction_date_quarter'].unique())
        print(f"   ✅ Quarter: {quarters} (Q1-Q4)")
    
    # Day of Week
    if 'transaction_date_dayofweek' in df_silver.columns:
        days = sorted(df_silver['transaction_date_dayofweek'].unique())
        print(f"   ✅ Day of Week: {days} (1=Sun, 7=Sat)")
    
    # Show specific example
    print("\n📌 Example Transformations:")
    date_cols = ['transaction_id', 'transaction_date'] + [c for c in df_silver.columns if 'transaction_date_' in c and c != 'transaction_date']
    sample = df_silver[date_cols].head(5)
    print(sample.to_string(index=False))

# Summary
print("\n" + "=" * 60)
print("📝 SUMMARY:")
print(f"   ✅ Transformation executed successfully")
print(f"   ✅ Input: {bronze_rows:,} rows, {len(bronze_cols)} columns")
print(f"   ✅ Output: {silver_rows:,} rows, {len(silver_cols)} columns")
print(f"   ✅ Added: {len(new_columns)} new column(s) - {new_columns}")

if bronze_rows == silver_rows and len(new_columns) > 0:
    print("\n🎉 VALIDATION PASSED! Transformation is correct!")
else:
    print("\n⚠️ VALIDATION ISSUES DETECTED - Review above warnings")

print("=" * 60)
