"""
Verify JOIN Output - Check if join on full_name worked correctly
"""
import pyarrow.parquet as pq
import pandas as pd
import os

print("🔍 Verifying JOIN Output on 'full_name' column\n")
print("=" * 60)

# Check Downloads folder
downloads_path = r"C:\Users\Syarifah\Downloads\test_join_output.parquet"

if os.path.exists(downloads_path):
    print(f"✅ Found file: {downloads_path}\n")
    
    # Read Parquet file
    table = pq.read_table(downloads_path)
    df = table.to_pandas()
    
    print(f"📊 File Info:")
    print(f"   Total Rows: {len(df)}")
    print(f"   Total Columns: {len(df.columns)}")
    print(f"   File Size: {os.path.getsize(downloads_path) / 1024:.2f} KB\n")
    
    print(f"📋 Columns in Result:")
    for i, col in enumerate(df.columns, 1):
        dtype = df[col].dtype
        null_count = df[col].isnull().sum()
        print(f"   {i}. {col:<30} ({dtype}) - {null_count} nulls")
    
    print(f"\n🔍 Checking for 'full_name' column:")
    if 'full_name' in df.columns:
        print(f"   ✅ 'full_name' column exists!")
        print(f"   Unique values: {df['full_name'].nunique()}")
        print(f"   Sample values:")
        for val in df['full_name'].head(5):
            print(f"      • {val}")
    else:
        print(f"   ❌ 'full_name' column NOT found!")
    
    # Check for duplicate columns (indicates successful join)
    print(f"\n🔗 JOIN Analysis:")
    original_cols = [col for col in df.columns if not col.endswith('_right')]
    joined_cols = [col for col in df.columns if col.endswith('_right')]
    
    if joined_cols:
        print(f"   ✅ JOIN successful! Found {len(joined_cols)} joined columns:")
        for col in joined_cols[:10]:  # Show first 10
            print(f"      • {col}")
    else:
        print(f"   ⚠️  No '_right' suffixed columns found")
        print(f"   This might be a self-join without column renaming")
    
    # Show sample data
    print(f"\n📄 Sample Data (first 3 rows):")
    print("=" * 60)
    print(df.head(3).to_string())
    
    # Check for duplicate full_name values (expected in self-join)
    if 'full_name' in df.columns:
        print(f"\n🔄 Self-Join Check:")
        value_counts = df['full_name'].value_counts()
        duplicates = value_counts[value_counts > 1]
        if len(duplicates) > 0:
            print(f"   ✅ Found {len(duplicates)} full_name values with multiple matches")
            print(f"   This indicates a successful self-join!")
            print(f"\n   Top 5 most common full_name values:")
            for name, count in duplicates.head(5).items():
                print(f"      • {name}: {count} occurrences")
        else:
            print(f"   ℹ️  Each full_name appears only once")
            print(f"   This is expected for 1:1 joins")
    
    print(f"\n{'=' * 60}")
    print(f"✅ JOIN Verification Complete!")
    
else:
    print(f"❌ File not found: {downloads_path}")
    print(f"\nTrying to check MinIO Silver layer...")
    
    # Alternative: Check MinIO directly
    try:
        import boto3
        from botocore.client import Config
        
        s3 = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='admin',
            aws_secret_access_key='password123',
            config=Config(signature_version='s3v4')
        )
        
        # List objects in silver bucket
        response = s3.list_objects_v2(
            Bucket='syniqai-silver',
            Prefix='finance/test_join_output'
        )
        
        if 'Contents' in response:
            print(f"\n✅ Found {len(response['Contents'])} files in MinIO:")
            for obj in response['Contents']:
                size_kb = obj['Size'] / 1024
                print(f"   • {obj['Key']} ({size_kb:.2f} KB)")
        else:
            print(f"\n❌ No files found in MinIO Silver layer")
            
    except Exception as e:
        print(f"\n❌ Could not connect to MinIO: {e}")
