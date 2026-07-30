"""Test DuckDB S3 connectivity to MinIO"""
import duckdb

conn = duckdb.connect(':memory:')

# Install and load httpfs
conn.execute('INSTALL httpfs')
conn.execute('LOAD httpfs')

# Configure S3 for MinIO
conn.execute("""
    SET s3_endpoint='localhost:9000';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
    SET s3_access_key_id='admin';
    SET s3_secret_access_key='password123';
""")

print("✅ DuckDB S3 configured for MinIO")

# Test reading from actual path
try:
    result = conn.execute("SELECT COUNT(*) FROM 's3://syniqai-bronze/finance/postgres/finance_transactions/*.parquet'").fetchone()
    print(f"✅ DuckDB successfully read {result[0]:,} rows from MinIO")
    
    # Test creating a view
    conn.execute("CREATE VIEW bronze.finance_transactions AS SELECT * FROM 's3://syniqai-bronze/finance/postgres/finance_transactions/*.parquet'")
    count = conn.execute("SELECT COUNT(*) FROM bronze.finance_transactions").fetchone()[0]
    print(f"✅ View created successfully with {count:,} rows")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()

conn.close()
