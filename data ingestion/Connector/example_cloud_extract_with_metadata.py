"""
MariaDB Cloud: Extract with Metadata Example
=============================================
Demonstrates how to extract data and save both:
- data.parquet (actual data)
- metadata.json (extraction info)

This is now the recommended way to extract from MariaDB Cloud.
"""

from mariadbcloud_conn import MariaDBCloudConnector

# Configuration (update with your credentials)
config = {
    "host": "serverless-us-west-2.sysp0000.db1.skysql.com",
    "port": 4020,
    "database": "idp_ingestion_db",
    "user": "analyst_readonly",  # Use one of your read-only users
    "password": "YOUR_PASSWORD_HERE",  # Update this
    "ssl_ca": r"C:\Users\Syarifah\OneDrive - M Telecommunication Sdn Bhd\INTERNSHIP\globalsignrootca.pem",
}

print("=" * 70)
print("☁️  MariaDB Cloud: Extract with Metadata")
print("=" * 70)

# Create connector
connector = MariaDBCloudConnector(config)

try:
    # Connect
    print("\n🔌 Connecting to MariaDB SkySQL...")
    connector.connect()
    print("✅ Connected!\n")
    
    # Extraction request
    extraction_request = {
        "entity": "user_credit_card_transaction",  # Table name
        "mode": "full",
        "enable_parallel": False,
        "flatten_json": {}
    }
    
    # Extract with metadata (saves both data and metadata automatically)
    print("📦 Starting extraction with metadata output...\n")
    result = connector.extract_with_metadata(
        extraction_request=extraction_request,
        output_dir="bronze_layer"  # Base output directory
    )
    
    # Display results
    print("\n" + "=" * 70)
    print("📊 EXTRACTION RESULTS")
    print("=" * 70)
    
    if result["success"]:
        print(f"✅ Status: SUCCESS")
        print(f"\n📋 Table: {result['table']}")
        print(f"📊 Rows Extracted: {result['row_count']:,}")
        print(f"⏱️  Extraction Time: {result['extraction_time_seconds']}s")
        print(f"📦 File Size: {result['file_size_kb']:.2f} KB ({result['file_size_kb']/1024:.2f} MB)")
        
        print(f"\n📁 Output Files:")
        print(f"   • Data: {result['data_file']}")
        print(f"   • Metadata: {result['metadata_file']}")
        
        print(f"\n💡 What's in metadata.json?")
        print(f"   • Source info (host, database, user)")
        print(f"   • Extraction timestamp")
        print(f"   • Row/column counts")
        print(f"   • Data schema (column types)")
        print(f"   • Performance metrics (rows/sec, MB/sec)")
        
        # Show directory structure
        print(f"\n📁 Directory Structure:")
        print(f"   bronze_layer/")
        print(f"   └─ {result['table']}/")
        print(f"      └─ YYYYMMDD_HHMMSS/")
        print(f"         ├─ data.parquet")
        print(f"         └─ metadata.json")
        
    else:
        print(f"❌ Status: FAILED")
        print(f"❌ Error: {result['error']}")
    
    print("\n" + "=" * 70)
    print("🎉 COMPLETE")
    print("=" * 70)

except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()

finally:
    connector.close()
    print("\n✅ Connection closed")


# Example output structure:
"""
bronze_layer/
└── user_credit_card_transaction/
    └── 20260213_155030/
        ├── data.parquet          # Actual data (Parquet format, compressed)
        └── metadata.json         # Extraction metadata

metadata.json contains:
{
  "source": {
    "type": "mariadb_cloud",
    "host": "serverless-us-west-2.sysp0000.db1.skysql.com",
    "database": "idp_ingestion_db",
    "user": "analyst_readonly",
    "ssl_enabled": true
  },
  "extraction": {
    "timestamp": "2026-02-13T15:50:30.123456",
    "table": "user_credit_card_transaction",
    "mode": "full",
    "row_count": 1234,
    "column_count": 8,
    "columns": ["id", "user_id", "amount", ...],
    "batch_count": 1,
    "extraction_time_seconds": 2.45
  },
  "output": {
    "data_file": "bronze_layer/user_credit_card_transaction/20260213_155030/data.parquet",
    "file_format": "parquet",
    "compression": "snappy",
    "file_size_kb": 145.32,
    "file_size_mb": 0.14
  },
  "schema": {
    "id": "int64",
    "user_id": "int64",
    "amount": "float64",
    ...
  },
  "statistics": {
    "rows_per_second": 503.67,
    "mb_per_second": 0.06
  }
}
"""
