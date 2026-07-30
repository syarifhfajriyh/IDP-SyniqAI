"""
Multi-Tenant IDP Ingestion Platform
====================================
Extracts data from multiple client read-only users.
Each client's data is stored in separate bronze layer folders.

This simulates a real SaaS IDP platform serving multiple clients.

Usage:
    python multi_tenant_ingestion.py
"""

import json
import os
import pandas as pd
from datetime import datetime
from mariadbcloud_conn import MariaDBCloudConnector

# Load credentials from file received from clients
CREDENTIALS_FILE = r"C:\Users\Syarifah\Downloads\readonly_users_list.json"
SSL_CERT = r"C:\Users\Syarifah\OneDrive - M Telecommunication Sdn Bhd\INTERNSHIP\globalsignrootca.pem"

print("=" * 70)
print("🏢 MULTI-TENANT IDP PLATFORM")
print("=" * 70)
print(f"\n📥 Loading client credentials from: {CREDENTIALS_FILE}")

try:
    with open(CREDENTIALS_FILE, 'r') as f:
        clients = json.load(f)
    print(f"✅ Found {len(clients)} client accounts\n")
except Exception as e:
    print(f"❌ Error loading credentials: {e}")
    exit(1)

# Create base directories
os.makedirs("bronze_data_multi_tenant", exist_ok=True)
os.makedirs("metadata_multi_tenant", exist_ok=True)

# Process each client
ingestion_summary = []

for idx, client in enumerate(clients, 1):
    print("\n" + "=" * 70)
    print(f"🔄 Processing Client {idx}/{len(clients)}")
    print("=" * 70)
    print(f"📋 Account: {client['username']}")
    print(f"📋 Purpose: {client['description']}")
    print(f"📋 Database: {client['database']}")
    
    # Build config
    config = {
        "host": client['host'],
        "port": client['port'],
        "database": client['database'],
        "user": client['username'],
        "password": client['password'],
        "ssl_ca": SSL_CERT,
        "ssl_verify_cert": True
    }
    
    client_result = {
        "username": client['username'],
        "description": client['description'],
        "status": None,
        "tables_extracted": [],
        "total_rows": 0,
        "files_created": []
    }
    
    try:
        # Connect
        print(f"\n🔌 Connecting as '{client['username']}'...")
        connector = MariaDBCloudConnector(config)
        connector.connect()
        print("   ✅ Connected")
        
        # Get list of tables
        print(f"\n📊 Discovering tables...")
        from sqlalchemy import text
        with connector.engine.connect() as conn:
            tables_result = conn.execute(text("""
                SELECT TABLE_NAME 
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = :db
                AND TABLE_TYPE = 'BASE TABLE'
            """), {"db": client['database']})
            tables = [row[0] for row in tables_result]
        
        print(f"   ✅ Found {len(tables)} table(s): {', '.join(tables)}")
        
        # Extract each table
        for table_name in tables:
            print(f"\n💾 Extracting: {table_name}")
            
            extraction_request = {
                "entity": table_name,
                "mode": "full",
                "enable_parallel": False,
                "flatten_json": {}
            }
            
            all_data = []
            batch_count = 0
            
            for batch_df in connector.extract(extraction_request):
                batch_count += 1
                all_data.append(batch_df)
                print(f"   📦 Batch {batch_count}: {len(batch_df)} rows")
            
            if all_data:
                # Combine batches
                final_df = pd.concat(all_data, ignore_index=True)
                
                # Create client-specific folder
                client_folder = f"bronze_data_multi_tenant/{client['username']}"
                os.makedirs(client_folder, exist_ok=True)
                
                # Save data
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                parquet_file = f"{client_folder}/{table_name}_{timestamp}.parquet"
                final_df.to_parquet(parquet_file, index=False, compression='snappy')
                
                file_size_kb = os.path.getsize(parquet_file) / 1024
                print(f"   ✅ Saved to: {parquet_file}")
                print(f"   📊 Total rows: {len(final_df):,}")
                print(f"   📦 File size: {file_size_kb:.2f} KB")
                
                # Save metadata
                metadata_folder = f"metadata_multi_tenant/{client['username']}"
                os.makedirs(metadata_folder, exist_ok=True)
                
                metadata = {
                    "client": {
                        "username": client['username'],
                        "description": client['description']
                    },
                    "extraction_timestamp": datetime.now().isoformat(),
                    "table": table_name,
                    "row_count": len(final_df),
                    "columns": list(final_df.columns),
                    "data_file": parquet_file,
                    "file_size_kb": file_size_kb
                }
                
                metadata_file = f"{metadata_folder}/{table_name}_{timestamp}.json"
                with open(metadata_file, 'w') as f:
                    json.dump(metadata, f, indent=2)
                
                print(f"   ✅ Metadata saved: {metadata_file}")
                
                # Update summary
                client_result["tables_extracted"].append(table_name)
                client_result["total_rows"] += len(final_df)
                client_result["files_created"].extend([parquet_file, metadata_file])
        
        connector.close()
        client_result["status"] = "✅ SUCCESS"
        print(f"\n✅ Client '{client['username']}' - Extraction complete!")
        
    except Exception as e:
        print(f"\n❌ Client '{client['username']}' - Failed: {e}")
        client_result["status"] = f"❌ FAILED: {e}"
        import traceback
        traceback.print_exc()
    
    ingestion_summary.append(client_result)

# Final Report
print("\n\n" + "=" * 70)
print("📊 MULTI-TENANT INGESTION REPORT")
print("=" * 70)

total_rows = 0
total_files = 0

for result in ingestion_summary:
    status_icon = "✅" if "SUCCESS" in result["status"] else "❌"
    print(f"\n{status_icon} {result['username']} - {result['description']}")
    print(f"   Status: {result['status']}")
    print(f"   Tables extracted: {len(result['tables_extracted'])}")
    print(f"   Total rows: {result['total_rows']:,}")
    print(f"   Files created: {len(result['files_created'])}")
    
    if result['tables_extracted']:
        print(f"   Tables: {', '.join(result['tables_extracted'])}")
    
    total_rows += result['total_rows']
    total_files += len(result['files_created'])

# Save summary report
summary_file = f"ingestion_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
with open(summary_file, 'w') as f:
    json.dump({
        "ingestion_date": datetime.now().isoformat(),
        "total_clients": len(clients),
        "total_rows_extracted": total_rows,
        "total_files_created": total_files,
        "client_results": ingestion_summary
    }, f, indent=2)

print(f"\n💾 Summary saved: {summary_file}")

# Success metrics
success_count = sum(1 for r in ingestion_summary if "SUCCESS" in r["status"])
print(f"\n🎯 Success Rate: {success_count}/{len(clients)} clients ({success_count/len(clients)*100:.1f}%)")
print(f"📊 Total Rows Extracted: {total_rows:,}")
print(f"📦 Total Files Created: {total_files}")

print("\n" + "=" * 70)
print("🎉 MULTI-TENANT INGESTION COMPLETE")
print("=" * 70)

# Show directory structure
print("\n📁 Output Structure:")
print("bronze_data_multi_tenant/")
for result in ingestion_summary:
    if result['tables_extracted']:
        print(f"  ├─ {result['username']}/")
        for table in result['tables_extracted']:
            print(f"  │  └─ {table}_*.parquet")

print("\nmetadata_multi_tenant/")
for result in ingestion_summary:
    if result['tables_extracted']:
        print(f"  ├─ {result['username']}/")
        for table in result['tables_extracted']:
            print(f"  │  └─ {table}_*.json")
