"""
Test All Read-Only Users from Client
=====================================
Tests multiple read-only users received from the client.
Simulates multi-tenant IDP platform serving multiple users.

Usage:
    python test_all_readonly_users.py
"""

import json
import os
from mariadbcloud_conn import MariaDBCloudConnector
from sqlalchemy import create_engine, text
from datetime import datetime

# Load credentials from file received from client
CREDENTIALS_FILE = r"C:\Users\Syarifah\Downloads\readonly_users_list.json"

print("=" * 70)
print("🚀 MULTI-USER IDP PLATFORM TEST")
print("=" * 70)
print(f"\n📥 Loading credentials from: {CREDENTIALS_FILE}")

try:
    with open(CREDENTIALS_FILE, 'r') as f:
        users = json.load(f)
    print(f"✅ Found {len(users)} read-only users from client\n")
except Exception as e:
    print(f"❌ Error loading credentials: {e}")
    exit(1)

# SSL Certificate (shared across all users)
SSL_CERT = r"C:\Users\Syarifah\OneDrive - M Telecommunication Sdn Bhd\INTERNSHIP\globalsignrootca.pem"

# Test each user
results = []

for idx, user_info in enumerate(users, 1):
    print("=" * 70)
    print(f"🔐 Testing User {idx}/{len(users)}: {user_info['username']}")
    print(f"📋 Description: {user_info['description']}")
    print("=" * 70)
    
    # Build config for this user
    config = {
        "host": user_info['host'],
        "port": user_info['port'],
        "database": user_info['database'],
        "user": user_info['username'],
        "password": user_info['password'],
        "ssl_ca": SSL_CERT,
        "ssl_verify_cert": True
    }
    
    user_result = {
        "username": user_info['username'],
        "description": user_info['description'],
        "tests": {}
    }
    
    try:
        # Test 1: Connection
        print(f"\n🔌 Test 1: Connecting as '{user_info['username']}'...")
        connector = MariaDBCloudConnector(config)
        connector.connect()
        print("   ✅ Connection successful")
        user_result["tests"]["connection"] = "✅ SUCCESS"
        
        # Test 2: Security Validation
        print(f"\n🔒 Test 2: Security Validation...")
        connection_string = (
            f"mysql+pymysql://{config['user']}:{config['password']}"
            f"@{config['host']}:{config['port']}/{config['database']}"
        )
        
        engine = create_engine(
            connection_string,
            connect_args={
                "ssl": {"ca": config["ssl_ca"], "check_hostname": True},
                "connect_timeout": 30
            }
        )
        
        with engine.connect() as conn:
            # Test SELECT (should work)
            try:
                result = conn.execute(text("SELECT COUNT(*) FROM customers"))
                row_count = result.scalar()
                print(f"   ✅ SELECT: Allowed ({row_count} rows found)")
                user_result["tests"]["select"] = f"✅ ALLOWED ({row_count} rows)"
            except Exception as e:
                print(f"   ❌ SELECT: Failed - {e}")
                user_result["tests"]["select"] = f"❌ FAILED: {e}"
            
            # Test INSERT (should fail)
            try:
                conn.execute(text("INSERT INTO customers (name, email) VALUES ('Test', 'test@test.com')"))
                conn.commit()
                print(f"   ❌ INSERT: Allowed (SECURITY RISK!)")
                user_result["tests"]["insert"] = "❌ SECURITY RISK"
            except Exception:
                print(f"   ✅ INSERT: Blocked correctly")
                user_result["tests"]["insert"] = "✅ BLOCKED"
            
            # Test UPDATE (should fail)
            try:
                conn.execute(text("UPDATE customers SET name='Hacked' WHERE id=1"))
                conn.commit()
                print(f"   ❌ UPDATE: Allowed (SECURITY RISK!)")
                user_result["tests"]["update"] = "❌ SECURITY RISK"
            except Exception:
                print(f"   ✅ UPDATE: Blocked correctly")
                user_result["tests"]["update"] = "✅ BLOCKED"
            
            # Test DELETE (should fail)
            try:
                conn.execute(text("DELETE FROM customers WHERE id=1"))
                conn.commit()
                print(f"   ❌ DELETE: Allowed (SECURITY RISK!)")
                user_result["tests"]["delete"] = "❌ SECURITY RISK"
            except Exception:
                print(f"   ✅ DELETE: Blocked correctly")
                user_result["tests"]["delete"] = "✅ BLOCKED"
        
        engine.dispose()
        
        # Test 3: Data Extraction
        print(f"\n📊 Test 3: Data Extraction...")
        extraction_request = {
            "entity": "customers",
            "mode": "full",
            "enable_parallel": False,
            "flatten_json": {}
        }
        
        batch_count = 0
        total_rows = 0
        
        for batch_df in connector.extract(extraction_request):
            batch_count += 1
            total_rows += len(batch_df)
        
        print(f"   ✅ Extracted {total_rows} rows in {batch_count} batch(es)")
        user_result["tests"]["extraction"] = f"✅ SUCCESS ({total_rows} rows)"
        
        connector.close()
        user_result["overall"] = "✅ ALL TESTS PASSED"
        print(f"\n✅ User '{user_info['username']}' - All tests passed!\n")
        
    except Exception as e:
        print(f"\n❌ User '{user_info['username']}' - Test failed: {e}\n")
        user_result["overall"] = f"❌ FAILED: {e}"
        import traceback
        traceback.print_exc()
    
    results.append(user_result)

# Final Summary
print("\n" + "=" * 70)
print("📊 FINAL SUMMARY - ALL USERS")
print("=" * 70)

for result in results:
    status_icon = "✅" if "ALL TESTS PASSED" in result["overall"] else "❌"
    print(f"\n{status_icon} {result['username']} ({result['description']})")
    print(f"   Status: {result['overall']}")
    
    if result["tests"]:
        print(f"   Tests:")
        for test_name, test_result in result["tests"].items():
            print(f"      • {test_name}: {test_result}")

# Save results to file
report_file = f"multi_user_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
with open(report_file, 'w') as f:
    json.dump({
        "test_date": datetime.now().isoformat(),
        "total_users": len(users),
        "results": results
    }, f, indent=2)

print(f"\n💾 Test report saved: {report_file}")

# Count successes
success_count = sum(1 for r in results if "ALL TESTS PASSED" in r["overall"])
print(f"\n🎯 Results: {success_count}/{len(users)} users passed all tests")

print("\n" + "=" * 70)
print("🎉 MULTI-USER TESTING COMPLETE")
print("=" * 70)
