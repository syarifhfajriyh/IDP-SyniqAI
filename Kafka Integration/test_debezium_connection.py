#!/usr/bin/env python3
"""
Test Debezium user connection to PostgreSQL
"""
import psycopg2
import sys

DB_CONFIG = {
    'host': '192.168.2.114',
    'port': 5432,
    'database': 'postgres',  # Changed to 'postgres' database
    'user': 'debezium_user',
    'password': 'debezium_password'
}

try:
    print(f"Connecting to PostgreSQL as debezium_user...")
    print(f"  Host: {DB_CONFIG['host']}")
    print(f"  Database: {DB_CONFIG['database']}")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Test query
    cursor.execute("SELECT current_database(), current_user, version()")
    db, user, version = cursor.fetchone()
    
    print(f"\n✓ Connection successful!")
    print(f"  Database: {db}")
    print(f"  User: {user}")
    print(f"  PostgreSQL: {version.split(',')[0]}")
    
    # Check replication permissions
    cursor.execute("""
        SELECT rolname, rolreplication 
        FROM pg_roles 
        WHERE rolname = 'debezium_user'
    """)
    user, has_replication = cursor.fetchone()
    
    if has_replication:
        print(f"\n✓ Replication privilege: ENABLED")
    else:
        print(f"\n✗ WARNING: Replication privilege NOT enabled!")
    
    # Check publication
    cursor.execute("""
        SELECT pubname 
        FROM pg_publication 
        WHERE pubname = 'dbz_syniq_postgres_publication'
    """)
    result = cursor.fetchone()
    
    if result:
        print(f"✓ Publication exists: {result[0]}")
    else:
        print(f"✗ WARNING: Publication not found!")
    
    cursor.close()
    conn.close()
    
    print(f"\n✅ Ready for CDC! You can now restart the Debezium connector.\n")
    sys.exit(0)
    
except psycopg2.OperationalError as e:
    print(f"\n✗ Connection failed: {e}")
    print(f"\nTroubleshooting:")
    print(f"  1. Make sure PostgreSQL is running on {DB_CONFIG['host']}")
    print(f"  2. Run setup_debezium_user_quick.sql on the PostgreSQL server")
    print(f"  3. Check pg_hba.conf allows connections from this machine")
    sys.exit(1)
except Exception as e:
    print(f"\n✗ Error: {e}")
    sys.exit(1)
