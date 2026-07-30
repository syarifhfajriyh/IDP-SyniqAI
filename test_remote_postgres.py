"""
Test Remote PostgreSQL Connection for CDC
==========================================
Quick test to verify connection to remote PostgreSQL before creating Debezium connector.

Usage: python test_remote_postgres.py
"""

import sys
import time

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2 not installed")
    print("Install it with: pip install psycopg2-binary")
    sys.exit(1)

# Configuration
REMOTE_HOST = "192.168.0.11"
PORT = 5432
DATABASE = "postgres"
USER = "debezium_user"
PASSWORD = "debezium_password"

print()
print("=" * 80)
print("  Remote PostgreSQL Connection Test")
print("=" * 80)
print()
print(f"Target: {REMOTE_HOST}:{PORT}")
print(f"Database: {DATABASE}")
print(f"User: {USER}")
print()

# Test 1: Regular Connection
print("[1/3] Testing PostgreSQL connection...")
try:
    conn = psycopg2.connect(
        host=REMOTE_HOST,
        port=PORT,
        database=DATABASE,
        user=USER,
        password=PASSWORD,
        connect_timeout=10
    )
    
    cur = conn.cursor()
    cur.execute('SELECT version()')
    version = cur.fetchone()[0]
    
    print(f"  ✓ Connection successful!")
    print(f"    {version[:80]}")
    
    cur.close()
    conn.close()
    
except psycopg2.OperationalError as e:
    error_msg = str(e).strip()
    print(f"  ✗ Connection failed")
    print(f"    Error: {error_msg}")
    print()
    
    if "password authentication failed" in error_msg:
        print("Issue: Wrong password or user doesn't exist")
        print("Fix: On remote PostgreSQL, run:")
        print("  psql -U postgres")
        print("  ALTER USER debezium_user WITH PASSWORD 'debezium_password';")
    elif "no pg_hba.conf entry" in error_msg:
        print("Issue: pg_hba.conf doesn't allow connection from this IP")
        print("Fix: On remote laptop, edit pg_hba.conf and add:")
        print("  host    all             debezium_user   192.168.0.0/16  scram-sha-256")
        print("  (Make sure it says 192.168, not 192.169!)")
        print("Then restart PostgreSQL service")
    elif "could not connect" in error_msg or "Connection refused" in error_msg:
        print("Issue: PostgreSQL not listening on external interface")
        print("Fix: Edit postgresql.conf on remote laptop:")
        print("  listen_addresses = '*'")
        print("Then restart PostgreSQL service")
    
    sys.exit(1)

except Exception as e:
    print(f"  ✗ Connection failed: {e}")
    sys.exit(1)

print()

# Test 2: Check WAL Configuration
print("[2/3] Checking WAL configuration (for CDC)...")
try:
    conn = psycopg2.connect(
        host=REMOTE_HOST,
        port=PORT,
        database=DATABASE,
        user=USER,
        password=PASSWORD,
        connect_timeout=5
    )
    
    cur = conn.cursor()
    cur.execute("""
        SELECT name, setting 
        FROM pg_settings 
        WHERE name IN ('wal_level', 'max_wal_senders', 'max_replication_slots')
    """)
    
    settings = dict(cur.fetchall())
    wal_level = settings.get('wal_level', 'unknown')
    wal_senders = int(settings.get('max_wal_senders', '0'))
    wal_slots = int(settings.get('max_replication_slots', '0'))
    
    print(f"  Current settings:")
    print(f"    wal_level: {wal_level} {'✓' if wal_level == 'logical' else '✗ (need: logical)'}")
    print(f"    max_wal_senders: {wal_senders} {'✓' if wal_senders >= 4 else '✗ (need: >= 4)'}")
    print(f"    max_replication_slots: {wal_slots} {'✓' if wal_slots >= 4 else '✗ (need: >= 4)'}")
    print()
    
    if wal_level == 'logical' and wal_senders >= 4 and wal_slots >= 4:
        print("  ✓ WAL configuration is correct for CDC")
    else:
        print("  ✗ WAL configuration needs updating for CDC")
        print()
        print("On remote PostgreSQL, edit postgresql.conf and add:")
        print("  wal_level = logical")
        print("  max_wal_senders = 10")
        print("  max_replication_slots = 10")
        print("Then restart PostgreSQL service")
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"  ⚠ Cannot check WAL settings: {e}")

print()

# Test 3: Check Test Table
print("[3/3] Checking for test table...")
try:
    conn = psycopg2.connect(
        host=REMOTE_HOST,
        port=PORT,
        database=DATABASE,
        user=USER,
        password=PASSWORD,
        connect_timeout=5
    )
    
    cur = conn.cursor()
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'cdc_test'
        )
    """)
    
    exists = cur.fetchone()[0]
    if exists:
        cur.execute('SELECT COUNT(*) FROM public.cdc_test')
        count = cur.fetchone()[0]
        print(f"  ✓ Table 'cdc_test' exists with {count} rows")
    else:
        print("  ⚠ Table 'cdc_test' doesn't exist yet")
        print("    Run setup_postgres_wal.sql to create it")
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"  ⚠ Could not check table: {e}")

# Summary
print()
print("=" * 80)
print("  Summary")
print("=" * 80)
print()
print("✓ Connection test passed!")
print()
print("Next steps:")
print("  1. Fix pg_hba.conf on remote laptop (if needed):")
print("     - Change 192.169.0.0/16 to 192.168.0.0/16")
print("     - Add: host    replication     debezium_user   192.168.0.0/16  scram-sha-256")
print("     - Restart PostgreSQL service")
print()
print("  2. Create Debezium connector:")
print("     cd 'Kafka Integration'")
print("     python setup_cdc_connectors.py --source postgres")
print()
print("  3. Test CDC by inserting data:")
print("     INSERT INTO public.cdc_test (message) VALUES ('Test from remote');")
print()
print("  4. Check Kafka UI for messages:")
print("     http://localhost:8080 - look for topic: cdc.syniq_postgres.public.cdc_test")
print()
