#!/usr/bin/env python3
"""Check loan_applications table schema"""
import psycopg2

DB_CONFIG = {
    'host': '192.168.2.114',
    'port': 5432,
    'database': 'postgres',
    'user': 'debezium_user',
    'password': 'debezium_password'
}

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'loan_applications' 
        AND table_schema = 'public'
        ORDER BY ordinal_position
    """)
    
    print("\n📋 loan_applications table schema:")
    print("-" * 40)
    for col, dtype in cursor.fetchall():
        print(f"  {col:<30} {dtype}")
    
    # Show sample data
    cursor.execute("SELECT * FROM loan_applications LIMIT 3")
    print(f"\n📊 Sample rows: {cursor.rowcount}")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"✗ Error: {e}")
