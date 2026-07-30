#!/usr/bin/env python3
"""Check which database has the loan_applications table"""
import psycopg2

for db_name in ['postgres', 'syniq_postgres']:
    try:
        conn = psycopg2.connect(
            host='192.168.2.114',
            port=5432,
            database=db_name,
            user='debezium_user',
            password='debezium_password'
        )
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'public' 
            AND tablename IN ('loan_applications', 'hosp_raya_patient_record', 'cdc_test')
            ORDER BY tablename
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        
        if tables:
            print(f"\n✓ Database: {db_name}")
            print(f"  Tables found: {', '.join(tables)}")
        else:
            print(f"\n✗ Database: {db_name}")
            print(f"  No target tables found")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\n✗ Database: {db_name}")
        print(f"  Error: {e}")
