#!/usr/bin/env python3
"""
Test CDC with INSERT, UPDATE, and DELETE operations
"""
import psycopg2
from datetime import datetime
import time

DB_CONFIG = {
    'host': '192.168.2.114',
    'port': 5432,
    'database': 'postgres',
    'user': 'debezium_user',
    'password': 'debezium_password'
}

def test_cdc_operations():
    """Test INSERT, UPDATE, DELETE with CDC"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Get next available loan_id
        cursor.execute("SELECT COALESCE(MAX(loan_id), 100) + 1 FROM loan_applications")
        test_id = cursor.fetchone()[0]
        
        print("\n" + "="*70)
        print("  CDC Testing: INSERT -> UPDATE -> DELETE")
        print("="*70)
        
        # ============ TEST 1: INSERT ============
        print(f"\n[1] INSERT: Creating loan_id={test_id}")
        cursor.execute("""
            INSERT INTO loan_applications (
                loan_id, applicant_name, applicant_email, loan_amount, 
                interest_rate, loan_term_months, application_date, application_status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            test_id,
            f'CDC Test User {test_id}',
            f'cdc_test_{test_id}@example.com',
            50000.00,
            5.5,
            36,
            datetime.now(),
            'pending'
        ))
        print(f"    -> Inserted: loan_id={test_id}, status='pending', amount=$50,000")
        time.sleep(2)
        
        # ============ TEST 2: UPDATE ============
        print(f"\n[2] UPDATE: Approving loan_id={test_id}")
        cursor.execute("""
            UPDATE loan_applications 
            SET application_status = 'approved',
                loan_amount = 45000.00,
                approval_date = %s
            WHERE loan_id = %s
        """, (datetime.now(), test_id))
        print(f"    -> Updated: status='approved', amount=$45,000")
        time.sleep(2)
        
        # ============ TEST 3: UPDATE AGAIN ============
        print(f"\n[3] UPDATE: Disbursing loan_id={test_id}")
        cursor.execute("""
            UPDATE loan_applications 
            SET application_status = 'disbursed',
                disbursement_date = %s
            WHERE loan_id = %s
        """, (datetime.now(), test_id))
        print(f"    -> Updated: status='disbursed'")
        time.sleep(2)
        
        # ============ TEST 4: DELETE ============
        print(f"\n[4] DELETE: Removing loan_id={test_id}")
        cursor.execute("DELETE FROM loan_applications WHERE loan_id = %s", (test_id,))
        print(f"    -> Deleted: loan_id={test_id}")
        
        print("\n" + "="*70)
        print("  All CDC operations completed!")
        print("="*70)
        print(f"\nCheck CDC pipeline:")
        print(f"  1. Kafka UI: http://localhost:8080")
        print(f"     Topic: cdc.syniq_postgres.public.loan_applications")
        print(f"     Expected: 4 messages (1 INSERT + 2 UPDATE + 1 DELETE)")
        print(f"\n  2. CDC Consumer logs:")
        print(f"     docker logs syniqai-cdc-consumer --tail 50")
        print(f"\n  3. MinIO: http://localhost:9001")
        print(f"     Bucket: bronze -> iceberg -> postgres_loan_applications")
        print(f"\nTest loan_id: {test_id}")
        print()
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\nError: {e}\n")

if __name__ == "__main__":
    test_cdc_operations()
