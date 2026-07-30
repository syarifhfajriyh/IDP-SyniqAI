#!/usr/bin/env python3
"""
Trigger a CDC event by inserting a test record into loan_applications table.
"""
import psycopg2
import sys
from datetime import datetime

# Database credentials (from .env)
DB_CONFIG = {
    'host': '192.168.2.114',
    'port': 5432,
    'database': 'syniq_postgres',
    'user': 'postgres',
    'password': 'postgres'
}

def insert_test_loan():
    """Insert a test loan application record"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Get next available loan_id
        cursor.execute("SELECT COALESCE(MAX(loan_id), 100) + 1 FROM loan_applications")
        next_id = cursor.fetchone()[0]
        
        # Insert test record
        insert_sql = """
        INSERT INTO loan_applications (
            loan_id, 
            applicant_name, 
            loan_amount, 
            interest_rate, 
            loan_term_months, 
            application_date, 
            status
        ) VALUES (
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s
        )
        """
        
        cursor.execute(insert_sql, (
            next_id,
            f'Test Applicant {next_id}',
            50000.00,
            5.5,
            36,
            datetime.now().date(),
            'pending'
        ))
        
        conn.commit()
        
        print(f"✓ Successfully inserted loan_id={next_id}")
        print(f"  Name: Test Applicant {next_id}")
        print(f"  Amount: $50,000.00")
        print(f"  Status: pending")
        print(f"\n→ Check Kafka UI for CDC event: cdc.syniq_postgres.public.loan_applications")
        
        cursor.close()
        conn.close()
        
        return next_id
        
    except Exception as e:
        print(f"✗ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    insert_test_loan()
