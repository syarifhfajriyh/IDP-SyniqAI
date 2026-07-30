#!/usr/bin/env python3
"""
Insert a test row into loan_applications to trigger CDC event
"""
import psycopg2
import sys
from datetime import datetime

# Connection details
DB_HOST = "192.168.2.114"
DB_PORT = 5432
DB_NAME = "postgres"
DB_USER = "debezium_user"
DB_PASS = "debezium_password"

def main():
    try:
        print("🔍 Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cursor = conn.cursor()
        
        print("✓ Connected!")
        print("\n📝 Inserting new loan application to trigger CDC event...")
        
        cursor.execute("""
            INSERT INTO public.loan_applications 
            (application_date, applicant_name, applicant_email, applicant_ssn, 
             phone_number, date_of_birth, annual_income, employment_status, 
             employer_name, loan_amount, loan_purpose, loan_term_months, 
             interest_rate, application_status, credit_score, 
             debt_to_income_ratio, previous_loans, risk_category, 
             fraud_flag, manual_review_required, created_by, notes)
            VALUES 
            (NOW(), 'Test CDC User', 'test.cdc@email.com', '999-99-9999',
             '555-9999', '1990-01-01', 75000.00, 'Full-time',
             'CDC Test Company', 20000.00, 'Testing CDC', 36,
             6.50, 'PENDING', 700,
             0.30, 0, 'MEDIUM',
             FALSE, FALSE, 'CDC_TEST', 'Test row to verify CDC is working')
            RETURNING loan_id;
        """)
        
        new_loan_id = cursor.fetchone()[0]
        conn.commit()
        
        print(f"✅ New loan application inserted! loan_id: {new_loan_id}")
        print(f"\n🎯 CDC Event Should Now Flow:")
        print(f"   PostgreSQL (loan_id={new_loan_id}) → Debezium → Kafka → Spark → Iceberg")
        print(f"\n📌 Check Kafka UI in 5-10 seconds:")
        print(f"   Topic: cdc.syniq_postgres.public.loan_applications")
        print(f"   Expected: 1 new message with loan_id={new_loan_id}")
        
        cursor.close()
        conn.close()
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
