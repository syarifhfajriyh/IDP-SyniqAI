-- =====================================================
-- CDC UPDATE Test - Run this on PostgreSQL laptop
-- =====================================================
-- Copy and execute these queries in pgAdmin or psql
-- =====================================================

-- STEP 1: Check existing loan applications
SELECT loan_id, applicant_name, loan_amount, application_status 
FROM loan_applications 
ORDER BY loan_id DESC 
LIMIT 5;

-- STEP 2: UPDATE a loan application (change any existing loan_id)
-- Change loan_id=101 to any existing ID from the result above
UPDATE loan_applications 
SET 
    application_status = 'approved',
    loan_amount = 60000.00,
    approval_date = NOW(),
    updated_at = NOW()
WHERE loan_id = 101;

-- STEP 3: Verify the update
SELECT loan_id, applicant_name, loan_amount, application_status, approval_date
FROM loan_applications 
WHERE loan_id = 101;

-- =====================================================
-- After running this update:
-- 1. Wait 5-10 seconds
-- 2. Check Kafka UI: http://localhost:8080
--    - Look for topic: cdc.syniq_postgres.public.loan_applications
--    - You should see a new message with op='u' (update)
--
-- 3. Check MinIO: http://localhost:9001
--    - Login: admin / password123
--    - Browse: bronze/iceberg/bronze/postgres_loan_applications/
--    - Look for new .parquet files
-- =====================================================
