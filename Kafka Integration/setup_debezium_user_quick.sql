-- =====================================================
-- Quick Setup: Create Debezium User for CDC
-- =====================================================
-- Run this on PostgreSQL server (192.168.2.114)
-- Usage: psql -U postgres -d postgres -f setup_debezium_user_quick.sql
-- =====================================================

-- 1. Create debezium user with REPLICATION privilege
DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'debezium_user') THEN
        -- User exists, update password
        ALTER ROLE debezium_user WITH PASSWORD 'debezium_password';
        RAISE NOTICE '✓ Updated password for debezium_user';
    ELSE
        -- Create new user
        CREATE ROLE debezium_user WITH REPLICATION LOGIN PASSWORD 'debezium_password';
        RAISE NOTICE '✓ Created debezium_user';
    END IF;
END
$$;

-- 2. Grant permissions on public schema
GRANT USAGE ON SCHEMA public TO debezium_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium_user;

-- 3. Grant specific table permissions
GRANT SELECT ON public.loan_applications TO debezium_user;
GRANT SELECT ON public.hosp_raya_patient_record TO debezium_user;
GRANT SELECT ON public.cdc_test TO debezium_user;

-- 4. Create publication for CDC (if not exists)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'dbz_syniq_postgres_publication') THEN
        CREATE PUBLICATION dbz_syniq_postgres_publication FOR ALL TABLES;
        RAISE NOTICE '✓ Created publication: dbz_syniq_postgres_publication';
    ELSE
        RAISE NOTICE '✓ Publication already exists: dbz_syniq_postgres_publication';
    END IF;
END
$$;

-- 5. Verify setup
\echo '=========================================='
\echo 'Verification:'
\echo '=========================================='

SELECT 
    rolname AS username,
    rolreplication AS has_replication,
    rolcanlogin AS can_login
FROM pg_roles 
WHERE rolname = 'debezium_user';

SELECT pubname, puballtables 
FROM pg_publication 
WHERE pubname = 'dbz_syniq_postgres_publication';

\echo '=========================================='
\echo '✓ Setup complete!'
\echo 'You can now restart the Debezium connector'
\echo '=========================================='
