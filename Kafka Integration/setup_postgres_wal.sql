-- =====================================================
-- PostgreSQL WAL Configuration for Debezium CDC
-- =====================================================
-- This script enables Write-Ahead Logging (WAL) and creates
-- a replication user for Debezium to consume database changes.
--
-- Prerequisites:
-- - PostgreSQL 10+ (preferably 12+)
-- - Superuser access
-- - Restart required after configuration changes
--
-- Usage:
--   psql -U postgres -d postgres -f setup_postgres_wal.sql
-- =====================================================

-- Step 1: Check current WAL configuration
SELECT name, setting 
FROM pg_settings 
WHERE name IN ('wal_level', 'max_wal_senders', 'max_replication_slots');

-- Expected values for CDC:
--   wal_level = 'logical'
--   max_wal_senders >= 4
--   max_replication_slots >= 4

-- =====================================================
-- MANUAL CONFIGURATION REQUIRED
-- =====================================================
-- If values above are not correct, you MUST edit postgresql.conf
-- and restart PostgreSQL. Add these lines:
--
-- wal_level = logical
-- max_wal_senders = 10
-- max_replication_slots = 10
-- wal_keep_size = 1024  # PostgreSQL 13+ (1GB)
-- # OR for PostgreSQL 12 and earlier:
-- # wal_keep_segments = 64  # (1GB if wal_segment_size=16MB)
--
-- After editing postgresql.conf, restart PostgreSQL:
--   Windows: Restart PostgreSQL service from Services
--   Linux: sudo systemctl restart postgresql
-- =====================================================

-- Step 2: Create replication user for Debezium
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'debezium_user') THEN
        CREATE ROLE debezium_user WITH REPLICATION LOGIN PASSWORD 'debezium_password';
        RAISE NOTICE 'Created replication user: debezium_user';
    ELSE
        RAISE NOTICE 'Replication user debezium_user already exists';
    END IF;
END
$$;

-- Grant necessary permissions
GRANT USAGE ON SCHEMA public TO debezium_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium_user;

-- Step 3: Create publication for all tables
-- This is required for pgoutput logical decoding plugin
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'dbz_publication') THEN
        CREATE PUBLICATION dbz_publication FOR ALL TABLES;
        RAISE NOTICE 'Created publication: dbz_publication';
    ELSE
        RAISE NOTICE 'Publication dbz_publication already exists';
    END IF;
END
$$;

-- View created publication
SELECT * FROM pg_publication WHERE pubname = 'dbz_publication';

-- Step 4: Create a test table to verify CDC setup
CREATE TABLE IF NOT EXISTS public.cdc_test (
    id SERIAL PRIMARY KEY,
    message TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert test data
INSERT INTO public.cdc_test (message) 
VALUES ('CDC Test Event 1'), ('CDC Test Event 2');

-- Grant access to debezium user
GRANT SELECT ON public.cdc_test TO debezium_user;
GRANT SELECT ON SEQUENCE public.cdc_test_id_seq TO debezium_user;

-- Step 5: Verify replication slot can be created
-- (Run this after Debezium connector is configured)
-- SELECT * FROM pg_replication_slots;

-- Step 6: Monitor active replication
-- SELECT * FROM pg_stat_replication;

-- =====================================================
-- Health Check Queries
-- =====================================================

-- Check WAL configuration
SELECT 
    'wal_level' as parameter,
    setting as value,
    CASE 
        WHEN setting = 'logical' THEN 'OK'
        ELSE 'NEEDS UPDATE'
    END as status
FROM pg_settings 
WHERE name = 'wal_level'
UNION ALL
SELECT 
    'max_wal_senders',
    setting,
    CASE 
        WHEN setting::int >= 4 THEN 'OK'
        ELSE 'NEEDS UPDATE'
    END
FROM pg_settings 
WHERE name = 'max_wal_senders'
UNION ALL
SELECT 
    'max_replication_slots',
    setting,
    CASE 
        WHEN setting::int >= 4 THEN 'OK'
        ELSE 'NEEDS UPDATE'
    END
FROM pg_settings 
WHERE name = 'max_replication_slots';

-- Check replication user
SELECT 
    rolname as username,
    rolreplication as has_replication,
    rolcanlogin as can_login
FROM pg_roles 
WHERE rolname = 'debezium_user';

-- Check publications
SELECT 
    pubname as publication_name,
    puballtables as all_tables,
    (SELECT count(*) FROM pg_publication_tables WHERE pubname = p.pubname) as table_count
FROM pg_publication p
WHERE pubname = 'dbz_publication';

-- =====================================================
-- Cleanup (if needed)
-- =====================================================

-- To remove everything:
-- DROP PUBLICATION IF EXISTS dbz_publication;
-- DROP TABLE IF EXISTS public.cdc_test;
-- REVOKE ALL ON SCHEMA public FROM debezium_user;
-- DROP ROLE IF EXISTS debezium_user;

-- To remove replication slot (if connector is deleted):
-- SELECT pg_drop_replication_slot('debezium_slot_name');

-- =====================================================
-- Next Steps
-- =====================================================
-- 1. Verify all health checks pass
-- 2. Restart PostgreSQL if configuration changes were made
-- 3. Run the Debezium connector setup from Python:
--    python setup_cdc_connectors.py --source postgres
-- 4. Monitor replication lag:
--    SELECT * FROM pg_replication_slots;
--    SELECT * FROM pg_stat_replication;
-- =====================================================

\echo ''
\echo '========================================='
\echo 'PostgreSQL WAL Setup Complete!'
\echo '========================================='
\echo 'IMPORTANT: If you modified postgresql.conf,'
\echo 'you MUST restart PostgreSQL for changes to take effect.'
\echo ''
\echo 'Run health check queries above to verify setup.'
\echo '========================================='
