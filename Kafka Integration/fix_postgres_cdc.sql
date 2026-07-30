-- PostgreSQL CDC Configuration Fix
-- Run this on the remote database server (192.168.2.114) as postgres user

-- 1. Check currentWAL level
SHOW wal_level;
-- Should be 'logical', if not, continue with fixes below

-- 2. Set WAL level to logical (requires restart)
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_wal_senders = 10;

-- 3. Create test table if not exists
CREATE TABLE IF NOT EXISTS public.cdc_test (
    id SERIAL PRIMARY KEY,
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Grant permissions to debezium user
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;
GRANT SELECT ON public.cdc_test TO debezium_user;

-- 5. Verify configuration
SHOW wal_level;
SELECT * FROM pg_replication_slots;

-- After running this script, RESTART PostgreSQL
-- Windows: Restart-Service postgresql-x64-15
-- Linux: sudo systemctl restart postgresql
