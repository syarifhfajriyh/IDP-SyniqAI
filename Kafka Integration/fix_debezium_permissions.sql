-- Grant additional permissions for Debezium to query database metadata
-- Run this on PostgreSQL server (192.168.2.114)

-- Connect to postgres database
\c postgres

-- Grant permission to query pg_database (needed for encoding check)
GRANT SELECT ON pg_database TO debezium_user;

-- Grant permission to query system catalogs
GRANT SELECT ON pg_catalog.pg_database TO debezium_user;

-- Verify permissions
SELECT 
    grantee,
    table_schema,
    table_name,
    privilege_type
FROM information_schema.role_table_grants
WHERE grantee = 'debezium_user'
AND table_name = 'pg_database';

\echo '✓ Permissions granted for database metadata queries'
