-- =====================================================
-- MariaDB Binlog Configuration for Debezium CDC
-- =====================================================
-- This script enables binary logging (binlog) and creates
-- a replication user for Debezium to consume database changes.
--
-- Prerequisites:
-- - MariaDB 10.2+ (preferably 10.5+)
-- - Root/admin access
-- - Restart required after configuration changes
--
-- Usage:
--   mysql -u root -p < setup_mariadb_binlog.sql
-- =====================================================

-- Step 1: Check current binlog configuration
SELECT 
    @@log_bin AS log_bin_enabled,
    @@binlog_format AS binlog_format,
    @@binlog_row_image AS binlog_row_image,
    @@server_id AS server_id,
    @@gtid_strict_mode AS gtid_strict_mode;

-- Expected values for CDC:
--   log_bin_enabled = 1 (ON)
--   binlog_format = 'ROW'
--   binlog_row_image = 'FULL'
--   server_id = <any unique non-zero value>
--   gtid_strict_mode = 1 (recommended)

-- =====================================================
-- MANUAL CONFIGURATION REQUIRED
-- =====================================================
-- If values above are not correct, you MUST edit my.cnf or my.ini
-- and restart MariaDB. Add these lines under [mysqld] section:
--
-- [mysqld]
-- server-id=1
-- log-bin=mysql-bin
-- binlog-format=ROW
-- binlog-row-image=FULL
-- expire_logs_days=7
-- max_binlog_size=100M
-- gtid_strict_mode=ON
--
-- File locations:
--   Linux: /etc/mysql/my.cnf or /etc/my.cnf
--   Windows: C:\Program Files\MariaDB\data\my.ini
--
-- After editing my.cnf/my.ini, restart MariaDB:
--   Windows: Restart MariaDB service from Services
--   Linux: sudo systemctl restart mariadb
-- =====================================================

-- Step 2: Create replication user for Debezium
CREATE USER IF NOT EXISTS 'debezium_user'@'%' 
IDENTIFIED BY 'debezium_password';

-- Grant necessary privileges
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT 
ON *.* TO 'debezium_user'@'%';

-- For specific database (adjust as needed)
GRANT SELECT ON mysql.* TO 'debezium_user'@'%';

-- Apply changes
FLUSH PRIVILEGES;

-- Step 3: Create test table to verify CDC setup
CREATE DATABASE IF NOT EXISTS test_cdc;
USE test_cdc;

CREATE TABLE IF NOT EXISTS cdc_test (
    id INT AUTO_INCREMENT PRIMARY KEY,
    message VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Insert test data
INSERT INTO cdc_test (message) 
VALUES ('CDC Test Event 1'), ('CDC Test Event 2');

-- Grant access to debezium user
GRANT SELECT ON test_cdc.* TO 'debezium_user'@'%';
FLUSH PRIVILEGES;

-- Step 4: Verify binlog files exist
SHOW BINARY LOGS;

-- Step 5: View current binlog position
SHOW MASTER STATUS;

-- Step 6: Show binlog events (recent changes)
SHOW BINLOG EVENTS LIMIT 10;

-- =====================================================
-- Health Check Queries
-- =====================================================

-- Check binlog configuration
SELECT 
    'log_bin' as parameter,
    @@log_bin as value,
    CASE 
        WHEN @@log_bin = 1 THEN 'OK'
        ELSE 'NEEDS UPDATE - Restart required'
    END as status
UNION ALL
SELECT 
    'binlog_format',
    @@binlog_format,
    CASE 
        WHEN @@binlog_format = 'ROW' THEN 'OK'
        ELSE 'NEEDS UPDATE - Restart required'
    END
UNION ALL
SELECT 
    'binlog_row_image',
    @@binlog_row_image,
    CASE 
        WHEN @@binlog_row_image = 'FULL' THEN 'OK'
        ELSE 'NEEDS UPDATE - Restart required'
    END
UNION ALL
SELECT 
    'server_id',
    CAST(@@server_id AS CHAR),
    CASE 
        WHEN @@server_id > 0 THEN 'OK'
        ELSE 'NEEDS UPDATE - Restart required'
    END;

-- Check replication user
SELECT 
    User as username,
    Host as host,
    Select_priv,
    Repl_slave_priv,
    Repl_client_priv
FROM mysql.user 
WHERE User = 'debezium_user';

-- Check binlog files and size
SELECT 
    COUNT(*) as binlog_count,
    ROUND(SUM(File_size)/1024/1024, 2) as total_size_mb
FROM information_schema.BINARY_LOGS;

-- Check database and tables
SELECT 
    table_schema as database_name,
    table_name,
    engine,
    table_rows
FROM information_schema.tables
WHERE table_schema = 'test_cdc';

-- =====================================================
-- Monitoring Queries
-- =====================================================

-- Monitor binlog growth
SELECT 
    Log_name,
    ROUND(File_size/1024/1024, 2) as size_mb
FROM information_schema.BINARY_LOGS
ORDER BY Log_name DESC
LIMIT 10;

-- View recent binlog events
-- SHOW BINLOG EVENTS IN 'mysql-bin.000001' LIMIT 20;

-- Check binary log status
SHOW MASTER STATUS\G

-- =====================================================
-- Troubleshooting
-- =====================================================

-- If binlog is not enabled, check if log_bin is dynamic
-- (Most binlog settings require restart)
SELECT 
    variable_name,
    variable_value,
    global_value_origin
FROM information_schema.system_variables
WHERE variable_name IN ('log_bin', 'binlog_format', 'binlog_row_image');

-- Test binlog by making a change
INSERT INTO test_cdc.cdc_test (message) VALUES ('Test insert for binlog');
-- Then check: SHOW MASTER STATUS;

-- =====================================================
-- Cleanup (if needed)
-- =====================================================

-- To remove everything:
-- DROP DATABASE IF EXISTS test_cdc;
-- DROP USER IF EXISTS 'debezium_user'@'%';
-- FLUSH PRIVILEGES;

-- To purge old binlog files (keep last N days):
-- PURGE BINARY LOGS BEFORE DATE_SUB(NOW(), INTERVAL 7 DAY);

-- =====================================================
-- Advanced Configuration (Optional)
-- =====================================================

-- Enable GTID mode for better replication tracking
-- Already set in my.cnf: gtid_strict_mode=ON

-- Set binlog retention
-- Already set in my.cnf: expire_logs_days=7

-- =====================================================
-- Next Steps
-- =====================================================
-- 1. Verify all health checks pass
-- 2. Restart MariaDB if configuration changes were made
-- 3. Verify binlog files exist: SHOW BINARY LOGS;
-- 4. Run the Debezium connector setup from Python:
--    python setup_cdc_connectors.py --source mariadb
-- 5. Monitor binlog position and replication lag
-- =====================================================

SELECT '';
SELECT '=========================================';
SELECT 'MariaDB Binlog Setup Complete!';
SELECT '=========================================';
SELECT 'IMPORTANT: If you modified my.cnf/my.ini,';
SELECT 'you MUST restart MariaDB for changes to take effect.';
SELECT '';
SELECT 'Run health check queries above to verify setup.';
SELECT '=========================================';
