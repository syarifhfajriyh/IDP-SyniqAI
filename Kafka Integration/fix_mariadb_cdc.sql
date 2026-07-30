-- MariaDB CDC Configuration Fix
-- Run this on the remote database server (192.168.2.114) as root user

-- 1. Check binlog status
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'server_id';

-- 2. Create test table if not exists
CREATE DATABASE IF NOT EXISTS mysql;
USE mysql;

CREATE TABLE IF NOT EXISTS test_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    message VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Grant permissions to debezium user
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium_user'@'%';
FLUSH PRIVILEGES;

-- 4. Verify configuration
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
SHOW MASTER STATUS;

-- If log_bin or binlog_format are not correct:
-- 1. Edit my.ini: log-bin=mysql-bin, binlog_format=ROW, server-id=1
-- 2. Restart MariaDB: Restart-Service MySQL
