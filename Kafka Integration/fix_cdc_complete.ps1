# CDC Pipeline Complete Fix
# Fixes all blocking issues for CDC pipeline

$ErrorActionPreference = "Continue"

Write-Host ""
Write-Host ("=" * 80) -ForegroundColor Cyan
Write-Host "          CDC PIPELINE COMPLETE FIX" -ForegroundColor Cyan
Write-Host ("=" * 80) -ForegroundColor Cyan
Write-Host ""

# Step 1: Restart Docker with Persistent Debezium
Write-Host "[1/4] Restarting Kafka with persistent Debezium connectors..." -ForegroundColor Yellow
Write-Host ""

docker-compose down
Write-Host "    Stopped containers" -ForegroundColor Gray

docker-compose up -d
Write-Host "    Started containers with new configuration" -ForegroundColor Gray
Write-Host ""

Write-Host "    Waiting for Kafka Connect to start (120 seconds)..." -ForegroundColor Yellow
$maxAttempts = 24
$attempt = 0
$ready = $false

while ($attempt -lt $maxAttempts -and -not $ready) {
    Start-Sleep -Seconds 5
    $attempt++
    
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8083/" -Method Get -TimeoutSec 2 -UseBasicParsing -ErrorAction SilentlyContinue
        if ($response.StatusCode -eq 200) {
            $ready = $true
            Write-Host "    [OK] Kafka Connect is ready!" -ForegroundColor Green
        }
    } catch {
        Write-Host "    Waiting... ($attempt/$maxAttempts)" -ForegroundColor Gray
    }
}

if (-not $ready) {
    Write-Host "    [ERROR] Kafka Connect did not start" -ForegroundColor Red
    Write-Host "    Check logs: docker logs syniqai-kafka-connect" -ForegroundColor Yellow
    exit 1
}

Write-Host ""

# Verify Debezium connectors
Write-Host "    Verifying Debezium connectors..." -ForegroundColor Yellow
try {
    $plugins = Invoke-RestMethod -Uri "http://localhost:8083/connector-plugins" -Method Get -UseBasicParsing
    $postgresPlugin = $plugins | Where-Object { $_.class -like "*PostgresConnector*" }
    $mysqlPlugin = $plugins | Where-Object { $_.class -like "*MySqlConnector*" }
    
    if ($postgresPlugin -and $mysqlPlugin) {
        Write-Host "    [OK] Debezium connectors verified!" -ForegroundColor Green
        Write-Host "        - PostgreSQL: $($postgresPlugin.class)" -ForegroundColor Gray
        Write-Host "        - MySQL: $($mysqlPlugin.class)" -ForegroundColor Gray
    } else {
        Write-Host "    [ERROR] Debezium connectors not found" -ForegroundColor Red
        Write-Host "    Check startup logs: docker logs syniqai-kafka-connect" -ForegroundColor Yellow
        exit 1
    }
} catch {
    Write-Host "    [ERROR] Failed to verify connectors: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

Write-Host ""

# Step 2: Create PostgreSQL Fix Script
Write-Host "[2/4] Creating PostgreSQL configuration fix script..." -ForegroundColor Yellow
Write-Host ""

$pgFixScript = @"
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
"@

$pgFixFile = "fix_postgres_cdc.sql"
$pgFixScript | Out-File -FilePath $pgFixFile -Encoding UTF8
Write-Host "    [OK] Created: $pgFixFile" -ForegroundColor Green
Write-Host ""

# Step 3: Create MariaDB Fix Script
Write-Host "[3/4] Creating MariaDB configuration fix script..." -ForegroundColor Yellow
Write-Host ""

$mysqlFixScript = @"
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
"@

$mysqlFixFile = "fix_mariadb_cdc.sql"
$mysqlFixScript | Out-File -FilePath $mysqlFixFile -Encoding UTF8
Write-Host "    [OK] Created: $mysqlFixFile" -ForegroundColor Green
Write-Host ""

# Step 4: Test Remote Database Connectivity
Write-Host "[4/4] Testing database connectivity..." -ForegroundColor Yellow
Write-Host ""

$REMOTE_HOST = "192.168.2.114"

# Test PostgreSQL
Write-Host "    Testing PostgreSQL..." -ForegroundColor Cyan
$pgTest = Test-NetConnection -ComputerName $REMOTE_HOST -Port 5432 -WarningAction SilentlyContinue
if ($pgTest.TcpTestSucceeded) {
    Write-Host "    [OK] PostgreSQL port 5432 accessible" -ForegroundColor Green
} else {
    Write-Host "    [FAIL] PostgreSQL port 5432 not accessible" -ForegroundColor Red
}

# Test MariaDB
Write-Host "    Testing MariaDB..." -ForegroundColor Cyan
$mysqlTest = Test-NetConnection -ComputerName $REMOTE_HOST -Port 3306 -WarningAction SilentlyContinue
if ($mysqlTest.TcpTestSucceeded) {
    Write-Host "    [OK] MariaDB port 3306 accessible" -ForegroundColor Green
} else {
    Write-Host "    [FAIL] MariaDB port 3306 not accessible" -ForegroundColor Red
}

Write-Host ""

# Summary
Write-Host ("=" * 80) -ForegroundColor Green
Write-Host "          SETUP PHASE COMPLETE!" -ForegroundColor Green
Write-Host ("=" * 80) -ForegroundColor Green
Write-Host ""
Write-Host "What was fixed:" -ForegroundColor Cyan
Write-Host "  * Debezium connectors installed with persistent storage" -ForegroundColor Green
Write-Host "  * Docker will remember connectors across restarts" -ForegroundColor Green
Write-Host "  * Database fix scripts created" -ForegroundColor Green
Write-Host ""
Write-Host "Next Steps (on remote server 192.168.2.114):" -ForegroundColor Yellow
Write-Host ""
Write-Host "1. Copy files to remote server:" -ForegroundColor White
Write-Host "   - $pgFixFile" -ForegroundColor Gray
Write-Host "   - $mysqlFixFile" -ForegroundColor Gray
Write-Host ""
Write-Host "2. Run PostgreSQL fix:" -ForegroundColor White
Write-Host "   psql -U postgres -f $pgFixFile" -ForegroundColor Yellow
Write-Host "   Restart-Service postgresql-x64-*" -ForegroundColor Yellow
Write-Host ""
Write-Host "3. Run MariaDB fix:" -ForegroundColor White
Write-Host "   mysql -u root -p" -ForegroundColor Yellow
Write-Host "   source $mysqlFixFile" -ForegroundColor Yellow
Write-Host ""
Write-Host "4. After database configuration complete:" -ForegroundColor White
Write-Host "   Run from THIS laptop: .\start_cdc_pipeline.ps1" -ForegroundColor Yellow
Write-Host ""
Write-Host "Services Running:" -ForegroundColor Cyan
Write-Host "  - Kafka Broker:    localhost:9092" -ForegroundColor Gray
Write-Host "  - Kafka Connect:   http://localhost:8083" -ForegroundColor Gray
Write-Host "  - Kafka UI:        http://localhost:8080" -ForegroundColor Gray
Write-Host ""
Write-Host "Troubleshooting:" -ForegroundColor Cyan
Write-Host "  - Kafka Connect logs: docker logs syniqai-kafka-connect" -ForegroundColor Gray
Write-Host "  - Test connectivity: .\test_database_connectivity.ps1" -ForegroundColor Gray
Write-Host "  - List connectors: curl http://localhost:8083/connector-plugins" -ForegroundColor Gray
Write-Host ""
Write-Host ("=" * 80) -ForegroundColor Cyan
Write-Host ""
