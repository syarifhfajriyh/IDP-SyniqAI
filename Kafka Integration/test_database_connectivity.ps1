#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Test Database Connectivity for CDC Pipeline

.DESCRIPTION
    Tests network connectivity to remote database server
    Run this on your CURRENT laptop to verify connectivity to 192.168.2.114
#>

$REMOTE_HOST = "192.168.2.114"
$POSTGRES_PORT = 5432
$MYSQL_PORT = 3306

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "          CDC DATABASE CONNECTIVITY TEST" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Testing connectivity to: $REMOTE_HOST" -ForegroundColor Yellow
Write-Host ""

# Test 1: Ping
Write-Host "[1/5] Testing network reachability..." -ForegroundColor Yellow
try {
    $ping = Test-Connection -ComputerName $REMOTE_HOST -Count 2 -ErrorAction Stop
    $avgTime = ($ping | Measure-Object -Property ResponseTime -Average).Average
    Write-Host "    [OK] Host is reachable (avg: $([math]::Round($avgTime, 2))ms)" -ForegroundColor Green
} catch {
    Write-Host "    [FAIL] Cannot reach host: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "    Check network connection and firewall settings" -ForegroundColor Yellow
}
Write-Host ""

# Test 2: PostgreSQL Port
Write-Host "[2/5] Testing PostgreSQL port ($POSTGRES_PORT)..." -ForegroundColor Yellow
try {
    $pgTest = Test-NetConnection -ComputerName $REMOTE_HOST -Port $POSTGRES_PORT -WarningAction SilentlyContinue
    if ($pgTest.TcpTestSucceeded) {
        Write-Host "    [OK] PostgreSQL port is open" -ForegroundColor Green
    } else {
        Write-Host "    [FAIL] PostgreSQL port is closed or filtered" -ForegroundColor Red
        Write-Host "    Action: Configure PostgreSQL and firewall on remote machine" -ForegroundColor Yellow
    }
} catch {
    Write-Host "    [FAIL] Cannot test port: $($_.Exception.Message)" -ForegroundColor Red
}
Write-Host ""

# Test 3: MariaDB Port
Write-Host "[3/5] Testing MariaDB port ($MYSQL_PORT)..." -ForegroundColor Yellow
try {
    $mysqlTest = Test-NetConnection -ComputerName $REMOTE_HOST -Port $MYSQL_PORT -WarningAction SilentlyContinue
    if ($mysqlTest.TcpTestSucceeded) {
        Write-Host "    [OK] MariaDB port is open" -ForegroundColor Green
    } else {
        Write-Host "    [FAIL] MariaDB port is closed or filtered" -ForegroundColor Red
        Write-Host "    Action: Configure MariaDB and firewall on remote machine" -ForegroundColor Yellow
    }
} catch {
    Write-Host "    [FAIL] Cannot test port: $($_.Exception.Message)" -ForegroundColor Red
}
Write-Host ""

# Test 4: Docker Container Network
Write-Host "[4/5] Testing from Docker container..." -ForegroundColor Yellow
$containerRunning = docker ps --filter "name=syniqai-kafka-connect" --format "{{.Names}}" 2>$null

if ($containerRunning) {
    Write-Host "    Testing PostgreSQL from container..." -ForegroundColor Cyan
    $pgDockerTest = docker exec syniqai-kafka-connect timeout 3 bash -c "cat < /dev/null > /dev/tcp/$REMOTE_HOST/$POSTGRES_PORT" 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "    [OK] Container can reach PostgreSQL" -ForegroundColor Green
    } else {
        Write-Host "    [FAIL] Container cannot reach PostgreSQL" -ForegroundColor Red
    }
    
    Write-Host "    Testing MariaDB from container..." -ForegroundColor Cyan
    $mysqlDockerTest = docker exec syniqai-kafka-connect timeout 3 bash -c "cat < /dev/null > /dev/tcp/$REMOTE_HOST/$MYSQL_PORT" 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "    [OK] Container can reach MariaDB" -ForegroundColor Green
    } else {
        Write-Host "    [FAIL] Container cannot reach MariaDB" -ForegroundColor Red
    }
} else {
    Write-Host "    [SKIP] Kafka Connect container not running" -ForegroundColor Yellow
}
Write-Host ""

# Test 5: Database Authentication (optional)
Write-Host "[5/5] Testing database authentication..." -ForegroundColor Yellow
$testAuth = Read-Host "    Test database login? This requires psql/mysql installed (y/n)"

if ($testAuth -eq "y") {
    Write-Host ""
    Write-Host "    Testing PostgreSQL authentication..." -ForegroundColor Cyan
    $pgCommand = "psql -h $REMOTE_HOST -U debezium_user -d postgres -c 'SELECT version();'"
    Write-Host "    Running: $pgCommand" -ForegroundColor Gray
    Write-Host "    (Enter password: debezium_password when prompted)" -ForegroundColor Gray
    try {
        $env:PGPASSWORD = "debezium_password"
        & psql.exe -h $REMOTE_HOST -U debezium_user -d postgres -c "SELECT version();" 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "    [OK] PostgreSQL authentication successful" -ForegroundColor Green
        } else {
            Write-Host "    [FAIL] PostgreSQL authentication failed" -ForegroundColor Red
        }
        Remove-Item Env:\PGPASSWORD
    } catch {
        Write-Host "    [SKIP] psql not found: $($_.Exception.Message)" -ForegroundColor Yellow
    }
    
    Write-Host ""
    Write-Host "    Testing MariaDB authentication..." -ForegroundColor Cyan
    $mysqlCommand = "mysql -h $REMOTE_HOST -u debezium_user -p -e 'SELECT VERSION();'"
    Write-Host "    Running: $mysqlCommand" -ForegroundColor Gray
    Write-Host "    (Enter password: debezium_password when prompted)" -ForegroundColor Gray
    try {
        & mysql.exe -h $REMOTE_HOST -u debezium_user -pdebezium_password -e "SELECT VERSION();" 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "    [OK] MariaDB authentication successful" -ForegroundColor Green
        } else {
            Write-Host "    [FAIL] MariaDB authentication failed" -ForegroundColor Red
        }
    } catch {
        Write-Host "    [SKIP] mysql not found: $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "          TEST SUMMARY" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "If any tests failed:" -ForegroundColor Yellow
Write-Host "  1. Run configure_remote_databases.ps1 ON THE REMOTE MACHINE (192.168.2.114)" -ForegroundColor White
Write-Host "  2. Edit configuration files as instructed" -ForegroundColor White
Write-Host "  3. Restart database services" -ForegroundColor White
Write-Host "  4. Re-run this test script" -ForegroundColor White
Write-Host ""
Write-Host "If all tests passed:" -ForegroundColor Green
Write-Host "  You're ready to run: .\start_cdc_pipeline.ps1" -ForegroundColor Cyan
Write-Host ""
