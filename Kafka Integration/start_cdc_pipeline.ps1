# =============================================================================
# SYINIQ CDC Pipeline - Quick Start Script
# =============================================================================
# Automates the setup of the complete CDC pipeline:
# 1. Starts Kafka stack with Debezium connectors
# 2. Creates CDC connectors for PostgreSQL and MariaDB
# 3. Runs validation tests
# 4. Starts Spark Streaming consumer
# 5. Starts monitoring dashboard
#
# Prerequisites:
# - Docker Desktop running
# - PostgreSQL and MariaDB with WAL/binlog configured and restarted
# - Python 3.8+ installed
#
# Usage:
#   .\start_cdc_pipeline.ps1
# =============================================================================

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "          SYINIQ CDC Pipeline - Quick Start" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""

# Navigate to Kafka Integration directory
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptPath

Write-Host "[1/6] Checking Docker..." -ForegroundColor Yellow
$dockerRunning = docker ps 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "    [ERROR] Docker is not running!" -ForegroundColor Red
    Write-Host "    Please start Docker Desktop and try again." -ForegroundColor Red
    exit 1
}
Write-Host "    [OK] Docker is running" -ForegroundColor Green

Write-Host ""
Write-Host "[2/6] Starting Kafka Stack with Debezium..." -ForegroundColor Yellow
Write-Host "    Stopping existing containers..." -ForegroundColor Gray
docker-compose down 2>$null

Write-Host "    Starting services (this will take 2-3 minutes)..." -ForegroundColor Gray
docker-compose up -d

if ($LASTEXITCODE -ne 0) {
    Write-Host "    [ERROR] Failed to start Kafka stack!" -ForegroundColor Red
    exit 1
}

Write-Host "    Waiting for Kafka Connect to install Debezium connectors..." -ForegroundColor Gray
Write-Host "    This may take up to 2 minutes..." -ForegroundColor Gray

$maxRetries = 24  # 2 minutes with 5-second intervals
$retries = 0
$connectReady = $false

while (-not $connectReady -and $retries -lt $maxRetries) {
    Start-Sleep -Seconds 5
    $retries++
    
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:8083/" -Method Get -TimeoutSec 5 -ErrorAction SilentlyContinue
        if ($response) {
            $connectReady = $true
            Write-Host "    [OK] Kafka Connect is ready! Version: $($response.version)" -ForegroundColor Green
        }
    } catch {
        Write-Host "    Still waiting... ($retries/$maxRetries)" -ForegroundColor Gray
    }
}

if (-not $connectReady) {
    Write-Host "    [WARNING] Kafka Connect may not be ready yet" -ForegroundColor Yellow
    Write-Host "    You may need to wait a bit longer before running setup" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "[3/6] Verifying Debezium connectors installed..." -ForegroundColor Yellow

try {
    $plugins = Invoke-RestMethod -Uri "http://localhost:8083/connector-plugins" -Method Get -ErrorAction Stop
    $postgresPlugin = $plugins | Where-Object { $_.class -like "*PostgresConnector*" }
    $mysqlPlugin = $plugins | Where-Object { $_.class -like "*MySqlConnector*" }
    
    if ($postgresPlugin) {
        Write-Host "    [OK] PostgreSQL Debezium connector installed" -ForegroundColor Green
    } else {
        Write-Host "    [WARNING] PostgreSQL Debezium connector not found" -ForegroundColor Yellow
    }
    
    if ($mysqlPlugin) {
        Write-Host "    [OK] MySQL/MariaDB Debezium connector installed" -ForegroundColor Green
    } else {
        Write-Host "    [WARNING] MySQL/MariaDB Debezium connector not found" -ForegroundColor Yellow
    }
} catch {
    Write-Host "    [ERROR] Failed to check connector plugins: $_" -ForegroundColor Red
}

Write-Host ""
Write-Host "[4/6] Creating CDC Connectors..." -ForegroundColor Yellow
Write-Host "    NOTE: This assumes you've run the database setup scripts:" -ForegroundColor Gray
Write-Host "      - setup_postgres_wal.sql (and restarted PostgreSQL)" -ForegroundColor Gray
Write-Host "      - setup_mariadb_binlog.sql (and restarted MariaDB)" -ForegroundColor Gray
Write-Host ""

$createConnectors = Read-Host "    Create CDC connectors now? (y/n)"
if ($createConnectors -eq "y") {
    python setup_cdc_connectors.py
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "    [OK] CDC connectors created successfully!" -ForegroundColor Green
    } else {
        Write-Host "    [ERROR] Failed to create connectors" -ForegroundColor Red
        Write-Host "    Check the error messages above" -ForegroundColor Red
    }
} else {
    Write-Host "    [SKIP] Skipping connector creation" -ForegroundColor Yellow
    Write-Host "    You can create them later with: python setup_cdc_connectors.py" -ForegroundColor Gray
}

Write-Host ""
Write-Host "[5/6] Running Validation Tests..." -ForegroundColor Yellow
$runTests = Read-Host "    Run CDC pipeline tests? (y/n)"
if ($runTests -eq "y") {
    python test_cdc_pipeline.py
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "    [OK] All tests passed!" -ForegroundColor Green
    } else {
        Write-Host "    [WARNING] Some tests failed. Review the output above." -ForegroundColor Yellow
    }
} else {
    Write-Host "    [SKIP] Skipping tests" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "[6/6] Starting Services..." -ForegroundColor Yellow
Write-Host ""
Write-Host "You can now start the following services:" -ForegroundColor Cyan
Write-Host ""
Write-Host "  1. Spark Streaming Consumer (in new terminal):" -ForegroundColor White
Write-Host "     python spark_cdc_consumer.py" -ForegroundColor Gray
Write-Host ""
Write-Host "  2. Monitoring Dashboard (in new terminal):" -ForegroundColor White
Write-Host "     python cdc_monitor.py" -ForegroundColor Gray
Write-Host ""
Write-Host "  3. Kafka UI (in browser):" -ForegroundColor White
Write-Host "     http://localhost:8080" -ForegroundColor Gray
Write-Host ""

$startSparkConsumer = Read-Host "Start Spark Streaming consumer now? (y/n)"
if ($startSparkConsumer -eq "y") {
    Write-Host "    Starting Spark consumer in background..." -ForegroundColor Yellow
    Start-Process powershell -ArgumentList "-NoExit", "-Command", "python spark_cdc_consumer.py"
    Write-Host "    [OK] Spark consumer started in new window" -ForegroundColor Green
}

Write-Host ""
$startMonitor = Read-Host "Start monitoring dashboard? (y/n)"
if ($startMonitor -eq "y") {
    Write-Host "    Starting monitoring dashboard in background..." -ForegroundColor Yellow
    Start-Process powershell -ArgumentList "-NoExit", "-Command", "python cdc_monitor.py"
    Write-Host "    [OK] Monitor started in new window" -ForegroundColor Green
}

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Green
Write-Host "                    CDC Pipeline Setup Complete!" -ForegroundColor Green
Write-Host "================================================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Services Running:" -ForegroundColor White
Write-Host "  - Kafka Broker:        localhost:9092" -ForegroundColor Gray
Write-Host "  - Kafka Connect:       localhost:8083" -ForegroundColor Gray
Write-Host "  - Kafka UI:            http://localhost:8080" -ForegroundColor Gray
Write-Host "  - Spark Consumer:      Running (if started)" -ForegroundColor Gray
Write-Host "  - Monitor Dashboard:   Running (if started)" -ForegroundColor Gray
Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Cyan
Write-Host "  1. Open Kafka UI: http://localhost:8080" -ForegroundColor White
Write-Host "  2. Make a database change:" -ForegroundColor White
Write-Host "     psql -U postgres -c " -NoNewline -ForegroundColor Gray
Write-Host '"INSERT INTO public.cdc_test (message) VALUES (' -NoNewline -ForegroundColor Gray
Write-Host "'Test!'" -NoNewline -ForegroundColor Gray
Write-Host ');"' -ForegroundColor Gray
Write-Host "  3. Watch the CDC event flow through Kafka UI" -ForegroundColor White
Write-Host "  4. Check Iceberg tables in MinIO: http://localhost:9001" -ForegroundColor White
Write-Host ""
Write-Host "Documentation:" -ForegroundColor Cyan
Write-Host "  - Setup Guide:     CDC_SETUP_GUIDE.md" -ForegroundColor Gray
Write-Host "  - Implementation:  PHASE5A_CDC_COMPLETE.md" -ForegroundColor Gray
Write-Host ""
Write-Host "Support:" -ForegroundColor Cyan
Write-Host "  - View logs:       docker logs syniqai-kafka-connect" -ForegroundColor Gray
Write-Host "  - Check status:    docker ps" -ForegroundColor Gray
Write-Host "  - Run tests:       python test_cdc_pipeline.py" -ForegroundColor Gray
Write-Host ""
Write-Host "================================================================================" -ForegroundColor Green
Write-Host ""

# Keep console open
Write-Host "Press any key to exit..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
