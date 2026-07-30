#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Fix Debezium Connector Installation

.DESCRIPTION
    This script properly installs Debezium connectors for Kafka Connect:
    1. Starts the full Kafka stack
    2. Waits for services to be ready
    3. Verifies Debezium connectors
    4. Manually installs if missing
    5. Validates installation
#>

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "          DEBEZIUM CONNECTOR INSTALLATION FIX" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: Stop existing containers
Write-Host "[1/6] Stopping existing containers..." -ForegroundColor Yellow
docker-compose down
if ($LASTEXITCODE -ne 0) {
    Write-Host "    [WARNING] docker-compose down had warnings (this is usually OK)" -ForegroundColor Yellow
}
Write-Host "    [OK] Containers stopped" -ForegroundColor Green
Write-Host ""

# Step 2: Start all services
Write-Host "[2/6] Starting Kafka stack (Zookeeper, Kafka, Kafka Connect, Kafka UI)..." -ForegroundColor Yellow
docker-compose up -d
if ($LASTEXITCODE -ne 0) {
    Write-Host "    [ERROR] Failed to start containers" -ForegroundColor Red
    exit 1
}
Write-Host "    [OK] Containers started" -ForegroundColor Green
Write-Host ""

# Step 3: Wait for Kafka Connect
Write-Host "[3/6] Waiting for Kafka Connect to be ready (this may take 2-3 minutes)..." -ForegroundColor Yellow
$maxAttempts = 24
$attempt = 0
$ready = $false

while ($attempt -lt $maxAttempts -and -not $ready) {
    Start-Sleep -Seconds 5
    $attempt++
    
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8083/" -Method Get -TimeoutSec 2 -ErrorAction SilentlyContinue
        if ($response.StatusCode -eq 200) {
            $ready = $true
            Write-Host "    [OK] Kafka Connect is ready!" -ForegroundColor Green
        }
    } catch {
        Write-Host "    Waiting... ($attempt/$maxAttempts)" -ForegroundColor Gray
    }
}

if (-not $ready) {
    Write-Host "    [ERROR] Kafka Connect did not start in time" -ForegroundColor Red
    Write-Host "    Check logs: docker logs syniqai-kafka-connect" -ForegroundColor Yellow
    exit 1
}
Write-Host ""

# Step 4: Check if Debezium connectors are installed
Write-Host "[4/6] Checking installed connector plugins..." -ForegroundColor Yellow
try {
    $plugins = Invoke-RestMethod -Uri "http://localhost:8083/connector-plugins" -Method Get
    $postgresPlugin = $plugins | Where-Object { $_.class -like "*PostgresConnector*" }
    $mysqlPlugin = $plugins | Where-Object { $_.class -like "*MySqlConnector*" }
    
    if ($postgresPlugin) {
        Write-Host "    [OK] PostgreSQL Debezium connector found: $($postgresPlugin.class)" -ForegroundColor Green
    } else {
        Write-Host "    [NOT FOUND] PostgreSQL Debezium connector" -ForegroundColor Red
    }
    
    if ($mysqlPlugin) {
        Write-Host "    [OK] MySQL Debezium connector found: $($mysqlPlugin.class)" -ForegroundColor Green
    } else {
        Write-Host "    [NOT FOUND] MySQL Debezium connector" -ForegroundColor Red
    }
    
    # If both found, we're done
    if ($postgresPlugin -and $mysqlPlugin) {
        Write-Host ""
        Write-Host "================================================================================" -ForegroundColor Green
        Write-Host "          DEBEZIUM CONNECTORS ALREADY INSTALLED!" -ForegroundColor Green
        Write-Host "================================================================================" -ForegroundColor Green
        Write-Host ""
        Write-Host "Your CDC pipeline is ready. Run: .\start_cdc_pipeline.ps1" -ForegroundColor Cyan
        Write-Host ""
        exit 0
    }
    
} catch {
    Write-Host "    [ERROR] Failed to check plugins: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
Write-Host ""

# Step 5: Manually install missing connectors
Write-Host "[5/6] Installing missing Debezium connectors..." -ForegroundColor Yellow

if (-not $postgresPlugin) {
    Write-Host "    Installing PostgreSQL connector..." -ForegroundColor Cyan
    docker exec syniqai-kafka-connect confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.4.0
    if ($LASTEXITCODE -eq 0) {
        Write-Host "    [OK] PostgreSQL connector installed" -ForegroundColor Green
    } else {
        Write-Host "    [ERROR] Failed to install PostgreSQL connector" -ForegroundColor Red
    }
}

if (-not $mysqlPlugin) {
    Write-Host "    Installing MySQL connector..." -ForegroundColor Cyan
    docker exec syniqai-kafka-connect confluent-hub install --no-prompt debezium/debezium-connector-mysql:2.4.0
    if ($LASTEXITCODE -eq 0) {
        Write-Host "    [OK] MySQL connector installed" -ForegroundColor Green
    } else {
        Write-Host "    [ERROR] Failed to install MySQL connector" -ForegroundColor Red
    }
}

# Restart Kafka Connect to load new plugins
Write-Host "    Restarting Kafka Connect to load plugins..." -ForegroundColor Cyan
docker restart syniqai-kafka-connect
Start-Sleep -Seconds 30
Write-Host "    [OK] Kafka Connect restarted" -ForegroundColor Green
Write-Host ""

# Step 6: Verify installation
Write-Host "[6/6] Verifying connector installation..." -ForegroundColor Yellow
$maxVerify = 6
$verifyAttempt = 0
$verified = $false

while ($verifyAttempt -lt $maxVerify -and -not $verified) {
    Start-Sleep -Seconds 5
    $verifyAttempt++
    
    try {
        $plugins = Invoke-RestMethod -Uri "http://localhost:8083/connector-plugins" -Method Get
        $postgresPlugin = $plugins | Where-Object { $_.class -like "*PostgresConnector*" }
        $mysqlPlugin = $plugins | Where-Object { $_.class -like "*MySqlConnector*" }
        
        if ($postgresPlugin -and $mysqlPlugin) {
            $verified = $true
            Write-Host "    [OK] Both Debezium connectors verified!" -ForegroundColor Green
            Write-Host "        - PostgreSQL: $($postgresPlugin.class)" -ForegroundColor Gray
            Write-Host "        - MySQL: $($mysqlPlugin.class)" -ForegroundColor Gray
        } else {
            Write-Host "    Verifying... ($verifyAttempt/$maxVerify)" -ForegroundColor Gray
        }
    } catch {
        Write-Host "    Waiting for Kafka Connect... ($verifyAttempt/$maxVerify)" -ForegroundColor Gray
    }
}

Write-Host ""

if ($verified) {
    Write-Host "================================================================================" -ForegroundColor Green
    Write-Host "          DEBEZIUM CONNECTORS SUCCESSFULLY INSTALLED!" -ForegroundColor Green
    Write-Host "================================================================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Next Steps:" -ForegroundColor Cyan
    Write-Host "  1. Ensure databases are configured:" -ForegroundColor White
    Write-Host "     - PostgreSQL: WAL enabled + debezium_user created" -ForegroundColor Gray
    Write-Host "     - MariaDB: Binlog enabled + debezium_user created" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  2. Update database hostname in setup_cdc_connectors.py:" -ForegroundColor White
    Write-Host "     hostname='host.docker.internal' (if databases on same machine)" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  3. Run CDC pipeline setup:" -ForegroundColor White
    Write-Host "     .\start_cdc_pipeline.ps1" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Services Running:" -ForegroundColor Cyan
    Write-Host "  - Kafka Broker:        localhost:9092" -ForegroundColor Gray
    Write-Host "  - Kafka Connect:       http://localhost:8083" -ForegroundColor Gray
    Write-Host "  - Kafka UI:            http://localhost:8080" -ForegroundColor Gray
    Write-Host ""
} else {
    Write-Host "================================================================================" -ForegroundColor Red
    Write-Host "          VERIFICATION FAILED" -ForegroundColor Red
    Write-Host "================================================================================" -ForegroundColor Red
    Write-Host ""
    Write-Host "Troubleshooting:" -ForegroundColor Yellow
    Write-Host "  1. Check Kafka Connect logs:" -ForegroundColor White
    Write-Host "     docker logs syniqai-kafka-connect" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  2. Check container status:" -ForegroundColor White
    Write-Host "     docker ps -a | Select-String syniqai" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  3. Try manual verification:" -ForegroundColor White
    Write-Host "     curl http://localhost:8083/connector-plugins" -ForegroundColor Gray
    Write-Host ""
    exit 1
}
