#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Install Debezium Connectors Manually

.DESCRIPTION
    Downloads Debezium connectors from official releases and installs them into Kafka Connect
#>

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "          MANUAL DEBEZIUM CONNECTOR INSTALLATION" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""

$DEBEZIUM_VERSION = "2.4.2.Final"
$POSTGRES_URL = "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/$DEBEZIUM_VERSION/debezium-connector-postgres-$DEBEZIUM_VERSION-plugin.tar.gz"
$MYSQL_URL = "https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/$DEBEZIUM_VERSION/debezium-connector-mysql-$DEBEZIUM_VERSION-plugin.tar.gz"

$TEMP_DIR = "$env:TEMP\debezium_install"

# Step 1: Create temp directory
Write-Host "[1/8] Creating temporary directory..." -ForegroundColor Yellow
if (Test-Path $TEMP_DIR) {
    Remove-Item $TEMP_DIR -Recurse -Force
}
New-Item -ItemType Directory -Path $TEMP_DIR | Out-Null
Write-Host "    [OK] Directory created: $TEMP_DIR" -ForegroundColor Green
Write-Host ""

# Step 2: Download PostgreSQL connector
Write-Host "[2/8] Downloading PostgreSQL connector..." -ForegroundColor Yellow
Write-Host "    URL: $POSTGRES_URL" -ForegroundColor Gray
$postgresFile = "$TEMP_DIR\postgres-connector.tar.gz"

try {
    Invoke-WebRequest -Uri $POSTGRES_URL -OutFile $postgresFile -UseBasicParsing
    Write-Host "    [OK] Downloaded: $('{0:N2}' -f ((Get-Item $postgresFile).Length / 1MB)) MB" -ForegroundColor Green
} catch {
    Write-Host "    [ERROR] Failed to download: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
Write-Host ""

# Step 3: Download MySQL connector
Write-Host "[3/8] Downloading MySQL connector..." -ForegroundColor Yellow
Write-Host "    URL: $MYSQL_URL" -ForegroundColor Gray
$mysqlFile = "$TEMP_DIR\mysql-connector.tar.gz"

try {
    Invoke-WebRequest -Uri $MYSQL_URL -OutFile $mysqlFile -UseBasicParsing
    Write-Host "    [OK] Downloaded: $('{0:N2}' -f ((Get-Item $mysqlFile).Length / 1MB)) MB" -ForegroundColor Green
} catch {
    Write-Host "    [ERROR] Failed to download: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
Write-Host ""

# Step 4: Copy tar.gz files to container
Write-Host "[4/8] Copying archives to Kafka Connect container..." -ForegroundColor Yellow
docker cp $postgresFile syniqai-kafka-connect:/tmp/postgres-connector.tar.gz
docker cp $mysqlFile syniqai-kafka-connect:/tmp/mysql-connector.tar.gz
Write-Host "    [OK] Archives copied to container" -ForegroundColor Green
Write-Host ""

# Step 5: Extract inside container (using container's tar)
Write-Host "[5/8] Extracting connectors inside container..." -ForegroundColor Yellow

Write-Host "    Extracting PostgreSQL connector..." -ForegroundColor Cyan
docker exec syniqai-kafka-connect mkdir -p /usr/share/confluent-hub-components/debezium-postgres
docker exec syniqai-kafka-connect tar -xzf /tmp/postgres-connector.tar.gz -C /usr/share/confluent-hub-components/debezium-postgres --strip-components=1

if ($LASTEXITCODE -eq 0) {
    Write-Host "    [OK] PostgreSQL connector extracted" -ForegroundColor Green
} else {
    Write-Host "    [ERROR] Failed to extract PostgreSQL connector" -ForegroundColor Red
    exit 1
}

Write-Host "    Extracting MySQL connector..." -ForegroundColor Cyan
docker exec syniqai-kafka-connect mkdir -p /usr/share/confluent-hub-components/debezium-mysql
docker exec syniqai-kafka-connect tar -xzf /tmp/mysql-connector.tar.gz -C /usr/share/confluent-hub-components/debezium-mysql --strip-components=1

if ($LASTEXITCODE -eq 0) {
    Write-Host "    [OK] MySQL connector extracted" -ForegroundColor Green
} else {
    Write-Host "    [ERROR] Failed to extract MySQL connector" -ForegroundColor Red
    exit 1
}

# Cleanup temp files in container
docker exec syniqai-kafka-connect rm /tmp/postgres-connector.tar.gz /tmp/mysql-connector.tar.gz
Write-Host ""

# Step 6: Verify files in container
Write-Host "[6/8] Verifying connector files..." -ForegroundColor Yellow
$postgresCount = docker exec syniqai-kafka-connect sh -c "ls -1 /usr/share/confluent-hub-components/debezium-postgres/*.jar 2>/dev/null | wc -l"
$mysqlCount = docker exec syniqai-kafka-connect sh -c "ls -1 /usr/share/confluent-hub-components/debezium-mysql/*.jar 2>/dev/null | wc -l"

Write-Host "    PostgreSQL connector JARs: $($postgresCount.Trim())" -ForegroundColor Gray
Write-Host "    MySQL connector JARs: $($mysqlCount.Trim())" -ForegroundColor Gray

if ([int]$postgresCount -gt 0 -and [int]$mysqlCount -gt 0) {
    Write-Host "    [OK] Connector files verified" -ForegroundColor Green
} else {
    Write-Host "    [WARNING] Some connector files may be missing" -ForegroundColor Yellow
}
Write-Host ""

# Step 7: Restart Kafka Connect
Write-Host "[7/8] Restarting Kafka Connect..." -ForegroundColor Yellow
docker restart syniqai-kafka-connect
Start-Sleep -Seconds 35
Write-Host "    [OK] Kafka Connect restarted" -ForegroundColor Green
Write-Host ""

# Step 8: Verify installation
Write-Host "[8/8] Verifying installation..." -ForegroundColor Yellow
$maxAttempts = 6
$attempt = 0
$verified = $false

while ($attempt -lt $maxAttempts -and -not $verified) {
    Start-Sleep -Seconds 5
    $attempt++
    
    try {
        $plugins = Invoke-RestMethod -Uri "http://localhost:8083/connector-plugins" -Method Get -UseBasicParsing
        $postgresPlugin = $plugins | Where-Object { $_.class -like "*PostgresConnector*" }
        $mysqlPlugin = $plugins | Where-Object { $_.class -like "*MySqlConnector*" }
        
        if ($postgresPlugin -and $mysqlPlugin) {
            $verified = $true
            Write-Host "    [OK] Both Debezium connectors verified!" -ForegroundColor Green
            Write-Host "        - PostgreSQL: $($postgresPlugin.class)" -ForegroundColor Gray
            Write-Host "        - MySQL: $($mysqlPlugin.class)" -ForegroundColor Gray
        } else {
            Write-Host "    Waiting for connectors to load... ($attempt/$maxAttempts)" -ForegroundColor Gray
        }
    } catch {
        Write-Host "    Waiting for Kafka Connect... ($attempt/$maxAttempts)" -ForegroundColor Gray
    }
}

Write-Host ""

# Cleanup
Write-Host "Cleaning up temporary files..." -ForegroundColor Yellow
Remove-Item $TEMP_DIR -Recurse -Force
Write-Host "    [OK] Cleanup complete" -ForegroundColor Green
Write-Host ""

if ($verified) {
    Write-Host "================================================================================" -ForegroundColor Green
    Write-Host "          DEBEZIUM CONNECTORS SUCCESSFULLY INSTALLED!" -ForegroundColor Green
    Write-Host "================================================================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Installed Components:" -ForegroundColor Cyan
    Write-Host "  - Debezium PostgreSQL Connector v$DEBEZIUM_VERSION" -ForegroundColor Gray
    Write-Host "  - Debezium MySQL Connector v$DEBEZIUM_VERSION" -ForegroundColor Gray
    Write-Host ""
    Write-Host "Next Steps:" -ForegroundColor Cyan
    Write-Host "  1. Verify databases are configured:" -ForegroundColor White
    Write-Host "     - PostgreSQL: WAL enabled (wal_level=logical)" -ForegroundColor Gray
    Write-Host "     - MariaDB: Binlog enabled (log_bin=ON, binlog_format=ROW)" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  2. Update database configuration in setup_cdc_connectors.py:" -ForegroundColor White
    Write-Host "     - Hostname: 'host.docker.internal' (if on same machine)" -ForegroundColor Gray
    Write-Host "     - Or use actual IP: '192.168.x.x' (if on network)" -ForegroundColor Gray
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
    Write-Host "  2. List installed plugins:" -ForegroundColor White
    Write-Host "     docker exec syniqai-kafka-connect ls -la /usr/share/confluent-hub-components/" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  3. Check plugin path:" -ForegroundColor White
    Write-Host "     docker exec syniqai-kafka-connect bash -c 'echo \$CONNECT_PLUGIN_PATH'" -ForegroundColor Gray
    Write-Host ""
    exit 1
}
