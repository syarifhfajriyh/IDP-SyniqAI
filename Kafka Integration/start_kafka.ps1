# ============================================================================
# SYINIQ Phase 5 - Kafka Integration Quick Start
# ============================================================================

Write-Host "`n=== SYINIQ Kafka Integration Quick Start ===`n" -ForegroundColor Cyan

# Change to Kafka Integration directory
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptPath

# Step 1: Check Docker
Write-Host "Step 1: Checking Docker..." -ForegroundColor Yellow
$dockerCheck = docker --version 2>$null
if (-not $dockerCheck) {
    Write-Host "❌ Docker not found. Please install Docker Desktop:" -ForegroundColor Red
    Write-Host "   https://www.docker.com/products/docker-desktop" -ForegroundColor White
    exit 1
}
Write-Host "✓ Docker is installed: $dockerCheck" -ForegroundColor Green

# Step 2: Start Kafka services
Write-Host "`nStep 2: Starting Kafka services (Docker)..." -ForegroundColor Yellow
Write-Host "This will start: Zookeeper, Kafka, Kafka Connect, and Kafka UI" -ForegroundColor Gray

docker-compose up -d

if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ Kafka services started" -ForegroundColor Green
} else {
    Write-Host "❌ Failed to start Kafka services" -ForegroundColor Red
    exit 1
}

# Step 3: Wait for services to be ready
Write-Host "`nStep 3: Waiting for services to initialize (30 seconds)..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

# Step 4: Check service status
Write-Host "`nStep 4: Checking service status..." -ForegroundColor Yellow
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | Select-String "syniqai"

# Step 5: Install Python dependencies
Write-Host "`nStep 5: Installing Python dependencies..." -ForegroundColor Yellow
pip install -q -r requirements.txt
if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ Dependencies installed" -ForegroundColor Green
} else {
    Write-Host "⚠ Warning: Some dependencies may have failed to install" -ForegroundColor Yellow
}

# Step 6: Test connectivity
Write-Host "`nStep 6: Testing Kafka connectivity..." -ForegroundColor Yellow

# Test Kafka (requires kafka-python)
python -c "from kafka import KafkaAdminClient; client = KafkaAdminClient(bootstrap_servers='localhost:9092', request_timeout_ms=3000); print('✓ Kafka broker reachable'); client.close()" 2>$null

if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ Kafka broker is reachable" -ForegroundColor Green
} else {
    Write-Host "⚠ Kafka broker not ready yet. It may need more time to start." -ForegroundColor Yellow
}

# Test Kafka Connect
$response = curl.exe -s http://localhost:8083/ 2>$null
if ($response) {
    Write-Host "✓ Kafka Connect is reachable" -ForegroundColor Green
} else {
    Write-Host "⚠ Kafka Connect not ready yet. It may need more time to start." -ForegroundColor Yellow
}

# Summary
Write-Host "`n=== Kafka Integration Ready ===`n" -ForegroundColor Cyan
Write-Host "Services:" -ForegroundColor White
Write-Host "  • Kafka Broker:      localhost:9092" -ForegroundColor Gray
Write-Host "  • Kafka Connect:     http://localhost:8083" -ForegroundColor Gray
Write-Host "  • Kafka UI:          http://localhost:8080" -ForegroundColor Gray
Write-Host "  • Zookeeper:         localhost:2181" -ForegroundColor Gray

Write-Host "`nNext Steps:" -ForegroundColor White
Write-Host "  1. Start the SYINIQ backend:" -ForegroundColor Gray
Write-Host "     cd ..\gui" -ForegroundColor Yellow
Write-Host "     python api/backend.py" -ForegroundColor Yellow
Write-Host ""
Write-Host "  2. Check Kafka integration:" -ForegroundColor Gray
Write-Host "     curl http://localhost:8000/api/kafka/health" -ForegroundColor Yellow
Write-Host ""
Write-Host "  3. Open Kafka UI in browser:" -ForegroundColor Gray
Write-Host "     http://localhost:8080" -ForegroundColor Yellow
Write-Host ""

Write-Host "To stop Kafka services:" -ForegroundColor White
Write-Host "  docker-compose down" -ForegroundColor Yellow
Write-Host ""
