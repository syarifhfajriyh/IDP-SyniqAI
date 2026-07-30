#!/usr/bin/env pwsh
# SyniqAI Automated Startup Script
# This script starts the backend which automatically starts Kafka + Debezium
# No need to manually run docker-compose or CDC setup scripts!

Write-Host "🚀 Starting SyniqAI Platform..." -ForegroundColor Cyan
Write-Host ""

# Check if Docker Desktop is running
Write-Host "Checking Docker Desktop..." -ForegroundColor Yellow
$dockerRunning = $false
try {
    docker ps 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        $dockerRunning = $true
        Write-Host "✓ Docker Desktop is running" -ForegroundColor Green
    }
} catch {
    # Docker not available
}

if (-not $dockerRunning) {
    Write-Host "⚠ Docker Desktop is not running!" -ForegroundColor Red
    Write-Host "Please start Docker Desktop and try again." -ForegroundColor Yellow
    Write-Host "The backend will start but Kafka features will be unavailable." -ForegroundColor Yellow
    Write-Host ""
    $continue = Read-Host "Continue anyway? (y/n)"
    if ($continue -ne 'y') {
        exit 1
    }
}

Write-Host ""
Write-Host "Starting Backend (FastAPI)..." -ForegroundColor Yellow
Write-Host "Backend will automatically:" -ForegroundColor Gray
Write-Host "  1. Start Kafka + Debezium (if Docker is available)" -ForegroundColor Gray
Write-Host "  2. Initialize PostgreSQL database" -ForegroundColor Gray
Write-Host "  3. Initialize MinIO storage" -ForegroundColor Gray
Write-Host "  4. Start API server on http://localhost:8000" -ForegroundColor Gray
Write-Host ""

# Change to API directory
Set-Location "gui\api"

# Start backend (output will stream to console)
python backend.py

# If backend exits unexpectedly
Write-Host ""
Write-Host "Backend stopped." -ForegroundColor Red
