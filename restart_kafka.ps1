#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Restart Kafka and all related services

.DESCRIPTION
    Stops and starts Kafka ecosystem (Zookeeper, Kafka, Connect, UI, CDC Consumer)
#>

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Kafka Ecosystem Restart" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Navigate to Kafka Integration folder
$kafkaDir = Join-Path $PSScriptRoot "Kafka Integration"
Push-Location $kafkaDir

try {
    # Stop all services
    Write-Host "[*] Stopping all Kafka services..." -ForegroundColor Yellow
    docker-compose down
    
    Start-Sleep -Seconds 3
    
    # Start services in order
    Write-Host "[*] Starting Zookeeper..." -ForegroundColor Yellow
    docker-compose up -d zookeeper
    
    Start-Sleep -Seconds 5
    
    Write-Host "[*] Starting Kafka broker..." -ForegroundColor Yellow
    docker-compose up -d kafka
    
    Start-Sleep -Seconds 10
    
    Write-Host "[*] Starting Kafka Connect..." -ForegroundColor Yellow
    docker-compose up -d kafka-connect
    
    Start-Sleep -Seconds 5
    
    Write-Host "[*] Starting Kafka UI..." -ForegroundColor Yellow
    docker-compose up -d kafka-ui
    
    Write-Host "[*] Starting CDC Consumer..." -ForegroundColor Yellow
    docker-compose up -d cdc-consumer
    
    Write-Host ""
    Write-Host "============================================" -ForegroundColor Green
    Write-Host "  [OK] Kafka Restart Complete!" -ForegroundColor Green
    Write-Host "============================================" -ForegroundColor Green
    Write-Host ""
    
    Write-Host "Services started:" -ForegroundColor Cyan
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | Select-String -Pattern "syniqai"
    
    Write-Host ""
    Write-Host "Access points:" -ForegroundColor Yellow
    Write-Host "  Kafka Broker:  localhost:9092" -ForegroundColor Gray
    Write-Host "  Kafka UI:      http://localhost:8080" -ForegroundColor Gray
    Write-Host "  Kafka Connect: http://localhost:8083" -ForegroundColor Gray
    
} catch {
    Write-Host "[ERROR] Failed to restart Kafka: $_" -ForegroundColor Red
    exit 1
} finally {
    Pop-Location
}
