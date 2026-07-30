# Start Airflow Services
# Run this script to start the Airflow scheduler and webserver

$ErrorActionPreference = 'Continue'

Write-Host "Starting Airflow Services..." -ForegroundColor Cyan

# Set paths
$VENV_PATH = "C:\airflow_venv"
$AIRFLOW_HOME = "C:\airflow"

# Activate virtual environment
& "$VENV_PATH\Scripts\Activate.ps1"

# Set Airflow home
$env:AIRFLOW_HOME = "$AIRFLOW_HOME"

# Start webserver in background on port 8081
Write-Host "  Starting Airflow webserver on port 8081..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "& '$VENV_PATH\Scripts\Activate.ps1'; `$env:AIRFLOW_HOME='$AIRFLOW_HOME'; airflow webserver --port 8081"

# Wait a bit for webserver to start
Start-Sleep -Seconds 3

# Start scheduler in new window
Write-Host "  Starting Airflow scheduler..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "& '$VENV_PATH\Scripts\Activate.ps1'; `$env:AIRFLOW_HOME='$AIRFLOW_HOME'; airflow scheduler"

Write-Host ""
Write-Host "✓ Airflow services started!" -ForegroundColor Green
Write-Host "  Webserver: http://localhost:8081" -ForegroundColor Cyan
Write-Host "  Username: admin" -ForegroundColor Cyan
Write-Host "  Password: admin123" -ForegroundColor Cyan
Write-Host ""
Write-Host "Press any key to open Airflow UI..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
Start-Process "http://localhost:8081"
