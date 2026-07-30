# Stop Airflow Services

Write-Host "Stopping Airflow Services..." -ForegroundColor Yellow

# Kill airflow processes
Get-Process python -ErrorAction SilentlyContinue | Where-Object {$_.CommandLine -like "*airflow*"} | Stop-Process -Force

Write-Host "✓ Airflow services stopped" -ForegroundColor Green
