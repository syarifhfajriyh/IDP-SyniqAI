# Simple Airflow Standalone Setup
# This runs Airflow in standalone mode (all-in-one) for development

Write-Host "Starting Airflow Standalone..." -ForegroundColor Cyan

# Stop any existing Airflow containers
docker rm -f airflow-standalone 2>$null

# Create directories
if (-not (Test-Path "airflow_dags")) { New-Item -ItemType Directory -Path "airflow_dags" }
if (-not (Test-Path "airflow_logs")) { New-Item -ItemType Directory -Path "airflow_logs" }

# Run Airflow in standalone mode
docker run -d `
  --name airflow-standalone `
  --network kafkaintegration_syniqai-network `
  -p 8081:8080 `
  -v "${PWD}/airflow_dags:/opt/airflow/dags" `
  -v "${PWD}/airflow_logs:/opt/airflow/logs" `
  -e AIRFLOW__CORE__LOAD_EXAMPLES=False `
  -e AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True `
  apache/airflow:2.10.5-python3.12 `
  bash -c "airflow db init && airflow users create --username admin --password admin123 --firstname Admin --lastname User --role Admin --email admin@syniqai.com && airflow standalone"

Write-Host ""
Write-Host "✓ Airflow starting in standalone mode..." -ForegroundColor Green
Write-Host "  This may take 60-90 seconds to fully start" -ForegroundColor Yellow
Write-Host ""
Write-Host "Access Airflow at: http://localhost:8081" -ForegroundColor Cyan
Write-Host "Username: admin" -ForegroundColor Cyan
Write-Host "Password: admin123" -ForegroundColor Cyan
Write-Host ""
Write-Host "To view logs: docker logs -f airflow-standalone" -ForegroundColor White
Write-Host "To stop: docker stop airflow-standalone" -ForegroundColor White
