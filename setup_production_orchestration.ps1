# ============================================================================
# SyniqAI Production Orchestration Setup
# ============================================================================
# This script sets up Airflow with Python 3.11 for production orchestration
# while keeping your main project on Python 3.14

$ErrorActionPreference = "Stop"

Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host "  SyniqAI Production Orchestration Setup" -ForegroundColor Cyan
Write-Host "  Airflow + MongoDB CDC Integration" -ForegroundColor Cyan
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""

# Configuration
$PYTHON311_URL = "https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe"
$PYTHON311_INSTALLER = "$env:TEMP\python-3.11.9-amd64.exe"
$AIRFLOW_HOME = "C:\airflow"
$VENV_PATH = "C:\airflow_venv"
$PROJECT_ROOT = $PSScriptRoot

Write-Host "[1/8] Checking Python 3.11 installation..." -ForegroundColor Yellow

# Check if Python 3.11 is already installed
$python311 = Get-Command "py" -ErrorAction SilentlyContinue
if ($python311) {
    $version = & py -3.11 --version 2>&1
    if ($version -match "Python 3\.11") {
        Write-Host "  ✓ Python 3.11 found: $version" -ForegroundColor Green
    } else {
        Write-Host "  ✗ Python 3.11 not found. Please install Python 3.11.9" -ForegroundColor Red
        Write-Host "    Download from: https://www.python.org/downloads/release/python-3119/" -ForegroundColor Yellow
        exit 1
    }
} else {
    Write-Host "  ✗ Python launcher not found. Please install Python 3.11.9" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "[2/8] Creating Airflow virtual environment..." -ForegroundColor Yellow

if (Test-Path $VENV_PATH) {
    Write-Host "  ✓ Virtual environment already exists at: $VENV_PATH" -ForegroundColor Green
} else {
    py -3.11 -m venv $VENV_PATH
    Write-Host "  ✓ Created virtual environment at: $VENV_PATH" -ForegroundColor Green
}

Write-Host ""
Write-Host "[3/8] Installing Airflow dependencies..." -ForegroundColor Yellow

& "$VENV_PATH\Scripts\Activate.ps1"

# Set Airflow home
[System.Environment]::SetEnvironmentVariable('AIRFLOW_HOME', $AIRFLOW_HOME, 'User')
$env:AIRFLOW_HOME = $AIRFLOW_HOME

# Create Airflow directory structure
if (!(Test-Path $AIRFLOW_HOME)) {
    New-Item -ItemType Directory -Path $AIRFLOW_HOME | Out-Null
    New-Item -ItemType Directory -Path "$AIRFLOW_HOME\dags" | Out-Null
    New-Item -ItemType Directory -Path "$AIRFLOW_HOME\logs" | Out-Null
    New-Item -ItemType Directory -Path "$AIRFLOW_HOME\plugins" | Out-Null
    Write-Host "  ✓ Created Airflow directory structure" -ForegroundColor Green
}

# Install Airflow with constraints
$PYTHON_VERSION = "3.11"
$AIRFLOW_VERSION = "2.8.0"
$CONSTRAINT_URL = "https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION/constraints-$PYTHON_VERSION.txt"

Write-Host "  Installing Apache Airflow $AIRFLOW_VERSION..." -ForegroundColor White
pip install "apache-airflow==$AIRFLOW_VERSION" --constraint $CONSTRAINT_URL

# Install additional providers
Write-Host "  Installing Airflow providers..." -ForegroundColor White
pip install apache-airflow-providers-apache-kafka
pip install apache-airflow-providers-postgres
pip install apache-airflow-providers-http

# Install other dependencies
Write-Host "  Installing additional dependencies..." -ForegroundColor White
pip install pymongo kafka-python python-dotenv dataclasses-json

Write-Host ""
Write-Host "[4/8] Copying MongoDB CDC files..." -ForegroundColor Yellow

# Copy MongoDB CDC components from temp directory
$mongoSourceDir = "$env:TEMP\SYINIQ_MongoDB_CDC_temp\SYINIQ_MongoDB_CDC\Kafka Integration"
$kafkaIntegrationDir = "$PROJECT_ROOT\Kafka Integration"

if (Test-Path $mongoSourceDir) {
    # Copy MongoDB batch extractor
    Copy-Item "$mongoSourceDir\mongodb_batch_extractor.py" -Destination $kafkaIntegrationDir -Force
    Write-Host "  ✓ Copied mongodb_batch_extractor.py" -ForegroundColor Green
    
    # Copy Spark consumer
    Copy-Item "$mongoSourceDir\spark_mongodb_batch_consumer.py" -Destination $kafkaIntegrationDir -Force
    Write-Host "  ✓ Copied spark_mongodb_batch_consumer.py" -ForegroundColor Green
    
    # Copy seed data script
    Copy-Item "$mongoSourceDir\seed_dummy_data.py" -Destination $kafkaIntegrationDir -Force
    Write-Host "  ✓ Copied seed_dummy_data.py" -ForegroundColor Green
    
    # Copy watermarks directory
    if (!(Test-Path "$kafkaIntegrationDir\watermarks")) {
        Copy-Item "$mongoSourceDir\watermarks" -Destination $kafkaIntegrationDir -Recurse -Force
        Write-Host "  ✓ Copied watermarks directory" -ForegroundColor Green
    }
    
    # Copy requirements
    Copy-Item "$mongoSourceDir\requirements.txt" -Destination "$kafkaIntegrationDir\requirements-mongodb.txt" -Force
    Write-Host "  ✓ Copied requirements.txt" -ForegroundColor Green
} else {
    Write-Host "  ⚠ MongoDB CDC source not found at: $mongoSourceDir" -ForegroundColor Yellow
    Write-Host "    Please extract SYINIQ_MongoDB_CDC (2).zip to temp directory first" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "[5/8] Initializing Airflow database..." -ForegroundColor Yellow

airflow db init

Write-Host ""
Write-Host "[6/8] Creating Airflow admin user..." -ForegroundColor Yellow

# Check if admin user exists
$userExists = airflow users list 2>&1 | Select-String "admin"
if (!$userExists) {
    airflow users create `
        --username admin `
        --firstname Admin `
        --lastname User `
        --role Admin `
        --email admin@syniqai.com `
        --password admin123
    Write-Host "  ✓ Created admin user (username: admin, password: admin123)" -ForegroundColor Green
} else {
    Write-Host "  ✓ Admin user already exists" -ForegroundColor Green
}

Write-Host ""
Write-Host "[7/8] Creating configuration files..." -ForegroundColor Yellow

# Create .env for MongoDB CDC if it doesn't exist
if (!(Test-Path "$kafkaIntegrationDir\.env")) {
    Write-Host "  Creating .env configuration..." -ForegroundColor White
    Write-Host "  ⚠ Please update MongoDB connection settings after setup!" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "[8/8] Creating startup scripts..." -ForegroundColor Yellow

# Create Airflow startup script
$airflowStartScript = @"
# Start Airflow Services
# Run this script to start the Airflow scheduler and webserver

`$ErrorActionPreference = 'Continue'

Write-Host "Starting Airflow Services..." -ForegroundColor Cyan

# Activate virtual environment
& "$VENV_PATH\Scripts\Activate.ps1"

# Set Airflow home
`$env:AIRFLOW_HOME = "$AIRFLOW_HOME"

# Start webserver in background
Start-Process powershell -ArgumentList "-NoExit", "-Command", "& '$VENV_PATH\Scripts\Activate.ps1'; `$env:AIRFLOW_HOME='$AIRFLOW_HOME'; airflow webserver --port 8080"

# Wait a bit for webserver to start
Start-Sleep -Seconds 3

# Start scheduler in new window
Start-Process powershell -ArgumentList "-NoExit", "-Command", "& '$VENV_PATH\Scripts\Activate.ps1'; `$env:AIRFLOW_HOME='$AIRFLOW_HOME'; airflow scheduler"

Write-Host ""
Write-Host "✓ Airflow services started!" -ForegroundColor Green
Write-Host "  Webserver: http://localhost:8080" -ForegroundColor Cyan
Write-Host "  Username: admin" -ForegroundColor Cyan
Write-Host "  Password: admin123" -ForegroundColor Cyan
Write-Host ""
Write-Host "Press any key to open Airflow UI..."
`$null = `$Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
Start-Process "http://localhost:8080"
"@

$airflowStartScript | Out-File -FilePath "$PROJECT_ROOT\start_airflow.ps1" -Encoding UTF8
Write-Host "  ✓ Created start_airflow.ps1" -ForegroundColor Green

# Create Airflow stop script
$airflowStopScript = @"
# Stop Airflow Services

Write-Host "Stopping Airflow Services..." -ForegroundColor Yellow

# Kill airflow processes
Get-Process *airflow* -ErrorAction SilentlyContinue | Stop-Process -Force
Get-Process python -ErrorAction SilentlyContinue | Where-Object {`$_.Path -like "*airflow*"} | Stop-Process -Force

Write-Host "✓ Airflow services stopped" -ForegroundColor Green
"@

$airflowStopScript | Out-File -FilePath "$PROJECT_ROOT\stop_airflow.ps1" -Encoding UTF8
Write-Host "  ✓ Created stop_airflow.ps1" -ForegroundColor Green

Write-Host ""
Write-Host "============================================================================" -ForegroundColor Green
Write-Host "  ✓ Production Orchestration Setup Complete!" -ForegroundColor Green
Write-Host "============================================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Cyan
Write-Host "  1. Configure MongoDB connection in: Kafka Integration\.env" -ForegroundColor White
Write-Host "  2. Start Airflow: .\start_airflow.ps1" -ForegroundColor White
Write-Host "  3. Access Airflow UI: http://localhost:8080 (admin/admin123)" -ForegroundColor White
Write-Host "  4. Copy DAG files to: $AIRFLOW_HOME\dags" -ForegroundColor White
Write-Host "  5. Enable and trigger DAGs from Airflow UI" -ForegroundColor White
Write-Host ""
Write-Host "Airflow Home: $AIRFLOW_HOME" -ForegroundColor Yellow
Write-Host "Virtual Environment: $VENV_PATH" -ForegroundColor Yellow
Write-Host ""
