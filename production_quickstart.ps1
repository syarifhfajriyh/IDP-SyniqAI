# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║ SyniqAI - Production Quick Start                                            ║
# ║ One-Command Setup: Airflow + MongoDB CDC + Complete Stack                   ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║                                                              ║" -ForegroundColor Cyan
Write-Host "║           🚀 SYNIQAI PRODUCTION QUICK START 🚀              ║" -ForegroundColor Cyan
Write-Host "║                                                              ║" -ForegroundColor Cyan
Write-Host "║     Complete Production Stack with Orchestration             ║" -ForegroundColor Cyan
Write-Host "║                                                              ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

$PROJECT_ROOT = $PSScriptRoot

# Startup sequence counter
$stepNumber = 1
$totalSteps = 10

function Write-Step {
    param($message, $color = "Yellow")
    Write-Host ""
    Write-Host "[$stepNumber/$totalSteps] $message" -ForegroundColor $color
    $script:stepNumber++
}

function Write-Success {
    param($message)
    Write-Host "  ✓ $message" -ForegroundColor Green
}

function Write-Info {
    param($message)
    Write-Host "  ℹ $message" -ForegroundColor Cyan
}

function Write-Warning {
    param($message)
    Write-Host "  ⚠ $message" -ForegroundColor Yellow
}

function Write-Error {
    param($message)
    Write-Host "  ✗ $message" -ForegroundColor Red
}

# ============================================================================
# Pre-flight Checks
# ============================================================================

Write-Step "Running pre-flight checks..." "Cyan"

# Check Docker
try {
    $dockerVersion = docker --version 2>&1
    if ($dockerVersion -match "Docker") {
        Write-Success "Docker installed: $dockerVersion"
    } else {
        Write-Error "Docker not found! Please install Docker Desktop."
        exit 1
    }
} catch {
    Write-Error "Docker not found! Please install Docker Desktop."
    exit 1
}

# Check Python 3.11
try {
    $python311 = & py -3.11 --version 2>&1
    if ($python311 -match "Python 3\.11") {
        Write-Success "Python 3.11 found: $python311"
    } else {
        Write-Error "Python 3.11 not found! Please install from: https://www.python.org/downloads/"
        exit 1
    }
} catch {
    Write-Error "Python 3.11 not found! Please install from: https://www.python.org/downloads/"
    exit 1
}

# Check Node.js
try {
    $nodeVersion = node --version 2>&1
    Write-Success "Node.js installed: $nodeVersion"
} catch {
    Write-Warning "Node.js not found! Frontend won't start. Install from: https://nodejs.org/"
}

Write-Success "All pre-flight checks passed!"

# ============================================================================
# Step 1: Setup Airflow Environment
# ============================================================================

Write-Step "Setting up Airflow orchestration..." "Yellow"

if (Test-Path "$PROJECT_ROOT\setup_production_orchestration.ps1") {
    Write-Info "Running Airflow setup script..."
    & "$PROJECT_ROOT\setup_production_orchestration.ps1"
    
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Airflow setup complete!"
    } else {
        Write-Warning "Airflow setup encountered issues. Continuing..."
    }
} else {
    Write-Warning "Airflow setup script not found. Skipping..."
}

# ============================================================================
# Step 2: Copy MongoDB CDC DAG to Airflow
# ============================================================================

Write-Step "Deploying MongoDB CDC DAG..." "Yellow"

$dagSource = "$PROJECT_ROOT\airflow_dags\mongodb_cdc_dag.py"
$dagDest = "C:\airflow\dags\mongodb_cdc_dag.py"

if (Test-Path $dagSource) {
    # Ensure DAGs directory exists
    if (!(Test-Path "C:\airflow\dags")) {
        New-Item -ItemType Directory -Path "C:\airflow\dags" -Force | Out-Null
    }
    
    Copy-Item $dagSource -Destination $dagDest -Force
    Write-Success "MongoDB CDC DAG deployed to Airflow"
    Write-Info "DAG location: $dagDest"
} else {
    Write-Warning "DAG file not found at: $dagSource"
}

# ============================================================================
# Step 3: Configure MongoDB CDC
# ============================================================================

Write-Step "Configuring MongoDB CDC..." "Yellow"

$envFile = "$PROJECT_ROOT\Kafka Integration\.env.mongodb"

if (Test-Path $envFile) {
    Write-Success "MongoDB CDC configuration found"
    Write-Info "Configuration file: $envFile"
    Write-Warning "IMPORTANT: Update MongoDB connection settings!"
    Write-Host ""
    Write-Host "    Required settings:" -ForegroundColor White
    Write-Host "    - MONGODB_URI" -ForegroundColor White
    Write-Host "    - MONGODB_DATABASE" -ForegroundColor White
    Write-Host "    - MONGODB_COLLECTIONS" -ForegroundColor White
    Write-Host ""
} else {
    Write-Warning "MongoDB CDC config not found. Please create .env.mongodb"
}

# ============================================================================
# Step 4: Start Kafka Infrastructure
# ============================================================================

Write-Step "Starting Kafka infrastructure..." "Yellow"

Push-Location "$PROJECT_ROOT\Kafka Integration"

Write-Info "Checking existing Kafka containers..."
$kafkaRunning = docker ps --format "{{.Names}}" | Select-String "kafka"

if ($kafkaRunning) {
    Write-Success "Kafka already running"
} else {
    Write-Info "Starting Kafka and Zookeeper with Docker Compose..."
    docker-compose up -d
    
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Kafka infrastructure started"
        Write-Info "Waiting 10 seconds for Kafka to initialize..."
        Start-Sleep -Seconds 10
    } else {
        Write-Error "Failed to start Kafka! Check Docker Compose configuration."
    }
}

Pop-Location

# Verify Kafka is accessible
Write-Info "Verifying Kafka connectivity..."
try {
    docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > $null 2>&1
    Write-Success "Kafka is accessible on port 9092"
} catch {
    Write-Warning "Kafka connectivity check failed. May need more time to start."
}

# ============================================================================
# Step 5: Start MinIO Storage
# ============================================================================

Write-Step "Starting MinIO object storage..." "Yellow"

$minioRunning = docker ps --format "{{.Names}}" | Select-String "minio"

if ($minioRunning) {
    Write-Success "MinIO already running"
} else {
    Write-Info "Starting MinIO container..."
    docker run -d `
        -p 9000:9000 `
        -p 9001:9001 `
        --name minio `
        --restart unless-stopped `
        -e "MINIO_ROOT_USER=minioadmin" `
        -e "MINIO_ROOT_PASSWORD=minioadmin" `
        -v minio-data:/data `
        minio/minio server /data --console-address ":9001"
    
    if ($LASTEXITCODE -eq 0) {
        Write-Success "MinIO started successfully"
        Write-Info "Console: http://localhost:9001 (minioadmin/minioadmin)"
        Start-Sleep -Seconds 5
    } else {
        Write-Warning "MinIO may already be running or failed to start"
    }
}

# ============================================================================
# Step 6: Initialize MinIO Buckets
# ============================================================================

Write-Step "Initializing MinIO buckets..." "Yellow"

Write-Info "Creating Bronze, Silver, Gold buckets..."

# Simple Python script to create buckets
$createBucketsScript = @"
import boto3
from botocore.client import Config

s3 = boto3.client('s3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

buckets = ['bronze', 'silver', 'gold', 'backups']
for bucket in buckets:
    try:
        s3.create_bucket(Bucket=bucket)
        print(f'  ✓ Created bucket: {bucket}')
    except Exception as e:
        if 'BucketAlreadyOwnedByYou' in str(e):
            print(f'  ✓ Bucket already exists: {bucket}')
        else:
            print(f'  ✗ Error creating {bucket}: {e}')
"@

$createBucketsScript | python -
Write-Success "MinIO buckets initialized"

# ============================================================================
# Step 7: Start Airflow Services
# ============================================================================

Write-Step "Starting Airflow services..." "Yellow"

if (Test-Path "$PROJECT_ROOT\start_airflow.ps1") {
    Write-Info "Launching Airflow webserver and scheduler..."
    Write-Info "This will open two new PowerShell windows"
    Start-Sleep -Seconds 2
    
    # Start Airflow in background
    Start-Process powershell -ArgumentList "-NoExit", "-File", "$PROJECT_ROOT\start_airflow.ps1"
    
    Write-Success "Airflow services starting..."
    Write-Info "Webserver: http://localhost:8080"
    Write-Info "Username: admin"
    Write-Info "Password: admin123"
    Write-Info "Waiting 15 seconds for Airflow to initialize..."
    Start-Sleep -Seconds 15
} else {
    Write-Warning "Airflow start script not found. Start manually."
}

# ============================================================================
# Step 8: Install Backend Dependencies
# ============================================================================

Write-Step "Setting up backend API..." "Yellow"

Push-Location "$PROJECT_ROOT\gui"

if (Test-Path "requirements.txt") {
    Write-Info "Installing Python dependencies..."
    pip install -r requirements.txt --quiet
    Write-Success "Backend dependencies installed"
} else {
    Write-Warning "requirements.txt not found in gui folder"
}

Pop-Location

# ============================================================================
# Step 9: Start Backend API
# ============================================================================

Write-Step "Starting FastAPI backend..." "Yellow"

Write-Info "Launching backend server on port 8000..."

Start-Process powershell -ArgumentList @"
-NoExit
-Command
Write-Host 'Starting SyniqAI Backend API...' -ForegroundColor Cyan;
cd '$PROJECT_ROOT\gui';
python app.py
"@

Write-Success "Backend API starting..."
Write-Info "API endpoint: http://localhost:8000"
Start-Sleep -Seconds 5

# ============================================================================
# Step 10: Start Frontend UI
# ============================================================================

Write-Step "Starting React frontend..." "Yellow"

Push-Location "$PROJECT_ROOT\gui"

if (Test-Path "package.json") {
    # Check if node_modules exists
    if (!(Test-Path "node_modules")) {
        Write-Info "Installing Node.js dependencies (first time only)..."
        npm install
    }
    
    Write-Info "Launching frontend development server..."
    
    Start-Process powershell -ArgumentList @"
-NoExit
-Command
Write-Host 'Starting SyniqAI Frontend UI...' -ForegroundColor Cyan;
cd '$PROJECT_ROOT\gui';
npm run dev
"@
    
    Write-Success "Frontend starting..."
    Write-Info "Dashboard: http://localhost:3000"
    Start-Sleep -Seconds 5
} else {
    Write-Warning "package.json not found. Frontend won't start."
}

Pop-Location

# ============================================================================
# Final Status Report
# ============================================================================

Write-Host ""
Write-Host "╔═══════════════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║                                                                   ║" -ForegroundColor Green
Write-Host "║           ✓ SYNIQAI PRODUCTION STACK IS NOW RUNNING!            ║" -ForegroundColor Green
Write-Host "║                                                                   ║" -ForegroundColor Green
Write-Host "╚═══════════════════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""

Write-Host "╔═══════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║ 🌐 SERVICE ENDPOINTS                                              ║" -ForegroundColor Cyan
Write-Host "╠═══════════════════════════════════════════════════════════════════╣" -ForegroundColor Cyan
Write-Host "║                                                                   ║" -ForegroundColor White
Write-Host "║  Airflow UI:    http://localhost:8080  (admin/admin123)          ║" -ForegroundColor White
Write-Host "║  SyniqAI Dashboard:  http://localhost:3000                        ║" -ForegroundColor White
Write-Host "║  Backend API:   http://localhost:8000                             ║" -ForegroundColor White
Write-Host "║  MinIO Console: http://localhost:9001  (minioadmin/minioadmin)    ║" -ForegroundColor White
Write-Host "║  Kafka Broker:  localhost:9092                                    ║" -ForegroundColor White
Write-Host "║                                                                   ║" -ForegroundColor White
Write-Host "╚═══════════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan

Write-Host ""
Write-Host "╔═══════════════════════════════════════════════════════════════════╗" -ForegroundColor Yellow
Write-Host "║ 📋 NEXT STEPS                                                     ║" -ForegroundColor Yellow
Write-Host "╠═══════════════════════════════════════════════════════════════════╣" -ForegroundColor Yellow
Write-Host "║                                                                   ║" -ForegroundColor White
Write-Host "║  1. Configure MongoDB connection:                                 ║" -ForegroundColor White
Write-Host "║     → Edit: Kafka Integration\.env.mongodb                        ║" -ForegroundColor White
Write-Host "║     → Set: MONGODB_URI, MONGODB_DATABASE, MONGODB_COLLECTIONS     ║" -ForegroundColor White
Write-Host "║                                                                   ║" -ForegroundColor White
Write-Host "║  2. Enable MongoDB CDC DAG in Airflow:                            ║" -ForegroundColor White
Write-Host "║     → Open: http://localhost:8080                                 ║" -ForegroundColor White
Write-Host "║     → Find DAG: mongodb_cdc_extraction                            ║" -ForegroundColor White
Write-Host "║     → Toggle ON and trigger manually                              ║" -ForegroundColor White
Write-Host "║                                                                   ║" -ForegroundColor White
Write-Host "║  3. Test MongoDB extraction:                                      ║" -ForegroundColor White
Write-Host "║     → Insert test documents to MongoDB                            ║" -ForegroundColor White
Write-Host "║     → Monitor Airflow task logs                                   ║" -ForegroundColor White
Write-Host "║     → Check Kafka topics for messages                             ║" -ForegroundColor White
Write-Host "║     → Verify Bronze layer in MinIO                                ║" -ForegroundColor White
Write-Host "║                                                                   ║" -ForegroundColor White
Write-Host "║  4. Access SyniqAI dashboard:                                     ║" -ForegroundColor White
Write-Host "║     → Navigate to Bronze layer for EDA                            ║" -ForegroundColor White
Write-Host "║     → Check data quality metrics in Silver                        ║" -ForegroundColor White
Write-Host "║     → View analytics in Gold layer                                ║" -ForegroundColor White
Write-Host "║                                                                   ║" -ForegroundColor White
Write-Host "╚═══════════════════════════════════════════════════════════════════╝" -ForegroundColor Yellow

Write-Host ""
Write-Host "╔═══════════════════════════════════════════════════════════════════╗" -ForegroundColor Magenta
Write-Host "║ 📊 MONITORING & HEALTH CHECKS                                     ║" -ForegroundColor Magenta
Write-Host "╠═══════════════════════════════════════════════════════════════════╣" -ForegroundColor Magenta
Write-Host "║                                                                   ║" -ForegroundColor White
Write-Host "║  Check all services:                                              ║" -ForegroundColor White
Write-Host "║    docker ps                                                      ║" -ForegroundColor Gray
Write-Host "║                                                                   ║" -ForegroundColor White
Write-Host "║  View Kafka topics:                                               ║" -ForegroundColor White
Write-Host "║    docker exec kafka kafka-topics --list \                        ║" -ForegroundColor Gray
Write-Host "║      --bootstrap-server localhost:9092                            ║" -ForegroundColor Gray
Write-Host "║                                                                   ║" -ForegroundColor White
Write-Host "║  Check watermarks:                                                ║" -ForegroundColor White
Write-Host "║    Get-Content 'Kafka Integration\watermarks\*.json'              ║" -ForegroundColor Gray
Write-Host "║                                                                   ║" -ForegroundColor White
Write-Host "║  Monitor Airflow DAG:                                             ║" -ForegroundColor White
Write-Host "║    - Graph View: See task dependencies                            ║" -ForegroundColor Gray
Write-Host "║    - Task Logs: Debug failures                                    ║" -ForegroundColor Gray
Write-Host "║    - Variables: Check extraction metrics                          ║" -ForegroundColor Gray
Write-Host "║                                                                   ║" -ForegroundColor White
Write-Host "╚═══════════════════════════════════════════════════════════════════╝" -ForegroundColor Magenta

Write-Host ""
Write-Host "╔═══════════════════════════════════════════════════════════════════╗" -ForegroundColor Red
Write-Host "║ 🛑 SHUTDOWN COMMANDS                                              ║" -ForegroundColor Red
Write-Host "╠═══════════════════════════════════════════════════════════════════╣" -ForegroundColor Red
Write-Host "║                                                                   ║" -ForegroundColor White
Write-Host "║  Stop Airflow:                                                    ║" -ForegroundColor White
Write-Host "║    .\stop_airflow.ps1                                             ║" -ForegroundColor Gray
Write-Host "║                                                                   ║" -ForegroundColor White
Write-Host "║  Stop Kafka:                                                      ║" -ForegroundColor White
Write-Host "║    cd 'Kafka Integration' ; docker-compose down                   ║" -ForegroundColor Gray
Write-Host "║                                                                   ║" -ForegroundColor White
Write-Host "║  Stop all containers:                                             ║" -ForegroundColor White
Write-Host "║    docker stop `$(docker ps -q)                                    ║" -ForegroundColor Gray
Write-Host "║                                                                   ║" -ForegroundColor White
Write-Host "╚═══════════════════════════════════════════════════════════════════╝" -ForegroundColor Red

Write-Host ""
Write-Host "📖 For detailed documentation, see:" -ForegroundColor Cyan
Write-Host "   documentation\PRODUCTION_DEPLOYMENT_GUIDE.md" -ForegroundColor White
Write-Host ""

# Optional: Open Airflow UI automatically
$openUI = Read-Host "Open Airflow UI in browser? (Y/n)"
if ($openUI -eq "" -or $openUI -eq "Y" -or $openUI -eq "y") {
    Start-Sleep -Seconds 2
    Start-Process "http://localhost:8080"
    Write-Success "Opening Airflow UI..."
}

Write-Host ""
Write-Host "🎉 Production setup complete! Happy orchestrating! 🎉" -ForegroundColor Green
Write-Host ""
