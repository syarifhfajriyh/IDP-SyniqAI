# ============================================================================
# MongoDB CDC - Status Check & Health Monitor
# ============================================================================
# Quick health check script for all MongoDB CDC components

$ErrorActionPreference = "Continue"

Write-Host ""
Write-Host "╔═══════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║     MongoDB CDC - System Status Check                        ║" -ForegroundColor Cyan
Write-Host "╚═══════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

$PROJECT_ROOT = Split-Path -Parent $PSScriptRoot
$KAFKA_INTEGRATION = "$PROJECT_ROOT\Kafka Integration"

function Test-Service {
    param($name, $test, $successMsg, $failMsg)
    
    Write-Host "[$name]" -ForegroundColor Yellow -NoNewline
    Write-Host " Checking..." -NoNewline
    
    try {
        $result = Invoke-Command -ScriptBlock $test
        if ($result) {
            Write-Host " ✓" -ForegroundColor Green
            Write-Host "  $successMsg" -ForegroundColor Gray
            return $true
        } else {
            Write-Host " ✗" -ForegroundColor Red
            Write-Host "  $failMsg" -ForegroundColor Gray
            return $false
        }
    } catch {
        Write-Host " ✗" -ForegroundColor Red
        Write-Host "  $failMsg" -ForegroundColor Gray
        Write-Host "  Error: $($_.Exception.Message)" -ForegroundColor DarkGray
        return $false
    }
}

$allHealthy = $true

# ============================================================================
# 1. Check Airflow Services
# ============================================================================

Write-Host ""
Write-Host "━━━ AIRFLOW ORCHESTRATION ━━━" -ForegroundColor Cyan

$airflowWebserver = Test-Service "Webserver" {
    $proc = Get-Process python -ErrorAction SilentlyContinue | Where-Object {$_.CommandLine -like "*airflow*webserver*"}
    return $null -ne $proc
} "Airflow UI running on port 8080" "Webserver not running - run .\start_airflow.ps1"

$airflowScheduler = Test-Service "Scheduler" {
    $proc = Get-Process python -ErrorAction SilentlyContinue | Where-Object {$_.CommandLine -like "*airflow*scheduler*"}
    return $null -ne $proc
} "Scheduler processing DAGs" "Scheduler not running"

$airflowDB = Test-Service "Database" {
    Test-Path "C:\airflow\airflow.db"
} "SQLite database initialized" "Database not found - run 'airflow db init'"

$airflowDAG = Test-Service "MongoDB DAG" {
    Test-Path "C:\airflow\dags\mongodb_cdc_dag.py"
} "DAG file deployed" "DAG not found - copy from airflow_dags folder"

$allHealthy = $allHealthy -and $airflowWebserver -and $airflowScheduler -and $airflowDB -and $airflowDAG

# ============================================================================
# 2. Check MongoDB Connection
# ============================================================================

Write-Host ""
Write-Host "━━━ MONGODB DATABASE ━━━" -ForegroundColor Cyan

# Load MongoDB URI from .env
$envFile = "$KAFKA_INTEGRATION\.env.mongodb"
if (Test-Path $envFile) {
    $mongoUri = (Get-Content $envFile | Select-String "^MONGODB_URI=").Line -replace "MONGODB_URI=", ""
    $mongoDatabase = (Get-Content $envFile | Select-String "^MONGODB_DATABASE=").Line -replace "MONGODB_DATABASE=", ""
    
    $mongoConnection = Test-Service "Connection" {
        # Test using Python pymongo
        python -c "from pymongo import MongoClient; client = MongoClient('$mongoUri', serverSelectionTimeoutMS=5000); client.admin.command('ping'); print('OK')" 2>&1 | Select-String "OK"
    } "Connected to: $mongoUri" "Cannot connect to MongoDB"
    
    if ($mongoConnection) {
        $mongoCollections = Test-Service "Collections" {
            python -c "from pymongo import MongoClient; client = MongoClient('$mongoUri'); db = client['$mongoDatabase']; collections = db.list_collection_names(); print(f'{len(collections)} collections')" 2>&1
        } "Database: $mongoDatabase" "Cannot list collections"
    }
    
    $allHealthy = $allHealthy -and $mongoConnection
} else {
    Write-Host "[MongoDB]" -ForegroundColor Yellow -NoNewline
    Write-Host " ✗ Configuration not found" -ForegroundColor Red
    Write-Host "  Create: $envFile" -ForegroundColor Gray
    $allHealthy = $false
}

# ============================================================================
# 3. Check Kafka Infrastructure
# ============================================================================

Write-Host ""
Write-Host "━━━ KAFKA MESSAGING ━━━" -ForegroundColor Cyan

$kafkaContainer = Test-Service "Kafka Broker" {
    $container = docker ps --format "{{.Names}}" | Select-String "^kafka$"
    return $null -ne $container
} "Kafka container running" "Kafka not running - run docker-compose up in Kafka Integration"

if ($kafkaContainer) {
    $kafkaConnectivity = Test-Service "Connectivity" {
        docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 2>&1 | Select-String "ApiVersion"
    } "Accessible on localhost:9092" "Cannot connect to Kafka broker"
    
    $kafkaTopics = Test-Service "MongoDB Topics" {
        $topics = docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>&1 | Select-String "airflow.mongodb"
        return $null -ne $topics
    } "CDC topics exist" "No MongoDB CDC topics yet (will be created on first run)"
}

$allHealthy = $allHealthy -and $kafkaContainer

# ============================================================================
# 4. Check MinIO Storage
# ============================================================================

Write-Host ""
Write-Host "━━━ MINIO STORAGE ━━━" -ForegroundColor Cyan

$minioContainer = Test-Service "MinIO Server" {
    $container = docker ps --format "{{.Names}}" | Select-String "minio"
    return $null -ne $container
} "MinIO running on ports 9000/9001" "MinIO not running"

if ($minioContainer) {
    $minioBuckets = Test-Service "Bronze Bucket" {
        python -c "import boto3; s3 = boto3.client('s3', endpoint_url='http://localhost:9000', aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin'); buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]; print('bronze' in buckets)" 2>&1 | Select-String "True"
    } "Bronze layer ready" "Bronze bucket not found"
}

# ============================================================================
# 5. Check Watermark Status
# ============================================================================

Write-Host ""
Write-Host "━━━ WATERMARK TRACKING ━━━" -ForegroundColor Cyan

$watermarkFile = "$KAFKA_INTEGRATION\watermarks\mongodb_watermarks.json"

$watermarkExists = Test-Service "Watermark File" {
    Test-Path $watermarkFile
} "Watermarks stored at: $watermarkFile" "No watermarks yet (normal on first run)"

if ($watermarkExists) {
    Write-Host ""
    Write-Host "  Current Watermarks:" -ForegroundColor Cyan
    try {
        $watermarks = Get-Content $watermarkFile | ConvertFrom-Json
        $watermarks.PSObject.Properties | ForEach-Object {
            $collection = $_.Name
            $data = $_.Value
            Write-Host "    ├─ $collection" -ForegroundColor White
            Write-Host "    │  ├─ Last Watermark: $($data.last_watermark)" -ForegroundColor Gray
            Write-Host "    │  ├─ Documents: $($data.documents_processed)" -ForegroundColor Gray
            Write-Host "    │  └─ Updated: $($data.last_updated)" -ForegroundColor Gray
        }
    } catch {
        Write-Host "    └─ Error reading watermarks: $($_.Exception.Message)" -ForegroundColor Red
    }
}

# ============================================================================
# 6. Check Backend & Frontend
# ============================================================================

Write-Host ""
Write-Host "━━━ APPLICATION SERVICES ━━━" -ForegroundColor Cyan

$backend = Test-Service "Backend API" {
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8000/health" -TimeoutSec 2 -ErrorAction SilentlyContinue
        return $response.StatusCode -eq 200
    } catch {
        return $false
    }
} "FastAPI running on port 8000" "Backend not running - start with: python gui/app.py"

$frontend = Test-Service "Frontend UI" {
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:3000" -TimeoutSec 2 -ErrorAction SilentlyContinue
        return $response.StatusCode -eq 200
    } catch {
        return $false
    }
} "React dev server on port 3000" "Frontend not running - start with: npm run dev (in gui folder)"

# ============================================================================
# Summary
# ============================================================================

Write-Host ""
Write-Host "╔═══════════════════════════════════════════════════════════════╗" -ForegroundColor $(if ($allHealthy) { "Green" } else { "Yellow" })
Write-Host "║  SYSTEM STATUS SUMMARY                                        ║" -ForegroundColor $(if ($allHealthy) { "Green" } else { "Yellow" })
Write-Host "╚═══════════════════════════════════════════════════════════════╝" -ForegroundColor $(if ($allHealthy) { "Green" } else { "Yellow" })
Write-Host ""

if ($allHealthy) {
    Write-Host "✓ All critical services are healthy!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Quick Links:" -ForegroundColor Cyan
    Write-Host "  • Airflow UI:     http://localhost:8080" -ForegroundColor White
    Write-Host "  • SyniqAI Dashboard:   http://localhost:3000" -ForegroundColor White
    Write-Host "  • MinIO Console:  http://localhost:9001" -ForegroundColor White
    Write-Host ""
} else {
    Write-Host "⚠ Some services need attention" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Next Steps:" -ForegroundColor Cyan
    Write-Host "  1. Review failed checks above" -ForegroundColor White
    Write-Host "  2. Start missing services" -ForegroundColor White
    Write-Host "  3. Run this script again to verify" -ForegroundColor White
    Write-Host ""
}

Write-Host "For detailed logs, check:" -ForegroundColor Gray
Write-Host "  • Airflow: C:\airflow\logs\" -ForegroundColor DarkGray
Write-Host "  • Kafka: docker logs kafka" -ForegroundColor DarkGray
Write-Host "  • Backend: gui\logs\" -ForegroundColor DarkGray
Write-Host ""
