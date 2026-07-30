<#
.SYNOPSIS
    Starts the SyniqAI ETL platform (PostgreSQL CDC use case) end-to-end:
    MinIO -> Kafka/Debezium (Docker) -> Debezium connector -> Spark CDC consumer
    -> FastAPI backend -> React frontend.

.NOTES
    Run from an elevated-capable PowerShell (Docker Desktop restart uses UAC only if
    Docker isn't already running). Re-running this script is safe: it skips steps
    that are already satisfied (containers already up, connector already registered).
#>

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot

# --- Fixed environment values (must match .env / MinIO buckets / Postgres setup) ---
$JavaHome    = "C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
$HadoopHome  = "$env:LOCALAPPDATA\Temp\hadoop"
$MinioBin    = "C:\minio"
$MinioData   = "C:\minio\data"
$MinioUser   = "admin"
$MinioPass   = "password123"
$PgSuperPass = "postgres"
$Venv        = Join-Path $root ".venv-run"

function Write-Step($msg) { Write-Host "`n=== $msg ===" -ForegroundColor Cyan }

# Make sure this session's PATH includes Docker + Java + Hadoop native libs
$env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" +
            [System.Environment]::GetEnvironmentVariable("Path","User") + ";" +
            "$JavaHome\bin;$HadoopHome\bin;C:\Program Files\Docker\Docker\resources\bin"
$env:JAVA_HOME = $JavaHome
$env:HADOOP_HOME = $HadoopHome
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"

# ---------------------------------------------------------------------------
Write-Step "1/7 PostgreSQL service"
$pg = Get-Service -Name "postgresql-x64-18" -ErrorAction SilentlyContinue
if ($pg -and $pg.Status -ne "Running") {
    Start-Service -Name "postgresql-x64-18"
    Start-Sleep -Seconds 3
}
Write-Host "PostgreSQL: $((Get-Service postgresql-x64-18).Status)"

# ---------------------------------------------------------------------------
Write-Step "2/7 MinIO"
if (-not (Get-NetTCPConnection -LocalPort 9000 -ErrorAction SilentlyContinue)) {
    $env:MINIO_ROOT_USER = $MinioUser
    $env:MINIO_ROOT_PASSWORD = $MinioPass
    Start-Process -FilePath "$MinioBin\minio.exe" `
        -ArgumentList "server $MinioData --console-address :9001" `
        -WindowStyle Hidden `
        -RedirectStandardOutput "$MinioBin\minio_stdout.log" `
        -RedirectStandardError "$MinioBin\minio_stderr.log"
    Start-Sleep -Seconds 5
} else {
    Write-Host "MinIO already running."
}
try {
    (Invoke-WebRequest -Uri "http://localhost:9000/minio/health/live" -UseBasicParsing -TimeoutSec 5).StatusCode | Out-Null
    Write-Host "MinIO: healthy"
} catch { Write-Warning "MinIO health check failed: $_" }

# Ensure buckets exist (idempotent)
& "$MinioBin\mc.exe" alias set syniqai http://localhost:9000 $MinioUser $MinioPass 2>$null | Out-Null
foreach ($b in @("syniqai-bronze","syniqai-silver","syniqai-gold","syniqai-quarantine")) {
    try { & "$MinioBin\mc.exe" mb "syniqai/$b" 2>$null | Out-Null } catch { }
}

# ---------------------------------------------------------------------------
Write-Step "3/7 Docker Desktop + Kafka/Debezium stack"
$dockerUp = $false
try { docker ps *> $null; $dockerUp = $true } catch { $dockerUp = $false }
if (-not $dockerUp) {
    Write-Host "Starting Docker Desktop (first boot can take 1-2 minutes)..."
    Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe"
    $tries = 0
    do {
        Start-Sleep -Seconds 5
        $tries++
        try { docker ps *> $null; $dockerUp = $true } catch { $dockerUp = $false }
    } while (-not $dockerUp -and $tries -lt 36)
}
if (-not $dockerUp) { throw "Docker did not become ready in time." }
Write-Host "Docker: ready"

Push-Location (Join-Path $root "Kafka Integration")
docker compose up -d
Pop-Location

Write-Host "Waiting for Kafka Connect REST API..."
$tries = 0
$connectUp = $false
do {
    Start-Sleep -Seconds 5
    $tries++
    try {
        $r = Invoke-WebRequest -Uri "http://localhost:8083/" -UseBasicParsing -TimeoutSec 5
        if ($r.StatusCode -eq 200) { $connectUp = $true }
    } catch { $connectUp = $false }
} while (-not $connectUp -and $tries -lt 30)
if (-not $connectUp) { throw "Kafka Connect did not come up in time." }
Write-Host "Kafka Connect: ready"

# ---------------------------------------------------------------------------
Write-Step "4/7 Debezium PostgreSQL connector"
$status = try { Invoke-RestMethod -Uri "http://localhost:8083/connectors/postgres-cdc-connector/status" -TimeoutSec 5 } catch { $null }
if ($null -eq $status) {
    Push-Location (Join-Path $root "Kafka Integration")
    & "$Venv\Scripts\python.exe" setup_cdc_connectors.py --source postgres
    Pop-Location
    Start-Sleep -Seconds 5
    $status = try { Invoke-RestMethod -Uri "http://localhost:8083/connectors/postgres-cdc-connector/status" -TimeoutSec 5 } catch { $null }
}
if ($status -and $status.tasks[0].state -ne "RUNNING") {
    Write-Warning "Connector task state: $($status.tasks[0].state) -- attempting restart"
    Invoke-RestMethod -Method Post -Uri "http://localhost:8083/connectors/postgres-cdc-connector/restart?includeTasks=true" | Out-Null
    Start-Sleep -Seconds 8
    $status = Invoke-RestMethod -Uri "http://localhost:8083/connectors/postgres-cdc-connector/status" -TimeoutSec 5
}
Write-Host "Connector state: $($status.connector.state) / task0: $($status.tasks[0].state)"

# ---------------------------------------------------------------------------
Write-Step "5/7 Spark CDC consumer (Kafka -> Bronze Iceberg)"
$alreadyRunning = Get-Process pyspark,java -ErrorAction SilentlyContinue
$pidFile = Join-Path $root "Kafka Integration\cdc_consumer.pid"
if (Test-Path $pidFile) { Remove-Item $pidFile -Force }

$env:MINIO_ENDPOINT = "localhost:9000"
$env:MINIO_ACCESS_KEY = $MinioUser
$env:MINIO_SECRET_KEY = $MinioPass
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

Push-Location (Join-Path $root "Kafka Integration")
Start-Process -FilePath "$Venv\Scripts\python.exe" `
    -ArgumentList "spark_cdc_consumer.py --source postgres --warehouse s3a://syniqai-bronze/iceberg" `
    -WindowStyle Hidden `
    -RedirectStandardOutput "$root\logs\cdc_consumer.log" `
    -RedirectStandardError "$root\logs\cdc_consumer.err.log"
Pop-Location
Write-Host "Spark CDC consumer launched (log: logs\cdc_consumer.log). First run re-resolves Maven JARs only if the Ivy cache was cleared."

# ---------------------------------------------------------------------------
Write-Step "6/7 FastAPI backend"
if (-not (Get-NetTCPConnection -LocalPort 8000 -ErrorAction SilentlyContinue)) {
    Push-Location (Join-Path $root "gui\api")
    Start-Process -FilePath "$Venv\Scripts\python.exe" `
        -ArgumentList "backend.py" `
        -WindowStyle Hidden `
        -RedirectStandardOutput "$root\logs\backend.log" `
        -RedirectStandardError "$root\logs\backend.err.log"
    Pop-Location
    Start-Sleep -Seconds 12
} else {
    Write-Host "Backend already running."
}
try {
    (Invoke-WebRequest -Uri "http://localhost:8000/" -UseBasicParsing -TimeoutSec 5).StatusCode | Out-Null
    Write-Host "Backend: healthy (http://localhost:8000, docs at /docs)"
} catch { Write-Warning "Backend not responding yet -- check logs\backend.log" }

# ---------------------------------------------------------------------------
Write-Step "7/7 React frontend"
if (-not (Get-NetTCPConnection -LocalPort 3000 -ErrorAction SilentlyContinue)) {
    Push-Location (Join-Path $root "gui")
    Start-Process -FilePath "npm.cmd" -ArgumentList "run dev" `
        -WindowStyle Hidden `
        -RedirectStandardOutput "$root\logs\frontend.log" `
        -RedirectStandardError "$root\logs\frontend.err.log"
    Pop-Location
    Start-Sleep -Seconds 8
} else {
    Write-Host "Frontend already running."
}
try {
    (Invoke-WebRequest -Uri "http://localhost:3000" -UseBasicParsing -TimeoutSec 5).StatusCode | Out-Null
    Write-Host "Frontend: healthy (http://localhost:3000)"
} catch { Write-Warning "Frontend not responding yet -- check logs\frontend.log" }

Write-Host "`n============================================================" -ForegroundColor Green
Write-Host " SyniqAI platform is up:" -ForegroundColor Green
Write-Host "   Frontend:        http://localhost:3000"
Write-Host "   Backend API:     http://localhost:8000  (docs: /docs)"
Write-Host "   Kafka UI:        http://localhost:8080"
Write-Host "   Kafka Connect:   http://localhost:8083"
Write-Host "   MinIO Console:   http://localhost:9001  ($MinioUser / $MinioPass)"
Write-Host "   PostgreSQL:      localhost:5432 (syniqai_user / syniqai_password, db syniqai_metadata)"
Write-Host "============================================================" -ForegroundColor Green
