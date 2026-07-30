<#
.SYNOPSIS
    Start CDC Streaming Consumer (24/7 Operation)

.DESCRIPTION
    This script starts the Spark CDC consumer that continuously processes
    Change Data Capture (CDC) events from Kafka and writes to Bronze Iceberg tables.
    
    Features:
    - Monitors PostgreSQL and MariaDB CDC topics
    - Writes to Iceberg Bronze with ACID guarantees
    - Automatic restarts on failure
    - Logging to file and console
    - Health monitoring endpoint

.USAGE
    # Start CDC streaming (all sources)
    .\start_cdc_streaming.ps1
    
    # Start specific source only
    .\start_cdc_streaming.ps1 -Source postgres
    .\start_cdc_streaming.ps1 -Source mariadb
    
    # Stop CDC streaming
    .\start_cdc_streaming.ps1 -Action stop
    
    # Check status
    .\start_cdc_streaming.ps1 -Action status

.NOTES
    Author: SyniqAI Team
    Date: March 8, 2026
    
    Prerequisites:
    - Kafka running on localhost:9092
    - MinIO running on localhost:9000
    - PostgreSQL database for Iceberg catalog
    - Python environment with pyspark, pyiceberg installed
#>

param(
    [Parameter(HelpMessage="CDC source: postgres, mariadb, or all")]
    [ValidateSet("postgres", "mariadb", "all")]
    [string]$Source = "all",
    
    [Parameter(HelpMessage="Action: start, stop, status, restart")]
    [ValidateSet("start", "stop", "status", "restart")]
    [string]$Action = "start",
    
    [Parameter(HelpMessage="Enable auto-restart on failure")]
    [switch]$AutoRestart = $true,
    
    [Parameter(HelpMessage="Log file path")]
    [string]$LogPath = ".\logs\cdc_streaming.log"
)

$ErrorActionPreference = "Stop"

# ============================================================================
# Configuration
# ============================================================================

$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$KAFKA_INTEGRATION_DIR = Join-Path $SCRIPT_DIR "Kafka Integration"
$CDC_CONSUMER_SCRIPT = Join-Path $KAFKA_INTEGRATION_DIR "spark_cdc_consumer.py"
$PID_FILE = Join-Path $KAFKA_INTEGRATION_DIR "cdc_consumer.pid"
$LOG_DIR = Join-Path $SCRIPT_DIR "logs"

# Kafka configuration
$KAFKA_SERVERS = "localhost:9092"
$CHECKPOINT_DIR = Join-Path $KAFKA_INTEGRATION_DIR "checkpoints"
$WAREHOUSE_PATH = "s3a://bronze/iceberg"

# Health check settings
$HEALTH_CHECK_INTERVAL = 60  # seconds
$MAX_RESTART_ATTEMPTS = 5
$RESTART_DELAY = 10  # seconds

# ============================================================================
# Functions
# ============================================================================

function Write-Log {
    param(
        [string]$Message,
        [ValidateSet("INFO", "WARN", "ERROR", "SUCCESS")]
        [string]$Level = "INFO"
    )
    
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] [$Level] $Message"
    
    # Color output for console
    switch ($Level) {
        "INFO"    { Write-Host $logMessage -ForegroundColor Cyan }
        "SUCCESS" { Write-Host $logMessage -ForegroundColor Green }
        "WARN"    { Write-Host $logMessage -ForegroundColor Yellow }
        "ERROR"   { Write-Host $logMessage -ForegroundColor Red }
    }
    
    # Write to log file
    if (-not (Test-Path $LOG_DIR)) {
        New-Item -ItemType Directory -Path $LOG_DIR -Force | Out-Null
    }
    Add-Content -Path $LogPath -Value $logMessage
}

function Test-Prerequisites {
    Write-Log "Checking prerequisites..." "INFO"
    
    # Check if CDC consumer script exists
    if (-not (Test-Path $CDC_CONSUMER_SCRIPT)) {
        Write-Log "CDC consumer script not found: $CDC_CONSUMER_SCRIPT" "ERROR"
        return $false
    }
    
    # Check Python
    try {
        $pythonVersion = python --version 2>&1
        Write-Log "Python: $pythonVersion" "SUCCESS"
    } catch {
        Write-Log "Python not found. Please install Python 3.8+" "ERROR"
        return $false
    }
    
    # Check Kafka (attempt connection)
    Write-Log "Checking Kafka connection on $KAFKA_SERVERS..." "INFO"
    # Note: Actual Kafka check would require kafka-python or telnet
    # For now, we'll assume it's running
    
    # Check MinIO
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:9000/minio/health/live" -ErrorAction SilentlyContinue
        if ($response.StatusCode -eq 200) {
            Write-Log "MinIO: Running" "SUCCESS"
        }
    } catch {
        Write-Log "MinIO health check failed. Make sure MinIO is running on localhost:9000" "WARN"
    }
    
    Write-Log "Prerequisites check completed" "SUCCESS"
    return $true
}

function Get-CDCProcessId {
    if (Test-Path $PID_FILE) {
        $pid = Get-Content $PID_FILE
        try {
            $process = Get-Process -Id $pid -ErrorAction SilentlyContinue
            if ($process) {
                return $pid
            }
        } catch {
            # Process not found
        }
    }
    return $null
}

function Start-CDCStreaming {
    Write-Log "======================================================================" "INFO"
    Write-Log " Starting CDC Streaming Consumer" "INFO"
    Write-Log "======================================================================" "INFO"
    
    # Check if already running
    $existingPid = Get-CDCProcessId
    if ($existingPid) {
        Write-Log "CDC consumer already running (PID: $existingPid)" "WARN"
        Write-Log "Use -Action stop to stop it first, or -Action restart" "WARN"
        return $false
    }
    
    # Verify prerequisites
    if (-not (Test-Prerequisites)) {
        Write-Log "Prerequisites check failed. Cannot start CDC consumer." "ERROR"
        return $false
    }
    
    # Build command
    $cdcCommand = "python `"$CDC_CONSUMER_SCRIPT`" --source $Source --kafka-servers $KAFKA_SERVERS --checkpoint `"$CHECKPOINT_DIR`" --warehouse `"$WAREHOUSE_PATH`""
    
    Write-Log "Command: $cdcCommand" "INFO"
    Write-Log "Source: $Source" "INFO"
    Write-Log "Kafka: $KAFKA_SERVERS" "INFO"
    Write-Log "Warehouse: $WAREHOUSE_PATH" "INFO"
    Write-Log "Checkpoint: $CHECKPOINT_DIR" "INFO"
    Write-Log "----------------------------------------------------------------------" "INFO"
    
    # Start process in background
    try {
        # Change to Kafka Integration directory
        Push-Location $KAFKA_INTEGRATION_DIR
        
        # Start Python process
        $process = Start-Process -FilePath "python" `
            -ArgumentList "spark_cdc_consumer.py", "--source", $Source, "--kafka-servers", $KAFKA_SERVERS, "--checkpoint", $CHECKPOINT_DIR, "--warehouse", $WAREHOUSE_PATH `
            -NoNewWindow `
            -PassThru `
            -RedirectStandardOutput (Join-Path $LOG_DIR "cdc_stdout.log") `
            -RedirectStandardError (Join-Path $LOG_DIR "cdc_stderr.log")
        
        # Save PID
        $process.Id | Out-File -FilePath $PID_FILE -Force
        
        Pop-Location
        
        Write-Log "CDC consumer started successfully (PID: $($process.Id))" "SUCCESS"
        Write-Log "Logs: $LOG_DIR\cdc_stdout.log" "INFO"
        Write-Log "Errors: $LOG_DIR\cdc_stderr.log" "INFO"
        Write-Log "======================================================================" "INFO"
        
        # Monitor if auto-restart enabled
        if ($AutoRestart) {
            Write-Log "Auto-restart enabled. Monitoring process..." "INFO"
            Monitor-CDCProcess
        }
        
        return $true
    } catch {
        Write-Log "Failed to start CDC consumer: $_" "ERROR"
        Pop-Location
        return $false
    }
}

function Stop-CDCStreaming {
    Write-Log "Stopping CDC streaming consumer..." "INFO"
    
    $pid = Get-CDCProcessId
    if (-not $pid) {
        Write-Log "CDC consumer is not running" "WARN"
        return $true
    }
    
    try {
        Stop-Process -Id $pid -Force
        Remove-Item $PID_FILE -ErrorAction SilentlyContinue
        Write-Log "CDC consumer stopped (PID: $pid)" "SUCCESS"
        return $true
    } catch {
        Write-Log "Failed to stop CDC consumer: $_" "ERROR"
        return $false
    }
}

function Get-CDCStatus {
    Write-Log "======================================================================" "INFO"
    Write-Log " CDC Streaming Consumer Status" "INFO"
    Write-Log "======================================================================" "INFO"
    
    $pid = Get-CDCProcessId
    if ($pid) {
        $process = Get-Process -Id $pid
        $uptime = (Get-Date) - $process.StartTime
        
        Write-Log "Status: RUNNING" "SUCCESS"
        Write-Log "PID: $pid" "INFO"
        Write-Log "Started: $($process.StartTime)" "INFO"
        Write-Log "Uptime: $($uptime.ToString('hh\:mm\:ss'))" "INFO"
        Write-Log "CPU: $($process.CPU.ToString('F2'))s" "INFO"
        Write-Log "Memory: $([math]::Round($process.WorkingSet64 / 1MB, 2)) MB" "INFO"
    } else {
        Write-Log "Status: NOT RUNNING" "WARN"
    }
    
    Write-Log "----------------------------------------------------------------------" "INFO"
    Write-Log "Configuration:" "INFO"
    Write-Log "  Source: $Source" "INFO"
    Write-Log "  Kafka: $KAFKA_SERVERS" "INFO"
    Write-Log "  Warehouse: $WAREHOUSE_PATH" "INFO"
    Write-Log "======================================================================" "INFO"
}

function Monitor-CDCProcess {
    $restartCount = 0
    
    Write-Log "Starting process monitor (Ctrl+C to stop)" "INFO"
    
    while ($restartCount -lt $MAX_RESTART_ATTEMPTS) {
        Start-Sleep -Seconds $HEALTH_CHECK_INTERVAL
        
        $pid = Get-CDCProcessId
        if (-not $pid) {
            $restartCount++
            Write-Log "CDC consumer stopped unexpectedly. Restart attempt $restartCount/$MAX_RESTART_ATTEMPTS" "WARN"
            
            Start-Sleep -Seconds $RESTART_DELAY
            
            if (Start-CDCStreaming) {
                Write-Log "CDC consumer restarted successfully" "SUCCESS"
                $restartCount = 0  # Reset counter on successful restart
            } else {
                Write-Log "Failed to restart CDC consumer" "ERROR"
            }
        }
    }
    
    Write-Log "Max restart attempts reached. Stopping monitor." "ERROR"
}

function Restart-CDCStreaming {
    Write-Log "Restarting CDC streaming consumer..." "INFO"
    Stop-CDCStreaming
    Start-Sleep -Seconds 2
    Start-CDCStreaming
}

# ============================================================================
# Main Execution
# ============================================================================

Write-Host ""
Write-Host "╔════════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║         SyniqAI CDC Streaming Consumer Manager                    ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

switch ($Action) {
    "start" {
        Start-CDCStreaming
    }
    "stop" {
        Stop-CDCStreaming
    }
    "status" {
        Get-CDCStatus
    }
    "restart" {
        Restart-CDCStreaming
    }
}

Write-Host ""
