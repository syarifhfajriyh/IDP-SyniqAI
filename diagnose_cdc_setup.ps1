# ============================================================================
# CDC Setup Diagnostic Tool
# ============================================================================
# This script checks all prerequisites for CDC to work from remote PostgreSQL

param(
    [Parameter(Mandatory=$false)]
    [string]$RemoteHost = "192.168.2.114",
    
    [Parameter(Mandatory=$false)]
    [int]$RemotePort = 5432,
    
    [Parameter(Mandatory=$false)]
    [string]$Database = "postgres",
    
    [Parameter(Mandatory=$false)]
    [string]$User = "debezium_user",
    
    [Parameter(Mandatory=$false)]
    [string]$Password = "debezium_password"
)

$ErrorActionPreference = "Continue"

Write-Host ""
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host "  CDC Setup Diagnostic Tool" -ForegroundColor Cyan
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""

$issues = @()
$checks = 0
$passed = 0

# ============================================================================
# Check 1: Network Connectivity
# ============================================================================
Write-Host "[1/6] Testing network connectivity to remote PostgreSQL..." -ForegroundColor Yellow
$checks++

try {
    $tcpTest = Test-NetConnection -ComputerName $RemoteHost -Port $RemotePort -WarningAction SilentlyContinue
    if ($tcpTest.TcpTestSucceeded) {
        Write-Host "  [OK] Can reach $RemoteHost`:$RemotePort" -ForegroundColor Green
        $passed++
    } else {
        Write-Host "  [ERROR] Cannot reach $RemoteHost`:$RemotePort" -ForegroundColor Red
        $issues += "Network connectivity failed. Check firewall and PostgreSQL pg_hba.conf"
    }
} catch {
    Write-Host "  [ERROR] Network test failed: $_" -ForegroundColor Red
    $issues += "Network connectivity test error"
}

Write-Host ""

# ============================================================================
# Check 2: Kafka Connect Running
# ============================================================================
Write-Host "[2/6] Checking Kafka Connect status..." -ForegroundColor Yellow
$checks++

try {
    $response = Invoke-WebRequest -Uri "http://localhost:8083/" -UseBasicParsing -TimeoutSec 5
    if ($response.StatusCode -eq 200) {
        Write-Host "  [OK] Kafka Connect is running" -ForegroundColor Green
        $passed++
    }
} catch {
    Write-Host "  [ERROR] Kafka Connect is not running" -ForegroundColor Red
    $issues += "Start Kafka Connect: docker-compose up -d kafka-connect"
}

Write-Host ""

# ============================================================================
# Check 3: Existing Connectors
# ============================================================================
Write-Host "[3/6] Checking for existing Debezium connectors..." -ForegroundColor Yellow
$checks++

try {
    $connectors = Invoke-WebRequest -Uri "http://localhost:8083/connectors" -UseBasicParsing | 
                  Select-Object -ExpandProperty Content | 
                  ConvertFrom-Json
    
    if ($connectors.Count -eq 0) {
        Write-Host "  [INFO] No connectors registered yet" -ForegroundColor Yellow
        Write-Host "        This is why CDC messages aren't being captured!" -ForegroundColor Yellow
        $issues += "No Debezium connector created - run setup_cdc_connectors.py"
    } else {
        Write-Host "  [OK] Found $($connectors.Count) connector(s):" -ForegroundColor Green
        foreach ($conn in $connectors) {
            Write-Host "        - $conn" -ForegroundColor Gray
        }
        $passed++
    }
} catch {
    Write-Host "  [ERROR] Failed to query connectors" -ForegroundColor Red
}

Write-Host ""

# ============================================================================
# Check 4: PostgreSQL Database Connection
# ============================================================================
Write-Host "[4/6] Testing PostgreSQL database connection..." -ForegroundColor Yellow
$checks++

# Create temporary Python script to test connection
$testScript = @"
import sys
try:
    import psycopg2
    conn = psycopg2.connect(
        host='$RemoteHost',
        port=$RemotePort,
        database='$Database',
        user='$User',
        password='$Password',
        connect_timeout=5
    )
    print('CONNECTION_SUCCESS')
    conn.close()
    sys.exit(0)
except ImportError:
    print('ERROR: psycopg2 not installed')
    sys.exit(1)
except Exception as e:
    print(f'CONNECTION_FAILED: {e}')
    sys.exit(1)
"@

$testScript | Out-File -FilePath ".\temp_pg_test.py" -Encoding UTF8

try {
    $result = python .\temp_pg_test.py 2>&1 | Out-String
    if ($result -like "*CONNECTION_SUCCESS*") {
        Write-Host "  [OK] Can connect to PostgreSQL as $User" -ForegroundColor Green
        $passed++
    } elseif ($result -like "*psycopg2 not installed*") {
        Write-Host "  [ERROR] psycopg2 Python package not installed" -ForegroundColor Red
        $issues += "Install psycopg2: pip install psycopg2-binary"
    } else {
        Write-Host "  [ERROR] Cannot connect to PostgreSQL" -ForegroundColor Red
        Write-Host "        $result" -ForegroundColor Gray
        $issues += "PostgreSQL connection failed. Check credentials and pg_hba.conf on remote laptop"
    }
} catch {
    Write-Host "  [ERROR] Connection test failed" -ForegroundColor Red
}

Remove-Item -Path ".\temp_pg_test.py" -ErrorAction SilentlyContinue

Write-Host ""

# ============================================================================
# Check 5: PostgreSQL WAL Configuration
# ============================================================================
Write-Host "[5/6] Checking PostgreSQL WAL configuration..." -ForegroundColor Yellow
$checks++

$walCheckScript = @"
import sys
try:
    import psycopg2
    conn = psycopg2.connect(
        host='$RemoteHost',
        port=$RemotePort,
        database='$Database',
        user='postgres',  # Need superuser to check settings
        password='8520',  # Default password from your setup
        connect_timeout=5
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT name, setting 
        FROM pg_settings 
        WHERE name IN ('wal_level', 'max_wal_senders', 'max_replication_slots')
    """)
    settings = dict(cur.fetchall())
    
    print(f"wal_level={settings.get('wal_level', 'unknown')}")
    print(f"max_wal_senders={settings.get('max_wal_senders', 'unknown')}")
    print(f"max_replication_slots={settings.get('max_replication_slots', 'unknown')}")
    
    conn.close()
except Exception as e:
    print(f"ERROR:{e}")
"@

$walCheckScript | Out-File -FilePath ".\temp_wal_check.py" -Encoding UTF8

try {
    $walResult = python .\temp_wal_check.py 2>&1 | Out-String
    
    if ($walResult -like "*ERROR:*") {
        Write-Host "  [WARN] Cannot check WAL settings (need postgres superuser)" -ForegroundColor Yellow
        Write-Host "        $walResult" -ForegroundColor Gray
    } else {
        $walLevel = if ($walResult -match "wal_level=(\w+)") { $matches[1] } else { "unknown" }
        $walSenders = if ($walResult -match "max_wal_senders=(\d+)") { $matches[1] } else { "0" }
        $walSlots = if ($walResult -match "max_replication_slots=(\d+)") { $matches[1] } else { "0" }
        
        Write-Host "  Current settings:" -ForegroundColor Gray
        Write-Host "    wal_level: $walLevel (need: logical)" -ForegroundColor Gray
        Write-Host "    max_wal_senders: $walSenders (need: >= 4)" -ForegroundColor Gray
        Write-Host "    max_replication_slots: $walSlots (need: >= 4)" -ForegroundColor Gray
        
        if ($walLevel -eq "logical" -and [int]$walSenders -ge 4 -and [int]$walSlots -ge 4) {
            Write-Host "  [OK] WAL configuration is correct" -ForegroundColor Green
            $passed++
        } else {
            Write-Host "  [ERROR] WAL configuration needs updating" -ForegroundColor Red
            $issues += "Run setup_postgres_wal.sql on the remote PostgreSQL database"
        }
    }
} catch {
    Write-Host "  [WARN] Could not verify WAL settings" -ForegroundColor Yellow
}

Remove-Item -Path ".\temp_wal_check.py" -ErrorAction SilentlyContinue

Write-Host ""

# ============================================================================
# Check 6: Kafka Topics
# ============================================================================
Write-Host "[6/6] Checking Kafka topics..." -ForegroundColor Yellow
$checks++

try {
    $containers = docker ps --filter "name=kafka" --format "{{.Names}}"
    if ($containers -like "*kafka*") {
        Write-Host "  [OK] Kafka container is running" -ForegroundColor Green
        $passed++
    } else {
        Write-Host "  [ERROR] Kafka container not found" -ForegroundColor Red
        $issues += "Start Kafka: docker-compose up -d"
    }
} catch {
    Write-Host "  [ERROR] Cannot check Kafka status" -ForegroundColor Red
}

Write-Host ""

# ============================================================================
# Summary and Next Steps
# ============================================================================
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host "  Diagnostic Summary" -ForegroundColor Cyan
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Checks passed: $passed / $checks" -ForegroundColor $(if ($passed -eq $checks) { "Green" } else { "Yellow" })
Write-Host ""

if ($issues.Count -gt 0) {
    Write-Host "Issues found:" -ForegroundColor Red
    for ($i = 0; $i -lt $issues.Count; $i++) {
        Write-Host "  $($i+1). $($issues[$i])" -ForegroundColor Yellow
    }
    Write-Host ""
}

Write-Host "Next Steps:" -ForegroundColor Cyan
Write-Host ""

if ($issues -like "*No Debezium connector*") {
    Write-Host "  PRIMARY ISSUE: No CDC connector configured!" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  To fix this, you need to:" -ForegroundColor White
    Write-Host "    1. On the REMOTE laptop (where PostgreSQL is running):" -ForegroundColor Gray
    Write-Host "       - Edit postgresql.conf and set:" -ForegroundColor Gray
    Write-Host "         wal_level = logical" -ForegroundColor Gray
    Write-Host "         max_wal_senders = 10" -ForegroundColor Gray
    Write-Host "         max_replication_slots = 10" -ForegroundColor Gray
    Write-Host "       - Edit pg_hba.conf and add:" -ForegroundColor Gray
    Write-Host "         host replication debezium_user 0.0.0.0/0 md5" -ForegroundColor Gray
    Write-Host "       - Restart PostgreSQL service" -ForegroundColor Gray
    Write-Host "       - Run: psql -U postgres -d postgres -f setup_postgres_wal.sql" -ForegroundColor Gray
    Write-Host ""
    Write-Host "    2. On THIS laptop (where SyniqAI is running):" -ForegroundColor Gray
    Write-Host "       - Verify connectivity with this diagnostic tool" -ForegroundColor Gray
    Write-Host "       - Run: cd 'Kafka Integration'" -ForegroundColor Gray
    Write-Host "       - Run: python setup_cdc_connectors.py --source postgres" -ForegroundColor Gray
    Write-Host ""
}

Write-Host "  Quick verification commands:" -ForegroundColor White
Write-Host "    - Check connectors: Invoke-WebRequest http://localhost:8083/connectors -UseBasicParsing" -ForegroundColor Gray
Write-Host "    - Check topics: Browse to http://localhost:8080 (Kafka UI)" -ForegroundColor Gray
Write-Host "    - Test CDC: Insert data into cdc_test table on remote PostgreSQL" -ForegroundColor Gray
Write-Host ""
