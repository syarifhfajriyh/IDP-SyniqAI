# ============================================================================
# Test Remote PostgreSQL Connection for CDC
# ============================================================================
# Quick test script to verify connection to remote PostgreSQL before creating
# Debezium connector
#
# Usage: .\test_remote_postgres.ps1

param(
    [string]$RemoteHost = "192.168.0.11",
    [int]$Port = 5432,
    [string]$Database = "postgres",
    [string]$User = "debezium_user",
    [string]$Password = "debezium_password"
)

Write-Host ""
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host "  Remote PostgreSQL Connection Test" -ForegroundColor Cyan
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Target: $RemoteHost`:$Port" -ForegroundColor White
Write-Host "Database: $Database" -ForegroundColor White
Write-Host "User: $User" -ForegroundColor White
Write-Host ""

# ============================================================================
# Test 1: Network Connectivity
# ============================================================================
Write-Host "[1/4] Testing network connectivity..." -ForegroundColor Yellow

try {
    $tcpTest = Test-NetConnection -ComputerName $RemoteHost -Port $Port -WarningAction SilentlyContinue
    if ($tcpTest.TcpTestSucceeded) {
        Write-Host "  ✓ Network connection successful" -ForegroundColor Green
        Write-Host "    Ping: $($tcpTest.PingSucceeded)" -ForegroundColor Gray
        Write-Host "    TCP Port $Port`: Open" -ForegroundColor Gray
    } else {
        Write-Host "  ✗ Cannot reach $RemoteHost`:$Port" -ForegroundColor Red
        Write-Host ""
        Write-Host "Possible issues:" -ForegroundColor Yellow
        Write-Host "  - PostgreSQL service not running on remote laptop" -ForegroundColor Gray
        Write-Host "  - Firewall blocking port $Port" -ForegroundColor Gray
        Write-Host "  - Wrong IP address" -ForegroundColor Gray
        exit 1
    }
} catch {
    Write-Host "  ✗ Network test failed: $_" -ForegroundColor Red
    exit 1
}

Write-Host ""

# ============================================================================
# Test 2: PostgreSQL Connection (Regular)
# ============================================================================
Write-Host "[2/4] Testing PostgreSQL connection..." -ForegroundColor Yellow

$testScript = @"
import sys
import psycopg2

try:
    conn = psycopg2.connect(
        host='$RemoteHost',
        port=$Port,
        database='$Database',
        user='$User',
        password='$Password',
        connect_timeout=10
    )
    
    cur = conn.cursor()
    cur.execute('SELECT version()')
    version = cur.fetchone()[0]
    print(f'SUCCESS|{version}')
    
    cur.close()
    conn.close()
    sys.exit(0)
    
except psycopg2.OperationalError as e:
    error_msg = str(e).strip()
    print(f'ERROR|{error_msg}')
    sys.exit(1)
except ImportError:
    print('ERROR|psycopg2 not installed')
    sys.exit(1)
except Exception as e:
    print(f'ERROR|{e}')
    sys.exit(1)
"@

$testScript | Out-File -FilePath ".\temp_connection_test.py" -Encoding UTF8

try {
    $result = python .\temp_connection_test.py 2>&1 | Out-String
    
    if ($result -like "*SUCCESS|*") {
        $version = ($result -split '\|')[1].Trim()
        Write-Host "  ✓ Connection successful!" -ForegroundColor Green
        Write-Host "    PostgreSQL Version: $version" -ForegroundColor Gray
    } elseif ($result -like "*psycopg2 not installed*") {
        Write-Host "  ✗ psycopg2 not installed" -ForegroundColor Red
        Write-Host ""
        Write-Host "Install it with: pip install psycopg2-binary" -ForegroundColor Yellow
        Remove-Item -Path ".\temp_connection_test.py" -ErrorAction SilentlyContinue
        exit 1
    } else {
        $errorMsg = ($result -split '\|', 2)[1].Trim()
        Write-Host "  ✗ Connection failed" -ForegroundColor Red
        Write-Host "    Error: $errorMsg" -ForegroundColor Gray
        Write-Host ""
        
        if ($errorMsg -like "*password authentication failed*") {
            Write-Host "Issue: Wrong password or user doesn't exist" -ForegroundColor Yellow
            Write-Host "Fix: On remote PostgreSQL, run:" -ForegroundColor White
            Write-Host "  psql -U postgres" -ForegroundColor Gray
            Write-Host "  ALTER USER debezium_user WITH PASSWORD 'debezium_password';" -ForegroundColor Gray
        } elseif ($errorMsg -like "*no pg_hba.conf entry*") {
            Write-Host "Issue: pg_hba.conf doesn't allow connection from this IP" -ForegroundColor Yellow
            Write-Host "Fix: On remote laptop, edit pg_hba.conf and add:" -ForegroundColor White
            Write-Host "  host    all             debezium_user   192.168.0.0/16  scram-sha-256" -ForegroundColor Gray
            Write-Host "  (Make sure it says 192.168, not 192.169!)" -ForegroundColor Gray
            Write-Host "Then restart PostgreSQL service" -ForegroundColor Gray
        } elseif ($errorMsg -like "*could not connect*" -or $errorMsg -like "*Connection refused*") {
            Write-Host "Issue: PostgreSQL not listening on external interface" -ForegroundColor Yellow
            Write-Host "Fix: Edit postgresql.conf on remote laptop:" -ForegroundColor White
            Write-Host "  listen_addresses = '*'" -ForegroundColor Gray
            Write-Host "Then restart PostgreSQL service" -ForegroundColor Gray
        }
        
        Remove-Item -Path ".\temp_connection_test.py" -ErrorAction SilentlyContinue
        exit 1
    }
} catch {
    Write-Host "  ✗ Test failed: $_" -ForegroundColor Red
    Remove-Item -Path ".\temp_connection_test.py" -ErrorAction SilentlyContinue
    exit 1
}

Remove-Item -Path ".\temp_connection_test.py" -ErrorAction SilentlyContinue
Write-Host ""

# ============================================================================
# Test 3: Check WAL Configuration
# ============================================================================
Write-Host "[3/4] Checking WAL configuration (for CDC)..." -ForegroundColor Yellow

$walScript = @"
import sys
import psycopg2

try:
    # Try with debezium_user first
    conn = psycopg2.connect(
        host='$RemoteHost',
        port=$Port,
        database='$Database',
        user='$User',
        password='$Password',
        connect_timeout=5
    )
    
    cur = conn.cursor()
    cur.execute('''
        SELECT name, setting 
        FROM pg_settings 
        WHERE name IN ('wal_level', 'max_wal_senders', 'max_replication_slots')
    ''')
    
    settings = dict(cur.fetchall())
    print(f"wal_level={settings.get('wal_level', 'unknown')}")
    print(f"max_wal_senders={settings.get('max_wal_senders', '0')}")
    print(f"max_replication_slots={settings.get('max_replication_slots', '0')}")
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"ERROR:{e}")
"@

$walScript | Out-File -FilePath ".\temp_wal_check.py" -Encoding UTF8

try {
    $walResult = python .\temp_wal_check.py 2>&1 | Out-String
    
    if ($walResult -like "*ERROR:*") {
        Write-Host "  ⚠ Cannot check WAL settings" -ForegroundColor Yellow
        Write-Host "    (May need postgres superuser for this check)" -ForegroundColor Gray
    } else {
        $walLevel = if ($walResult -match "wal_level=(\w+)") { $matches[1] } else { "unknown" }
        $walSenders = if ($walResult -match "max_wal_senders=(\d+)") { [int]$matches[1] } else { 0 }
        $walSlots = if ($walResult -match "max_replication_slots=(\d+)") { [int]$matches[1] } else { 0 }
        
        Write-Host "  Current settings:" -ForegroundColor Gray
        Write-Host "    wal_level: $walLevel" -ForegroundColor $(if ($walLevel -eq "logical") {"Green"} else {"Yellow"})
        Write-Host "    max_wal_senders: $walSenders" -ForegroundColor $(if ($walSenders -ge 4) {"Green"} else {"Yellow"})
        Write-Host "    max_replication_slots: $walSlots" -ForegroundColor $(if ($walSlots -ge 4) {"Green"} else {"Yellow"})
        Write-Host ""
        
        if ($walLevel -eq "logical" -and $walSenders -ge 4 -and $walSlots -ge 4) {
            Write-Host "  ✓ WAL configuration is correct for CDC" -ForegroundColor Green
        } else {
            Write-Host "  ⚠ WAL configuration needs updating for CDC" -ForegroundColor Yellow
            Write-Host ""
            Write-Host "On remote PostgreSQL, edit postgresql.conf and add:" -ForegroundColor White
            Write-Host "  wal_level = logical" -ForegroundColor Gray
            Write-Host "  max_wal_senders = 10" -ForegroundColor Gray
            Write-Host "  max_replication_slots = 10" -ForegroundColor Gray
            Write-Host "Then restart PostgreSQL service" -ForegroundColor Gray
        }
    }
} catch {
    Write-Host "  ⚠ Could not check WAL settings" -ForegroundColor Yellow
}

Remove-Item -Path ".\temp_wal_check.py" -ErrorAction SilentlyContinue
Write-Host ""

# ============================================================================
# Test 4: Check Test Table
# ============================================================================
Write-Host "[4/4] Checking for test table..." -ForegroundColor Yellow

$tableScript = @"
import sys
import psycopg2

try:
    conn = psycopg2.connect(
        host='$RemoteHost',
        port=$Port,
        database='$Database',
        user='$User',
        password='$Password',
        connect_timeout=5
    )
    
    cur = conn.cursor()
    cur.execute('''
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'cdc_test'
        )
    ''')
    
    exists = cur.fetchone()[0]
    if exists:
        cur.execute('SELECT COUNT(*) FROM public.cdc_test')
        count = cur.fetchone()[0]
        print(f'EXISTS|{count}')
    else:
        print('NOT_EXISTS')
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f'ERROR:{e}')
"@

$tableScript | Out-File -FilePath ".\temp_table_check.py" -Encoding UTF8

try {
    $tableResult = python .\temp_table_check.py 2>&1 | Out-String
    
    if ($tableResult -like "*EXISTS|*") {
        $count = ($tableResult -split '\|')[1].Trim()
        Write-Host "  ✓ Table 'cdc_test' exists with $count rows" -ForegroundColor Green
    } elseif ($tableResult -like "*NOT_EXISTS*") {
        Write-Host "  ⚠ Table 'cdc_test' doesn't exist yet" -ForegroundColor Yellow
        Write-Host "    Run setup_postgres_wal.sql to create it" -ForegroundColor Gray
    } else {
        Write-Host "  ⚠ Could not check table" -ForegroundColor Yellow
    }
} catch {
    Write-Host "  ⚠ Could not check table" -ForegroundColor Yellow
}

Remove-Item -Path ".\temp_table_check.py" -ErrorAction SilentlyContinue

# ============================================================================
# Summary
# ============================================================================
Write-Host ""
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host "  Summary" -ForegroundColor Cyan
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "✓ Connection test passed!" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor White
Write-Host "  1. Fix pg_hba.conf on remote laptop (if needed):" -ForegroundColor Gray
Write-Host "     - Change 192.169.0.0/16 to 192.168.0.0/16" -ForegroundColor Gray
Write-Host "     - Add: host    replication     debezium_user   192.168.0.0/16  scram-sha-256" -ForegroundColor Gray
Write-Host "     - Restart PostgreSQL service" -ForegroundColor Gray
Write-Host ""
Write-Host "  2. Create Debezium connector:" -ForegroundColor Gray
Write-Host "     cd 'Kafka Integration'" -ForegroundColor Gray
Write-Host "     python setup_cdc_connectors.py --source postgres" -ForegroundColor Gray
Write-Host ""
Write-Host "  3. Test CDC by inserting data:" -ForegroundColor Gray
Write-Host "     INSERT INTO public.cdc_test (message) VALUES ('Test from remote');" -ForegroundColor Gray
Write-Host ""
Write-Host "  4. Check Kafka UI for messages:" -ForegroundColor Gray
Write-Host "     http://localhost:8080" -ForegroundColor Gray
Write-Host ""
