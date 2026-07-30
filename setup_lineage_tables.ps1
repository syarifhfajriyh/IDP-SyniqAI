#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Initialize PostgreSQL lineage and audit tables for reporting

.DESCRIPTION
    Creates data_lineage and audit_log tables in PostgreSQL using Python
    Required for Reports functionality with persistent lineage tracking
#>

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  PostgreSQL Lineage Tables Setup" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# PostgreSQL connection settings
$POSTGRES_HOST = if ($env:POSTGRES_HOST) { $env:POSTGRES_HOST } else { "localhost" }
$POSTGRES_PORT = if ($env:POSTGRES_PORT) { $env:POSTGRES_PORT } else { "5432" }
$POSTGRES_DB = if ($env:POSTGRES_DB) { $env:POSTGRES_DB } else { "syniqai_metadata" }
$POSTGRES_USER = if ($env:POSTGRES_USER) { $env:POSTGRES_USER } else { "syniqai_user" }
$POSTGRES_PASSWORD = if ($env:POSTGRES_PASSWORD) { $env:POSTGRES_PASSWORD } else { "syniqai_password" }

Write-Host "[*] Target Database: $POSTGRES_DB @ ${POSTGRES_HOST}:${POSTGRES_PORT}" -ForegroundColor Yellow
Write-Host ""

# Read SQL schema file
$schemaFile = Join-Path $PSScriptRoot "data lakehouse"
$schemaFile = Join-Path $schemaFile "syniq_project"
$schemaFile = Join-Path $schemaFile "metadata"
$schemaFile = Join-Path $schemaFile "rules_schema.sql"

if (-not (Test-Path $schemaFile)) {
    Write-Host "[ERROR] Schema file not found: $schemaFile" -ForegroundColor Red
    exit 1
}

Write-Host "[*] Reading schema from: rules_schema.sql" -ForegroundColor Yellow

# Create temporary Python script
$pythonScript = @"
import psycopg2
import sys
import re

host = '$POSTGRES_HOST'
port = '$POSTGRES_PORT'
database = '$POSTGRES_DB'
user = '$POSTGRES_USER'
password = '$POSTGRES_PASSWORD'

try:
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    conn.autocommit = False
    cursor = conn.cursor()
    
    # Read SQL file
    with open(r'$schemaFile', 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    # Split into individual statements (rough split by semicolon)
    statements = [s.strip() for s in sql_content.split(';') if s.strip()]
    
    critical_tables = ['data_lineage', 'audit_log']
    tables_created = []
    errors_ignored = 0
    
    for stmt in statements:
        if not stmt or stmt.startswith('--'):
            continue
            
        try:
            cursor.execute(stmt)
            conn.commit()
            
            # Track table creation
            if 'CREATE TABLE' in stmt.upper():
                for table in critical_tables:
                    if table in stmt:
                        tables_created.append(table)
                        
        except psycopg2.errors.DuplicateTable as e:
            # Table already exists - this is OK
            conn.rollback()
            errors_ignored += 1
            continue
            
        except psycopg2.errors.DuplicateObject as e:
            # Index/constraint already exists - this is OK
            conn.rollback()
            errors_ignored += 1
            continue
            
        except Exception as e:
            # Other errors might be critical
            error_msg = str(e).lower()
            if 'already exists' in error_msg or 'duplicate' in error_msg:
                conn.rollback()
                errors_ignored += 1
                continue
            else:
                # Critical error - check if tables were created
                conn.rollback()
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name IN ('data_lineage', 'audit_log')
                """)
                existing = cursor.fetchall()
                if len(existing) == 2:
                    # Both tables exist, error is non-critical
                    errors_ignored += 1
                    continue
                else:
                    raise
    
    # Verify critical tables exist
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name IN ('data_lineage', 'audit_log')
    """)
    tables = cursor.fetchall()
    
    if len(tables) < 2:
        print(f'ERROR:Required tables not found. Only found: {[t[0] for t in tables]}', file=sys.stderr)
        sys.exit(1)
    
    print('TABLES:')
    for table in tables:
        print(f'  - {table[0]}')
    
    if errors_ignored > 0:
        print(f'INFO:Ignored {errors_ignored} non-critical errors (objects already exist)')
    
    cursor.close()
    conn.close()
    sys.exit(0)
    
except psycopg2.OperationalError as e:
    print(f'CONNECTION_ERROR:{e}', file=sys.stderr)
    sys.exit(2)
except Exception as e:
    print(f'ERROR:{e}', file=sys.stderr)
    sys.exit(1)
"@

$tempPyFile = Join-Path $PSScriptRoot "temp_setup_lineage.py"
$pythonScript | Out-File -FilePath $tempPyFile -Encoding UTF8

try {
    Write-Host "[*] Checking PostgreSQL connection and creating tables..." -ForegroundColor Yellow
    Write-Host ""
    
    $result = python $tempPyFile 2>&1
    $exitCode = $LASTEXITCODE
    
    if ($exitCode -eq 0) {
        Write-Host "[OK] Tables created successfully!" -ForegroundColor Green
        Write-Host ""
        Write-Host "[*] Tables verified:" -ForegroundColor Green
        Write-Host $result -ForegroundColor Gray
        Write-Host ""
        
        Write-Host "============================================" -ForegroundColor Green
        Write-Host "  [OK] Setup Complete!" -ForegroundColor Green
        Write-Host "============================================" -ForegroundColor Green
        Write-Host ""
        Write-Host "Now restart the backend to enable reporting:" -ForegroundColor Yellow
        Write-Host "  Ctrl+C in the backend terminal, then:" -ForegroundColor Gray
        Write-Host "  cd gui\api" -ForegroundColor Gray
        Write-Host "  uvicorn backend:app --reload" -ForegroundColor Gray
        
    } elseif ($exitCode -eq 2) {
        Write-Host "[ERROR] PostgreSQL connection failed!" -ForegroundColor Red
        Write-Host "   Error: $result" -ForegroundColor Red
        Write-Host ""
        Write-Host "   Make sure PostgreSQL is running:" -ForegroundColor Yellow
        Write-Host "   - Docker: docker-compose up -d postgres" -ForegroundColor Gray
        Write-Host "   - Windows: Start 'postgresql' service" -ForegroundColor Gray
        exit 1
        
    } else {
        Write-Host "[ERROR] Error creating tables!" -ForegroundColor Red
        Write-Host "   Error: $result" -ForegroundColor Red
        exit 1
    }
    
} catch {
    Write-Host "[ERROR] Error executing setup: $_" -ForegroundColor Red
    exit 1
} finally {
    # Clean up temp file
    if (Test-Path $tempPyFile) {
        Remove-Item $tempPyFile -Force
    }
}
