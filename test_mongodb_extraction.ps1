# ============================================================================
# MongoDB CDC - Manual Extraction Test
# ============================================================================
# Test MongoDB extraction without Airflow for debugging/development

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "╔═══════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║     MongoDB CDC - Manual Extraction Test                     ║" -ForegroundColor Cyan
Write-Host "╚═══════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

$PROJECT_ROOT = Split-Path -Parent $PSScriptRoot
$KAFKA_INTEGRATION = "$PROJECT_ROOT\Kafka Integration"

# ============================================================================
# Load configuration
# ============================================================================

Write-Host "[1/5] Loading configuration..." -ForegroundColor Yellow

$envFile = "$KAFKA_INTEGRATION\.env.mongodb"

if (!(Test-Path $envFile)) {
    Write-Host ""
    Write-Host "✗ Configuration file not found: $envFile" -ForegroundColor Red
    Write-Host "  Please create .env.mongodb with MongoDB connection settings." -ForegroundColor Gray
    Write-Host ""
    exit 1
}

# Parse .env file
$env:MONGODB_URI = (Get-Content $envFile | Select-String "^MONGODB_URI=").Line -replace "MONGODB_URI=", ""
$env:MONGODB_DATABASE = (Get-Content $envFile | Select-String "^MONGODB_DATABASE=").Line -replace "MONGODB_DATABASE=", ""
$collections = (Get-Content $envFile | Select-String "^MONGODB_COLLECTIONS=").Line -replace "MONGODB_COLLECTIONS=", ""
$env:MONGODB_BATCH_SIZE = (Get-Content $envFile | Select-String "^MONGODB_BATCH_SIZE=").Line -replace "MONGODB_BATCH_SIZE=", "1000"
$env:KAFKA_BOOTSTRAP_SERVERS = (Get-Content $envFile | Select-String "^KAFKA_BOOTSTRAP_SERVERS=").Line -replace "KAFKA_BOOTSTRAP_SERVERS=", "localhost:9092"

Write-Host "  ✓ MongoDB URI: $env:MONGODB_URI" -ForegroundColor Green
Write-Host "  ✓ Database: $env:MONGODB_DATABASE" -ForegroundColor Green
Write-Host "  ✓ Collections: $collections" -ForegroundColor Green
Write-Host ""

# ============================================================================
# Select collection to test
# ============================================================================

Write-Host "[2/5] Select collection to test..." -ForegroundColor Yellow

$collectionArray = $collections -split "," | ForEach-Object { $_.Trim() }

if ($collectionArray.Count -eq 0) {
    Write-Host ""
    Write-Host "✗ No collections configured in MONGODB_COLLECTIONS" -ForegroundColor Red
    Write-Host ""
    exit 1
}

Write-Host ""
for ($i = 0; $i -lt $collectionArray.Count; $i++) {
    Write-Host "  $($i+1). $($collectionArray[$i])" -ForegroundColor White
}
Write-Host "  $($collectionArray.Count+1). Test ALL collections" -ForegroundColor White
Write-Host ""

$choice = Read-Host "Select collection (1-$($collectionArray.Count+1))"

if ([int]$choice -eq ($collectionArray.Count+1)) {
    $selectedCollections = $collectionArray
    Write-Host "  ✓ Testing all collections" -ForegroundColor Green
} else {
    $selectedCollections = @($collectionArray[[int]$choice - 1])
    Write-Host "  ✓ Testing: $($selectedCollections[0])" -ForegroundColor Green
}
Write-Host ""

# ============================================================================
# Run mode selection
# ============================================================================

Write-Host "[3/5] Select run mode..." -ForegroundColor Yellow
Write-Host ""
Write-Host "  1. DRY RUN (no Kafka, just preview)" -ForegroundColor White
Write-Host "  2. SEND TO KAFKA (full extraction)" -ForegroundColor White
Write-Host ""

$modeChoice = Read-Host "Select mode (1-2)"

if ($modeChoice -eq "1") {
    $env:MONGODB_DRY_RUN = "true"
    Write-Host "  ✓ Dry run mode enabled" -ForegroundColor Green
    $dryRun = $true
} else {
    $env:MONGODB_DRY_RUN = "false"
    Write-Host "  ✓ Live mode - will send to Kafka" -ForegroundColor Green
    $dryRun = $false
}
Write-Host ""

# ============================================================================
# Pre-flight checks
# ============================================================================

Write-Host "[4/5] Running pre-flight checks..." -ForegroundColor Yellow

# Check MongoDB connection
Write-Host "  Testing MongoDB connection..." -NoNewline
try {
    $testOutput = python -c @"
from pymongo import MongoClient
import sys
try:
    client = MongoClient('$env:MONGODB_URI', serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    db = client['$env:MONGODB_DATABASE']
    print(f'OK|{len(db.list_collection_names())}')
except Exception as e:
    print(f'ERROR|{str(e)}')
    sys.exit(1)
"@
    
    if ($testOutput -match "OK\|(\d+)") {
        $collCount = $Matches[1]
        Write-Host " ✓" -ForegroundColor Green
        Write-Host "    Connected! Found $collCount collections in database" -ForegroundColor Gray
    } else {
        Write-Host " ✗" -ForegroundColor Red
        Write-Host "    $testOutput" -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Host " ✗" -ForegroundColor Red
    Write-Host "    Error: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# Check Kafka (if not dry run)
if (!$dryRun) {
    Write-Host "  Testing Kafka connection..." -NoNewline
    try {
        docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) {
            Write-Host " ✓" -ForegroundColor Green
        } else {
            Write-Host " ✗" -ForegroundColor Red
            Write-Host "    Kafka not accessible. Run: docker-compose up -d (in Kafka Integration folder)" -ForegroundColor Red
            exit 1
        }
    } catch {
        Write-Host " ✗" -ForegroundColor Red
        exit 1
    }
}

Write-Host ""

# ============================================================================
# Run extraction
# ============================================================================

Write-Host "[5/5] Running extraction..." -ForegroundColor Yellow
Write-Host ""

$extractorScript = "$KAFKA_INTEGRATION\mongodb_batch_extractor.py"

if (!(Test-Path $extractorScript)) {
    Write-Host "✗ Extractor script not found: $extractorScript" -ForegroundColor Red
    Write-Host "  Please copy from MongoDB CDC package" -ForegroundColor Gray
    Write-Host ""
    exit 1
}

# Create temporary Python test script
$testScript = @"
import sys
import os
sys.path.insert(0, r'$KAFKA_INTEGRATION')

# Set environment variables
os.environ['MONGODB_URI'] = '$env:MONGODB_URI'
os.environ['MONGODB_DATABASE'] = '$env:MONGODB_DATABASE'
os.environ['MONGODB_BATCH_SIZE'] = '$env:MONGODB_BATCH_SIZE'
os.environ['KAFKA_BOOTSTRAP_SERVERS'] = '$env:KAFKA_BOOTSTRAP_SERVERS'
os.environ['MONGODB_DRY_RUN'] = '$env:MONGODB_DRY_RUN'

from mongodb_batch_extractor import MongoDBBatchExtractor
import json
from pathlib import Path

# Load watermarks
watermark_file = Path(r'$KAFKA_INTEGRATION') / 'watermarks' / 'mongodb_watermarks.json'
watermarks = {}
if watermark_file.exists():
    with open(watermark_file, 'r') as f:
        watermarks = json.load(f)

# Test collections
collections = '$($selectedCollections -join ",")''.split(',')

print()
print('='*80)
print('  MONGODB CDC EXTRACTION TEST')
print('='*80)
print()

total_docs = 0
total_batches = 0

for collection in collections:
    print(f'Collection: {collection}')
    print('-'*80)
    
    try:
        extractor = MongoDBBatchExtractor(
            collection_name=collection,
            watermarks=watermarks
        )
        
        stats = extractor.extract_and_send()
        
        docs = stats.get('documents_processed', 0)
        batches = stats.get('batches_sent', 0)
        watermark = stats.get('new_watermark', 'None')
        
        total_docs += docs
        total_batches += batches
        
        print(f'  ✓ Documents processed: {docs:,}')
        print(f'  ✓ Batches sent: {batches}')
        print(f'  ✓ New watermark: {watermark}')
        print()
        
    except Exception as e:
        print(f'  ✗ Error: {str(e)}')
        print()
        sys.exit(1)

print('='*80)
print(f'  SUMMARY')
print('='*80)
print(f'  Total Documents: {total_docs:,}')
print(f'  Total Batches: {total_batches}')
print(f'  Collections: {len(collections)}')
print('='*80)
print()

if '$env:MONGODB_DRY_RUN' == 'true':
    print('ℹ DRY RUN - No data was actually sent to Kafka')
else:
    print('✓ Data successfully sent to Kafka!')
    print('  Check Bronze layer in MinIO for results')
print()
"@

# Write test script to temp file
$tempScript = "$env:TEMP\test_mongodb_extraction.py"
$testScript | Out-File -FilePath $tempScript -Encoding UTF8

# Run extraction
try {
    python $tempScript
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host ""
        Write-Host "╔═══════════════════════════════════════════════════════════════╗" -ForegroundColor Green
        Write-Host "║  ✓ Extraction Test Complete!                                 ║" -ForegroundColor Green
        Write-Host "╚═══════════════════════════════════════════════════════════════╝" -ForegroundColor Green
        Write-Host ""
        
        if (!$dryRun) {
            Write-Host "Next Steps:" -ForegroundColor Cyan
            Write-Host "  1. Check Kafka topics:" -ForegroundColor White
            Write-Host "     docker exec kafka kafka-topics --list --bootstrap-server localhost:9092" -ForegroundColor Gray
            Write-Host ""
            Write-Host "  2. View messages:" -ForegroundColor White
            Write-Host "     docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic airflow.mongodb.$($selectedCollections[0]) --from-beginning --max-messages 5" -ForegroundColor Gray
            Write-Host ""
            Write-Host "  3. Check Bronze layer in MinIO: http://localhost:9001" -ForegroundColor White
            Write-Host ""
        } else {
            Write-Host "Next Steps:" -ForegroundColor Cyan
            Write-Host "  • Review dry run output above" -ForegroundColor White
            Write-Host "  • If everything looks good, run again in live mode" -ForegroundColor White
            Write-Host "  • Or enable DAG in Airflow for scheduled extraction" -ForegroundColor White
            Write-Host ""
        }
    } else {
        Write-Host ""
        Write-Host "✗ Extraction failed! Check error messages above." -ForegroundColor Red
        Write-Host ""
    }
} catch {
    Write-Host ""
    Write-Host "✗ Error running extraction: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host ""
} finally {
    # Cleanup temp script
    if (Test-Path $tempScript) {
        Remove-Item $tempScript -Force
    }
}
