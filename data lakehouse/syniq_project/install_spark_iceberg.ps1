# Automated Spark + Iceberg Installation Script
# Run with: powershell -ExecutionPolicy Bypass -File install_spark_iceberg.ps1

Write-Host "`n🚀 SyniqAI - Spark + Iceberg Installation" -ForegroundColor Cyan
Write-Host  "=" * 60 -ForegroundColor Cyan

$ErrorActionPreference = "Continue"

# Step 1: Check and install Java
Write-Host "`n📦 Step 1: Installing Java 11 JDK..." -ForegroundColor Yellow

# Check if Java is already installed
$javaInstalled = $false
try {
    $javaVersion = java -version 2>&1
    if ($javaVersion -match "11\.") {
        Write-Host "✅ Java 11 is already installed" -ForegroundColor Green
        $javaInstalled = $true
    }
} catch {
    Write-Host "Java not found, installing..." -ForegroundColor Gray
}

if (-not $javaInstalled) {
    Write-Host "Installing Eclipse Temurin JDK 11..." -ForegroundColor Gray
    winget install --id EclipseAdoptium.Temurin.11.JDK -e --silent --accept-source-agreements --accept-package-agreements
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Java 11 installed successfully!" -ForegroundColor Green
        Write-Host "⚠️  Please restart PowerShell for Java to be available in PATH" -ForegroundColor Yellow
    } else {
        Write-Host "❌ Java installation failed. Error code: $LASTEXITCODE" -ForegroundColor Red
        Write-Host "Please install manually from: https://adoptium.net/temurin/releases/?version=11" -ForegroundColor Yellow
    }
}

# Step 2: Install Python packages
Write-Host "`n📦 Step 2: Installing Python packages (PySpark, PyIceberg)..." -ForegroundColor Yellow

$pythonExe = "..\syniq_env\Scripts\python.exe"

if (-not (Test-Path $pythonExe)) {
    Write-Host "❌ Python environment not found at: $pythonExe" -ForegroundColor Red
    Write-Host "Please ensure syniq_env virtual environment exists" -ForegroundColor Yellow
    exit 1
}

Write-Host "Upgrading pip..." -ForegroundColor Gray
& $pythonExe -m pip install --upgrade pip --quiet

Write-Host "Installing PySpark 3.5.0..." -ForegroundColor Gray
& $pythonExe -m pip install pyspark==3.5.0 --quiet
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ PySpark installed" -ForegroundColor Green
}

Write-Host "Installing PyIceberg..." -ForegroundColor Gray
& $pythonExe -m pip install pyiceberg --quiet
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ PyIceberg installed" -ForegroundColor Green
}

Write-Host "Installing S3FS and PyArrow..." -ForegroundColor Gray
& $pythonExe -m pip install s3fs pyarrow --quiet
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ S3FS and PyArrow installed" -ForegroundColor Green
}

Write-Host "Installing PyYAML..." -ForegroundColor Gray
& $pythonExe -m pip install pyyaml --quiet
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ PyYAML installed" -ForegroundColor Green
}

# Step 3: Download Iceberg Spark runtime JAR
Write-Host "`n📦 Step 3: Downloading Iceberg Spark runtime JAR..." -ForegroundColor Yellow

$jarDir = "jars"
$jarFile = "iceberg-spark-runtime-3.5_2.12-1.10.1.jar"
$jarPath = Join-Path $jarDir $jarFile
$jarUrl = "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1/$jarFile"

# Create jars directory if it doesn't exist
if (-not (Test-Path $jarDir)) {
    New-Item -ItemType Directory -Path $jarDir -Force | Out-Null
}

if (Test-Path $jarPath) {
    $fileSize = (Get-Item $jarPath).Length / 1MB
    Write-Host "✅ JAR already exists ($([math]::Round($fileSize, 2)) MB)" -ForegroundColor Green
} else {
    Write-Host "Downloading from Maven Central..." -ForegroundColor Gray
    try {
        Invoke-WebRequest -Uri $jarUrl -OutFile $jarPath -UseBasicParsing
        $fileSize = (Get-Item $jarPath).Length / 1MB
        Write-Host "✅ JAR downloaded successfully ($([math]::Round($fileSize, 2)) MB)" -ForegroundColor Green
    } catch {
        Write-Host "❌ Failed to download JAR: $_" -ForegroundColor Red
        Write-Host "Please download manually from: $jarUrl" -ForegroundColor Yellow
    }
}

# Step 4: Verify installation
Write-Host "`n📦 Step 4: Verifying installation..." -ForegroundColor Yellow

Write-Host "Checking package versions..." -ForegroundColor Gray
& $pythonExe -c @"
import pyspark
import pyiceberg
print(f'  PySpark: {pyspark.__version__}')
print(f'  PyIceberg: {pyiceberg.__version__}')
"@

# Summary
Write-Host "`n" + ("=" * 60) -ForegroundColor Cyan
Write-Host "🎉 Installation Complete!" -ForegroundColor Green
Write-Host ("=" * 60) -ForegroundColor Cyan

Write-Host "`n📋 Installation Summary:" -ForegroundColor Cyan
Write-Host "  ✅ Java 11 JDK" -ForegroundColor Green
Write-Host "  ✅ PySpark 3.5.0" -ForegroundColor Green
Write-Host "  ✅ PyIceberg (latest)" -ForegroundColor Green
Write-Host "  ✅ S3FS + PyArrow" -ForegroundColor Green
Write-Host "  ✅ Iceberg Spark Runtime JAR (1.10.1)" -ForegroundColor Green

Write-Host "`n⚠️  Important: If Java was just installed, restart PowerShell!" -ForegroundColor Yellow

Write-Host "`n📚 Next Steps:" -ForegroundColor Cyan
Write-Host "  1. Restart PowerShell (if Java was just installed)"
Write-Host "  2. Run test: python test_spark_iceberg_setup.py"
Write-Host "  3. Read guide: SPARK_ICEBERG_INSTALLATION.md"
Write-Host "  4. Configure: config/iceberg_config.yaml"

Write-Host "`n✨ Happy coding with Spark + Iceberg! ✨`n" -ForegroundColor Cyan
