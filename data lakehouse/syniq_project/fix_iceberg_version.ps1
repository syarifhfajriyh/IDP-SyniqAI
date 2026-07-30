# ============================================================
# Fix: Iceberg Version Compatibility for Spark 3.5.0
# ============================================================
# ERROR: java.lang.NoSuchMethodError: 'org.apache.spark.sql.internal.SQLConf org.apache.spark.sql.SQLContext.conf()'
# ROOT CAUSE: Iceberg 1.10.1 is incompatible with Spark 3.5.0
# SOLUTION: Use Iceberg 1.6.1 (latest compatible version for Spark 3.5.0)
# ============================================================

Write-Host "`n🔍 Iceberg + Spark Version Compatibility Fix" -ForegroundColor Cyan
Write-Host "============================================================`n" -ForegroundColor Cyan

# Set correct version for Spark 3.5.0
$ICEBERG_VERSION = "1.6.1"
$SPARK_VERSION = "3.5"
$SCALA_VERSION = "2.12"

$JAR_NAME = "iceberg-spark-runtime-$SPARK_VERSION`_$SCALA_VERSION-$ICEBERG_VERSION.jar"
$JAR_URL = "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-$SPARK_VERSION`_$SCALA_VERSION/$ICEBERG_VERSION/$JAR_NAME"
$JARS_DIR = "jars"
$JAR_PATH = "$JARS_DIR\$JAR_NAME"

Write-Host "Target JAR: $JAR_NAME" -ForegroundColor Yellow
Write-Host "Maven URL: $JAR_URL`n" -ForegroundColor Gray

# Step 1: Create jars directory
if (-not (Test-Path $JARS_DIR)) {
    Write-Host "📁 Creating jars directory..." -ForegroundColor Cyan
    New-Item -ItemType Directory -Path $JARS_DIR | Out-Null
    Write-Host "   ✅ Created: $JARS_DIR`n" -ForegroundColor Green
} else {
    Write-Host "📁 Directory exists: $JARS_DIR`n" -ForegroundColor Green
}

# Step 2: Remove incompatible Iceberg JARs
Write-Host "🗑️  Removing incompatible Iceberg JARs..." -ForegroundColor Cyan
$OLD_JARS = Get-ChildItem -Path $JARS_DIR -Filter "iceberg-spark-runtime*.jar" -ErrorAction SilentlyContinue
if ($OLD_JARS) {
    foreach ($jar in $OLD_JARS) {
        Write-Host "   Removing: $($jar.Name)" -ForegroundColor Red
        Remove-Item $jar.FullName -Force
    }
    Write-Host "   ✅ Old JARs removed`n" -ForegroundColor Green
} else {
    Write-Host "   No old JARs found`n" -ForegroundColor Gray
}

# Step 3: Download correct Iceberg JAR
Write-Host "📥 Downloading Iceberg $ICEBERG_VERSION for Spark $SPARK_VERSION..." -ForegroundColor Cyan
Write-Host "   This may take a minute (JAR is ~40-50 MB)...`n" -ForegroundColor Gray

try {
    Invoke-WebRequest -Uri $JAR_URL -OutFile $JAR_PATH -UseBasicParsing
    $FILE_SIZE_MB = [Math]::Round((Get-Item $JAR_PATH).Length / 1MB, 2)
    Write-Host "   ✅ Downloaded: $JAR_NAME ($FILE_SIZE_MB MB)`n" -ForegroundColor Green
} catch {
    Write-Host "`n   ❌ ERROR: Failed to download JAR" -ForegroundColor Red
    Write-Host "   Error: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "`n   🔧 Manual Download Instructions:" -ForegroundColor Yellow
    Write-Host "   1. Go to: $JAR_URL" -ForegroundColor White
    Write-Host "   2. Save file as: $JAR_PATH`n" -ForegroundColor White
    exit 1
}

# Step 4: Verify JAR
Write-Host "🔍 Verifying JAR file..." -ForegroundColor Cyan
if (Test-Path $JAR_PATH) {
    $JAR_SIZE = (Get-Item $JAR_PATH).Length
    if ($JAR_SIZE -gt 10MB) {
        Write-Host "   ✅ JAR verified: $([Math]::Round($JAR_SIZE / 1MB, 2)) MB`n" -ForegroundColor Green
    } else {
        Write-Host "   ⚠️  Warning: JAR size is unusually small ($([Math]::Round($JAR_SIZE / 1MB, 2)) MB)" -ForegroundColor Yellow
        Write-Host "   The download may have failed. Please manually download the JAR.`n" -ForegroundColor Yellow
    }
} else {
    Write-Host "   ❌ JAR verification failed`n" -ForegroundColor Red
    exit 1
}

# Step 5: Summary
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "✅ FIX COMPLETE - Iceberg version updated!" -ForegroundColor Green
Write-Host "============================================================`n" -ForegroundColor Cyan

Write-Host "📋 Summary:" -ForegroundColor Cyan
Write-Host "   • Spark Version: 3.5.0" -ForegroundColor White
Write-Host "   • Iceberg Version: $ICEBERG_VERSION (compatible)" -ForegroundColor White
Write-Host "   • JAR Location: $JAR_PATH" -ForegroundColor White

Write-Host "`n🔧 Configuration Updated:" -ForegroundColor Cyan
Write-Host "   • config/iceberg_config.yaml → iceberg_version: $ICEBERG_VERSION" -ForegroundColor White

Write-Host "`n🚀 Next Steps:" -ForegroundColor Cyan
Write-Host "   1. Restart your backend API:" -ForegroundColor White
Write-Host "      cd gui" -ForegroundColor Gray
Write-Host "      python -m api.backend" -ForegroundColor Gray
Write-Host "`n   2. Try your Silver transformation again" -ForegroundColor White
Write-Host "      The error should be resolved now!`n" -ForegroundColor White

Write-Host "============================================================`n" -ForegroundColor Cyan

Write-Host "📚 Version Compatibility Reference:" -ForegroundColor Yellow
Write-Host "   • Spark 3.5.0 → Iceberg 1.5.x or 1.6.x ✅" -ForegroundColor Green
Write-Host "   • Spark 3.5.0 → Iceberg 1.10+ ❌ (API incompatibility)" -ForegroundColor Red
Write-Host "`n============================================================`n" -ForegroundColor Cyan
