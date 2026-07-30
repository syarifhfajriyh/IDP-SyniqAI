# ============================================================
# Restart Backend with New Iceberg JAR
# This script ensures the backend uses Iceberg 1.6.1
# ============================================================

Write-Host "`n🔄 Restarting Backend with Iceberg 1.6.1" -ForegroundColor Cyan
Write-Host "============================================================`n" -ForegroundColor Cyan

# Step 1: Stop all Python processes (backend + Spark)
Write-Host "🛑 Step 1: Stopping all Python processes..." -ForegroundColor Yellow
$pythonProcesses = Get-Process python -ErrorAction SilentlyContinue

if ($pythonProcesses) {
    Write-Host "   Found $($pythonProcesses.Count) Python process(es)" -ForegroundColor Gray
    foreach ($proc in $pythonProcesses) {
        Write-Host "   Stopping PID $($proc.Id) (Memory: $([Math]::Round($proc.WorkingSet64/1MB, 2)) MB)..." -ForegroundColor Gray
        Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 2
    Write-Host "   ✅ All Python processes stopped`n" -ForegroundColor Green
} else {
    Write-Host "   No Python processes found`n" -ForegroundColor Gray
}

# Step 2: Clean up old Iceberg JARs
Write-Host "🧹 Step 2: Cleaning up old Iceberg JARs..." -ForegroundColor Yellow
Set-Location "data lakehouse\syniq_project\jars"

$oldJars = Get-ChildItem -Filter "iceberg-spark-runtime-3.5_2.12-1.*.jar" | Where-Object { $_.Name -notlike "*1.6.1.jar" }
if ($oldJars) {
    foreach ($jar in $oldJars) {
        Write-Host "   Removing: $($jar.Name)" -ForegroundColor Gray
        Remove-Item $jar.FullName -Force -ErrorAction SilentlyContinue
    }
    Write-Host "   ✅ Old JARs removed`n" -ForegroundColor Green
} else {
    Write-Host "   No old JARs to remove`n" -ForegroundColor Gray
}

# Step 3: Verify correct JAR exists
Write-Host "✅ Step 3: Verifying Iceberg 1.6.1 JAR..." -ForegroundColor Yellow
$correctJar = Get-ChildItem -Filter "iceberg-spark-runtime-3.5_2.12-1.6.1.jar" -ErrorAction SilentlyContinue

if ($correctJar) {
    $jarSizeMB = [Math]::Round($correctJar.Length / 1MB, 2)
    Write-Host "   ✅ Found: $($correctJar.Name) ($jarSizeMB MB)`n" -ForegroundColor Green
} else {
    Write-Host "   ❌ ERROR: Iceberg 1.6.1 JAR not found!" -ForegroundColor Red
    Write-Host "   Please run the download command again`n" -ForegroundColor Red
    Set-Location ..\..\..
    exit 1
}

# Step 4: List all JARs
Write-Host "📦 Step 4: Current JAR inventory:" -ForegroundColor Yellow
Get-ChildItem -Filter "*.jar" | Select-Object Name, @{Name="Size(MB)";Expression={[Math]::Round($_.Length/1MB, 2)}} | Format-Table -AutoSize

# Step 5: Navigate back and start backend
Write-Host "`n🚀 Step 5: Starting backend..." -ForegroundColor Yellow
Set-Location ..\..\..
Set-Location gui

Write-Host "   Backend will start in a new window in 3 seconds..." -ForegroundColor Gray
Write-Host "   Watch for 'Spark' and 'Iceberg' initialization messages" -ForegroundColor Gray
Write-Host "`n   Press Ctrl+C in this window if you want to cancel`n" -ForegroundColor Gray

Start-Sleep -Seconds 3

# Start backend in current terminal (not background)
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "🎯 STARTING BACKEND - Look for Iceberg messages below" -ForegroundColor Green
Write-Host "============================================================`n" -ForegroundColor Cyan

python -m api.backend

# This line will only execute if backend stops
Write-Host "`n`n============================================================" -ForegroundColor Cyan
Write-Host "Backend stopped" -ForegroundColor Yellow
Write-Host "============================================================`n" -ForegroundColor Cyan
