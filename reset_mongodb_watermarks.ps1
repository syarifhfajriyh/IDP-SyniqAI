# ============================================================================
# MongoDB CDC - Watermark Reset Tool
# ============================================================================
# Use this script to reset watermarks for testing or troubleshooting
# CAUTION: Resetting watermarks may cause duplicate data extraction

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "╔═══════════════════════════════════════════════════════════════╗" -ForegroundColor Yellow
Write-Host "║     MongoDB CDC - Watermark Reset Tool                       ║" -ForegroundColor Yellow
Write-Host "╚═══════════════════════════════════════════════════════════════╝" -ForegroundColor Yellow
Write-Host ""

$PROJECT_ROOT = Split-Path -Parent $PSScriptRoot
$KAFKA_INTEGRATION = "$PROJECT_ROOT\Kafka Integration"
$WATERMARK_FILE = "$KAFKA_INTEGRATION\watermarks\mongodb_watermarks.json"
$BACKUP_DIR = "$KAFKA_INTEGRATION\watermarks\backups"

# ============================================================================
# Show current watermarks
# ============================================================================

Write-Host "Current Watermark Status:" -ForegroundColor Cyan
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Cyan
Write-Host ""

if (Test-Path $WATERMARK_FILE) {
    try {
        $watermarks = Get-Content $WATERMARK_FILE | ConvertFrom-Json
        
        if ($watermarks.PSObject.Properties.Count -eq 0) {
            Write-Host "  ℹ Watermark file is empty (no collections tracked yet)" -ForegroundColor Cyan
        } else {
            $watermarks.PSObject.Properties | ForEach-Object {
                $collection = $_.Name
                $data = $_.Value
                
                Write-Host "  📊 Collection: " -NoNewline -ForegroundColor White
                Write-Host $collection -ForegroundColor Yellow
                Write-Host "     └─ Last Watermark: $($data.last_watermark)" -ForegroundColor Gray
                Write-Host "     └─ Documents Processed: $($data.documents_processed)" -ForegroundColor Gray
                Write-Host "     └─ Last Updated: $($data.last_updated)" -ForegroundColor Gray
                Write-Host ""
            }
        }
    } catch {
        Write-Host "  ✗ Error reading watermarks: $($_.Exception.Message)" -ForegroundColor Red
        Write-Host ""
    }
} else {
    Write-Host "  ℹ No watermark file found at: $WATERMARK_FILE" -ForegroundColor Cyan
    Write-Host "  This is normal on first run." -ForegroundColor Gray
    Write-Host ""
}

# ============================================================================
# Reset options
# ============================================================================

Write-Host "╔═══════════════════════════════════════════════════════════════╗" -ForegroundColor Yellow
Write-Host "║  Reset Options                                                ║" -ForegroundColor Yellow
Write-Host "╚═══════════════════════════════════════════════════════════════╝" -ForegroundColor Yellow
Write-Host ""
Write-Host "1. Reset ALL watermarks (full re-extraction)" -ForegroundColor White
Write-Host "2. Reset SPECIFIC collection" -ForegroundColor White
Write-Host "3. Restore from backup" -ForegroundColor White
Write-Host "4. View watermark backups" -ForegroundColor White
Write-Host "5. Cancel (no changes)" -ForegroundColor White
Write-Host ""

$choice = Read-Host "Select option (1-5)"

switch ($choice) {
    "1" {
        # Reset all watermarks
        Write-Host ""
        Write-Host "⚠ WARNING: This will delete ALL watermarks!" -ForegroundColor Yellow
        Write-Host "  Next extraction will re-process all documents from the beginning." -ForegroundColor Yellow
        Write-Host "  This may cause duplicate data in your Bronze layer." -ForegroundColor Yellow
        Write-Host ""
        
        $confirm = Read-Host "Are you sure? Type 'RESET' to confirm"
        
        if ($confirm -eq "RESET") {
            # Create backup first
            if (Test-Path $WATERMARK_FILE) {
                if (!(Test-Path $BACKUP_DIR)) {
                    New-Item -ItemType Directory -Path $BACKUP_DIR | Out-Null
                }
                
                $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
                $backupFile = "$BACKUP_DIR\mongodb_watermarks_backup_$timestamp.json"
                Copy-Item $WATERMARK_FILE -Destination $backupFile
                Write-Host "  ✓ Backup created: $backupFile" -ForegroundColor Green
            }
            
            # Delete watermark file
            if (Test-Path $WATERMARK_FILE) {
                Remove-Item $WATERMARK_FILE -Force
                Write-Host "  ✓ All watermarks reset!" -ForegroundColor Green
            } else {
                # Create empty file
                "{}" | Out-File -FilePath $WATERMARK_FILE -Encoding UTF8
                Write-Host "  ✓ Created new empty watermark file" -ForegroundColor Green
            }
            
            Write-Host ""
            Write-Host "✓ Reset complete! Next Airflow DAG run will start from beginning." -ForegroundColor Green
        } else {
            Write-Host ""
            Write-Host "✗ Reset cancelled" -ForegroundColor Red
        }
    }
    
    "2" {
        # Reset specific collection
        if (!(Test-Path $WATERMARK_FILE)) {
            Write-Host ""
            Write-Host "✗ No watermark file found. Nothing to reset." -ForegroundColor Red
            Write-Host ""
            exit
        }
        
        $watermarks = Get-Content $WATERMARK_FILE | ConvertFrom-Json
        $collections = $watermarks.PSObject.Properties.Name
        
        if ($collections.Count -eq 0) {
            Write-Host ""
            Write-Host "✗ No collections in watermark file." -ForegroundColor Red
            Write-Host ""
            exit
        }
        
        Write-Host ""
        Write-Host "Available collections:" -ForegroundColor Cyan
        for ($i = 0; $i -lt $collections.Count; $i++) {
            Write-Host "  $($i+1). $($collections[$i])" -ForegroundColor White
        }
        Write-Host ""
        
        $collectionChoice = Read-Host "Select collection number (1-$($collections.Count))"
        $selectedCollection = $collections[[int]$collectionChoice - 1]
        
        if ($selectedCollection) {
            Write-Host ""
            Write-Host "⚠ Resetting watermark for: $selectedCollection" -ForegroundColor Yellow
            Write-Host ""
            
            $confirm = Read-Host "Confirm? (y/N)"
            
            if ($confirm -eq "y" -or $confirm -eq "Y") {
                # Create backup
                if (!(Test-Path $BACKUP_DIR)) {
                    New-Item -ItemType Directory -Path $BACKUP_DIR | Out-Null
                }
                
                $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
                $backupFile = "$BACKUP_DIR\mongodb_watermarks_backup_$timestamp.json"
                Copy-Item $WATERMARK_FILE -Destination $backupFile
                Write-Host "  ✓ Backup created: $backupFile" -ForegroundColor Green
                
                # Remove selected collection from watermarks
                $watermarks.PSObject.Properties.Remove($selectedCollection)
                
                # Save updated watermarks
                $watermarks | ConvertTo-Json | Out-File -FilePath $WATERMARK_FILE -Encoding UTF8
                
                Write-Host "  ✓ Watermark reset for: $selectedCollection" -ForegroundColor Green
                Write-Host ""
                Write-Host "✓ Next extraction will re-process this collection from beginning." -ForegroundColor Green
            } else {
                Write-Host ""
                Write-Host "✗ Reset cancelled" -ForegroundColor Red
            }
        }
    }
    
    "3" {
        # Restore from backup
        if (!(Test-Path $BACKUP_DIR)) {
            Write-Host ""
            Write-Host "✗ No backup directory found." -ForegroundColor Red
            Write-Host ""
            exit
        }
        
        $backups = Get-ChildItem "$BACKUP_DIR\*.json" | Sort-Object LastWriteTime -Descending
        
        if ($backups.Count -eq 0) {
            Write-Host ""
            Write-Host "✗ No backups found in: $BACKUP_DIR" -ForegroundColor Red
            Write-Host ""
            exit
        }
        
        Write-Host ""
        Write-Host "Available backups:" -ForegroundColor Cyan
        for ($i = 0; $i -lt $backups.Count; $i++) {
            $backup = $backups[$i]
            Write-Host "  $($i+1). $($backup.Name) - $(Get-Date $backup.LastWriteTime -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor White
        }
        Write-Host ""
        
        $backupChoice = Read-Host "Select backup number (1-$($backups.Count))"
        $selectedBackup = $backups[[int]$backupChoice - 1]
        
        if ($selectedBackup) {
            Write-Host ""
            Write-Host "Restoring from: $($selectedBackup.Name)" -ForegroundColor Yellow
            Write-Host ""
            
            $confirm = Read-Host "Confirm restore? (y/N)"
            
            if ($confirm -eq "y" -or $confirm -eq "Y") {
                Copy-Item $selectedBackup.FullName -Destination $WATERMARK_FILE -Force
                Write-Host "  ✓ Watermarks restored!" -ForegroundColor Green
                Write-Host ""
                
                # Show restored watermarks
                $watermarks = Get-Content $WATERMARK_FILE | ConvertFrom-Json
                Write-Host "Restored watermarks:" -ForegroundColor Cyan
                $watermarks.PSObject.Properties | ForEach-Object {
                    Write-Host "  • $($_.Name): $($_.Value.last_watermark)" -ForegroundColor White
                }
                Write-Host ""
            } else {
                Write-Host ""
                Write-Host "✗ Restore cancelled" -ForegroundColor Red
            }
        }
    }
    
    "4" {
        # View backups
        if (!(Test-Path $BACKUP_DIR)) {
            Write-Host ""
            Write-Host "✗ No backup directory found." -ForegroundColor Red
            Write-Host ""
            exit
        }
        
        $backups = Get-ChildItem "$BACKUP_DIR\*.json" | Sort-Object LastWriteTime -Descending
        
        if ($backups.Count -eq 0) {
            Write-Host ""
            Write-Host "ℹ No backups found in: $BACKUP_DIR" -ForegroundColor Cyan
            Write-Host ""
        } else {
            Write-Host ""
            Write-Host "Backup History:" -ForegroundColor Cyan
            Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Cyan
            
            foreach ($backup in $backups) {
                $timestamp = Get-Date $backup.LastWriteTime -Format 'yyyy-MM-dd HH:mm:ss'
                $size = [math]::Round($backup.Length / 1KB, 2)
                Write-Host ""
                Write-Host "  📁 $($backup.Name)" -ForegroundColor White
                Write-Host "     ├─ Created: $timestamp" -ForegroundColor Gray
                Write-Host "     ├─ Size: $size KB" -ForegroundColor Gray
                Write-Host "     └─ Path: $($backup.FullName)" -ForegroundColor DarkGray
            }
            Write-Host ""
        }
    }
    
    "5" {
        Write-Host ""
        Write-Host "✓ No changes made" -ForegroundColor Green
        Write-Host ""
        exit
    }
    
    default {
        Write-Host ""
        Write-Host "✗ Invalid choice" -ForegroundColor Red
        Write-Host ""
        exit
    }
}

Write-Host ""
Write-Host "═══════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""
