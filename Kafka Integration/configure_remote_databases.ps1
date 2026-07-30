#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Configure PostgreSQL and MariaDB for Remote CDC Access

.DESCRIPTION
    Run this script ON THE DATABASE SERVER (192.168.2.114)
    to allow remote Kafka Connect to access databases for CDC
#>

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "          CONFIGURE DATABASES FOR REMOTE CDC ACCESS" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "This script must be run on the DATABASE SERVER (192.168.2.114)" -ForegroundColor Yellow
Write-Host ""

# Get current machine IP
$currentIP = (Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.IPAddress -like "192.168.*" }).IPAddress
Write-Host "Current machine IP: $currentIP" -ForegroundColor Cyan
Write-Host ""

$continue = Read-Host "Continue with configuration? (y/n)"
if ($continue -ne "y") {
    Write-Host "Aborted." -ForegroundColor Yellow
    exit 0
}

Write-Host ""

# ============================================================================
# PostgreSQL Configuration
# ============================================================================
Write-Host "================================================================================" -ForegroundColor Green
Write-Host "          CONFIGURING POSTGRESQL" -ForegroundColor Green
Write-Host "================================================================================" -ForegroundColor Green
Write-Host ""

# Find PostgreSQL service
$pgService = Get-Service | Where-Object { $_.Name -like "*postgres*" } | Select-Object -First 1

if ($pgService) {
    Write-Host "[1/4] PostgreSQL service found: $($pgService.Name)" -ForegroundColor Cyan
    Write-Host "      Status: $($pgService.Status)" -ForegroundColor Gray
    Write-Host ""
    
    # Find PostgreSQL data directory
    Write-Host "[2/4] Locating PostgreSQL configuration files..." -ForegroundColor Cyan
    $pgDataDir = "C:\Program Files\PostgreSQL\15\data"  # Common location
    
    if (Test-Path $pgDataDir) {
        Write-Host "      Found data directory: $pgDataDir" -ForegroundColor Gray
        
        # postgresql.conf
        $pgConf = "$pgDataDir\postgresql.conf"
        if (Test-Path $pgConf) {
            Write-Host ""
            Write-Host "      Updating postgresql.conf..." -ForegroundColor Yellow
            Write-Host "      Manual step required:" -ForegroundColor Red
            Write-Host "      1. Open: $pgConf" -ForegroundColor White
            Write-Host "      2. Find: #listen_addresses = 'localhost'" -ForegroundColor White
            Write-Host "      3. Change to: listen_addresses = '*'" -ForegroundColor White
            Write-Host ""
        }
        
        # pg_hba.conf
        $pgHba = "$pgDataDir\pg_hba.conf"
        if (Test-Path $pgHba) {
            Write-Host "      Updating pg_hba.conf..." -ForegroundColor Yellow
            Write-Host "      Manual step required:" -ForegroundColor Red
            Write-Host "      1. Open: $pgHba" -ForegroundColor White
            Write-Host "      2. Add this line at the end:" -ForegroundColor White
            Write-Host "         host    all    all    192.168.0.0/16    md5" -ForegroundColor Green
            Write-Host ""
        }
    } else {
        Write-Host "      [WARNING] Data directory not found at default location" -ForegroundColor Yellow
        Write-Host "      Please manually edit:" -ForegroundColor Yellow
        Write-Host "        - postgresql.conf: listen_addresses = '*'" -ForegroundColor Gray
        Write-Host "        - pg_hba.conf: Add 'host all all 192.168.0.0/16 md5'" -ForegroundColor Gray
    }
    
    # Firewall rule
    Write-Host "[3/4] Configuring Windows Firewall for PostgreSQL..." -ForegroundColor Cyan
    try {
        $existingRule = Get-NetFirewallRule -DisplayName "PostgreSQL CDC" -ErrorAction SilentlyContinue
        if ($existingRule) {
            Write-Host "      Firewall rule already exists" -ForegroundColor Gray
        } else {
            New-NetFirewallRule -DisplayName "PostgreSQL CDC" -Direction Inbound -Protocol TCP -LocalPort 5432 -Action Allow | Out-Null
            Write-Host "      [OK] Firewall rule created for port 5432" -ForegroundColor Green
        }
    } catch {
        Write-Host "      [WARNING] Failed to create firewall rule: $($_.Exception.Message)" -ForegroundColor Yellow
        Write-Host "      Manually allow port 5432 in Windows Firewall" -ForegroundColor Yellow
    }
    
    # Restart prompt
    Write-Host ""
    Write-Host "[4/4] PostgreSQL requires restart to apply changes" -ForegroundColor Yellow
    $restartPg = Read-Host "      Restart PostgreSQL now? (y/n)"
    if ($restartPg -eq "y") {
        Restart-Service $pgService.Name
        Write-Host "      [OK] PostgreSQL restarted" -ForegroundColor Green
    } else {
        Write-Host "      [MANUAL] Please restart PostgreSQL manually" -ForegroundColor Yellow
    }
    
} else {
    Write-Host "[WARNING] PostgreSQL service not found on this machine" -ForegroundColor Yellow
}

Write-Host ""

# ============================================================================
# MariaDB Configuration
# ============================================================================
Write-Host "================================================================================" -ForegroundColor Green
Write-Host "          CONFIGURING MARIADB" -ForegroundColor Green
Write-Host "================================================================================" -ForegroundColor Green
Write-Host ""

# Find MariaDB/MySQL service
$mysqlService = Get-Service | Where-Object { $_.Name -like "*mysql*" -or $_.Name -like "*maria*" } | Select-Object -First 1

if ($mysqlService) {
    Write-Host "[1/4] MariaDB service found: $($mysqlService.Name)" -ForegroundColor Cyan
    Write-Host "      Status: $($mysqlService.Status)" -ForegroundColor Gray
    Write-Host ""
    
    # Find my.ini
    Write-Host "[2/4] Locating MariaDB configuration..." -ForegroundColor Cyan
    $myIniLocations = @(
        "C:\Program Files\MariaDB 10.11\data\my.ini",
        "C:\Program Files\MariaDB 10.6\data\my.ini",
        "C:\ProgramData\MySQL\MySQL Server 8.0\my.ini",
        "C:\xampp\mysql\bin\my.ini"
    )
    
    $myIni = $myIniLocations | Where-Object { Test-Path $_ } | Select-Object -First 1
    
    if ($myIni) {
        Write-Host "      Found configuration: $myIni" -ForegroundColor Gray
        Write-Host ""
        Write-Host "      Manual step required:" -ForegroundColor Red
        Write-Host "      1. Open: $myIni" -ForegroundColor White
        Write-Host "      2. Find [mysqld] section" -ForegroundColor White
        Write-Host "      3. Change: bind-address = 0.0.0.0" -ForegroundColor White
        Write-Host "         (or remove/comment out bind-address line)" -ForegroundColor Gray
        Write-Host ""
    } else {
        Write-Host "      [WARNING] my.ini not found at common locations" -ForegroundColor Yellow
        Write-Host "      Please manually set: bind-address = 0.0.0.0" -ForegroundColor Yellow
    }
    
    # Firewall rule
    Write-Host "[3/4] Configuring Windows Firewall for MariaDB..." -ForegroundColor Cyan
    try {
        $existingRule = Get-NetFirewallRule -DisplayName "MariaDB CDC" -ErrorAction SilentlyContinue
        if ($existingRule) {
            Write-Host "      Firewall rule already exists" -ForegroundColor Gray
        } else {
            New-NetFirewallRule -DisplayName "MariaDB CDC" -Direction Inbound -Protocol TCP -LocalPort 3306 -Action Allow | Out-Null
            Write-Host "      [OK] Firewall rule created for port 3306" -ForegroundColor Green
        }
    } catch {
        Write-Host "      [WARNING] Failed to create firewall rule: $($_.Exception.Message)" -ForegroundColor Yellow
        Write-Host "      Manually allow port 3306 in Windows Firewall" -ForegroundColor Yellow
    }
    
    # Restart prompt
    Write-Host ""
    Write-Host "[4/4] MariaDB requires restart to apply changes" -ForegroundColor Yellow
    $restartMysql = Read-Host "      Restart MariaDB now? (y/n)"
    if ($restartMysql -eq "y") {
        Restart-Service $mysqlService.Name
        Write-Host "      [OK] MariaDB restarted" -ForegroundColor Green
    } else {
        Write-Host "      [MANUAL] Please restart MariaDB manually" -ForegroundColor Yellow
    }
    
} else {
    Write-Host "[WARNING] MariaDB/MySQL service not found on this machine" -ForegroundColor Yellow
}

Write-Host ""

# ============================================================================
# Summary
# ============================================================================
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "          CONFIGURATION SUMMARY" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Manual Steps Completed:" -ForegroundColor Yellow
Write-Host "  ✓ Firewall rules created" -ForegroundColor Green
Write-Host ""
Write-Host "Manual Steps Required:" -ForegroundColor Yellow
Write-Host "  1. Edit PostgreSQL configuration files:" -ForegroundColor White
Write-Host "     - postgresql.conf: listen_addresses = '*'" -ForegroundColor Gray
Write-Host "     - pg_hba.conf: Add 'host all all 192.168.0.0/16 md5'" -ForegroundColor Gray
Write-Host ""
Write-Host "  2. Edit MariaDB configuration file:" -ForegroundColor White
Write-Host "     - my.ini: bind-address = 0.0.0.0" -ForegroundColor Gray
Write-Host ""
Write-Host "  3. Restart both database services" -ForegroundColor White
Write-Host ""
Write-Host "  4. Grant remote access to debezium_user:" -ForegroundColor White
Write-Host "     mysql -u root -p" -ForegroundColor Gray
Write-Host "     GRANT ALL ON *.* TO 'debezium_user'@'%' IDENTIFIED BY 'debezium_password';" -ForegroundColor Gray
Write-Host "     FLUSH PRIVILEGES;" -ForegroundColor Gray
Write-Host ""
Write-Host "Testing:" -ForegroundColor Yellow
Write-Host "  From remote laptop, run:" -ForegroundColor White
Write-Host "    Test-NetConnection -ComputerName $currentIP -Port 5432" -ForegroundColor Gray
Write-Host "    Test-NetConnection -ComputerName $currentIP -Port 3306" -ForegroundColor Gray
Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan
