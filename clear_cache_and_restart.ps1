# Clear Python cache and restart backend
Write-Host "🧹 Clearing Python cache files..." -ForegroundColor Cyan

# Clear __pycache__ directories
Get-ChildItem -Path ".\data lakehouse\syniq_project\ingestion" -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force
Write-Host "✅ Cleared __pycache__ directories" -ForegroundColor Green

# Clear .pyc files
Get-ChildItem -Path ".\data lakehouse\syniq_project\ingestion" -Recurse -Filter "*.pyc" | Remove-Item -Force
Write-Host "✅ Removed .pyc files" -ForegroundColor Green

Write-Host "`n🔄 Please restart your backend server now!" -ForegroundColor Yellow
Write-Host "   Run: python gui/app.py" -ForegroundColor White
