@echo off
REM Start Backend API Server using syniq_env Python

cd /d "c:\Users\Syarifah\OneDrive - M Telecommunication Sdn Bhd\INTERNSHIP\SyniqAi\gui\api"

echo Starting SyniqAI Data Lakehouse API...
echo Backend will run on http://localhost:8000
echo.

"c:\Users\Syarifah\OneDrive - M Telecommunication Sdn Bhd\INTERNSHIP\SyniqAi\data lakehouse\syniq_env\Scripts\python.exe" start_backend.py

pause
