@echo off
REM ============================================================
REM Flink Fingerprint Pipeline - Shutdown Script
REM ============================================================

set PROJECT_DIR=%~dp0
cd /d %PROJECT_DIR%

echo ============================================================
echo  Stopping Flink Fingerprint Pipeline
echo ============================================================
echo.

REM ------------------------------------------------------------
REM Kill terminal windows
REM ------------------------------------------------------------
echo [1/3] Closing pipeline terminals...
taskkill /FI "WINDOWTITLE eq INPUT STREAM*" /F 2>nul
taskkill /FI "WINDOWTITLE eq OUTPUT STREAM*" /F 2>nul
taskkill /FI "WINDOWTITLE eq DATA GENERATOR*" /F 2>nul
taskkill /FI "WINDOWTITLE eq FLINK JOB*" /F 2>nul

echo.
echo [2/3] Stopping Docker containers...
docker-compose down

echo.
echo [3/3] Cleanup complete.
echo.
echo ============================================================
echo  Pipeline stopped successfully!
echo ============================================================
pause
