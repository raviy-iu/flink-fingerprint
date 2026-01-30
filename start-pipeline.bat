@echo off
REM ============================================================
REM Flink Fingerprint Pipeline - Startup Script
REM ============================================================

set PROJECT_DIR=%~dp0
cd /d %PROJECT_DIR%

echo ============================================================
echo  Flink Fingerprint Streaming Pipeline
echo ============================================================
echo.

REM ------------------------------------------------------------
REM Step 1: Start Docker Infrastructure
REM ------------------------------------------------------------
echo [1/5] Starting Docker infrastructure...
docker-compose up -d

echo.
echo [2/5] Waiting for services to be ready (30 seconds)...
timeout /t 30 /nobreak > nul

REM Check if containers are running
docker-compose ps

echo.
echo [3/5] Creating Kafka topics (if not exists)...
docker exec flink-fingerprint-main-kafka kafka-topics --bootstrap-server localhost:9093 --create --if-not-exists --topic sensor-data --partitions 1 --replication-factor 1 2>nul
docker exec flink-fingerprint-main-kafka kafka-topics --bootstrap-server localhost:9093 --create --if-not-exists --topic fingerprint-output --partitions 1 --replication-factor 1 2>nul

echo.
echo [4/5] Opening monitoring terminals...

REM ------------------------------------------------------------
REM Terminal: Kafka Input Consumer (sensor-data)
REM ------------------------------------------------------------
start "INPUT STREAM - sensor-data" cmd /k "title INPUT STREAM - sensor-data && color 0A && echo ============================================ && echo  KAFKA INPUT STREAM: sensor-data && echo ============================================ && echo Waiting for messages... && echo. && docker exec -it flink-fingerprint-main-kafka kafka-console-consumer --bootstrap-server localhost:9093 --topic sensor-data"

REM ------------------------------------------------------------
REM Terminal: Kafka Output Consumer (fingerprint-output)
REM ------------------------------------------------------------
start "OUTPUT STREAM - fingerprint-output" cmd /k "title OUTPUT STREAM - fingerprint-output && color 0B && echo ============================================ && echo  KAFKA OUTPUT STREAM: fingerprint-output && echo ============================================ && echo Waiting for messages (output appears after 1-minute window)... && echo. && docker exec -it flink-fingerprint-main-kafka kafka-console-consumer --bootstrap-server localhost:9093 --topic fingerprint-output"

timeout /t 5 /nobreak > nul

REM ------------------------------------------------------------
REM Terminal: Data Generator
REM ------------------------------------------------------------
start "DATA GENERATOR" cmd /k "title DATA GENERATOR && color 0E && cd /d %PROJECT_DIR% && echo ============================================ && echo  DATA GENERATOR && echo ============================================ && echo. && .venv\Scripts\activate && python main.py generator"

timeout /t 5 /nobreak > nul

REM ------------------------------------------------------------
REM Terminal: Flink Job
REM ------------------------------------------------------------
echo [5/5] Starting Flink job...
start "FLINK JOB" cmd /k "title FLINK JOB && color 0D && echo ============================================ && echo  FLINK FINGERPRINT JOB && echo ============================================ && echo. && docker exec -it flink-fingerprint-main-jobmanager bash -c \"cd /opt/flink/jobs && python main.py flink\""

echo.
echo ============================================================
echo  All components started successfully!
echo ============================================================
echo.
echo  Terminals opened:
echo    [GREEN]  INPUT STREAM  - Shows sensor-data messages
echo    [CYAN]   OUTPUT STREAM - Shows fingerprint results (after 1 min)
echo    [YELLOW] DATA GENERATOR - Producing sensor data
echo    [PURPLE] FLINK JOB     - Processing pipeline
echo.
echo  Flink Web UI: http://localhost:18081
echo.
echo  To stop everything, run: stop-pipeline.bat
echo ============================================================
pause
