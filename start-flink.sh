#!/bin/bash
set -e

echo "========================================"
echo "Starting Flink Fingerprint Pipeline"
echo "========================================"

FLINK_HOME=/opt/flink
JOB_FILE=/opt/flink/jobs/main.py

echo "Waiting for Flink services..."
sleep 10

echo "Submitting Flink job..."
$FLINK_HOME/bin/flink run -py $JOB_FILE -d

echo "Flink job submitted successfully."

tail -f /dev/null
