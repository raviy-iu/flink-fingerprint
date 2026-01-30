#!/usr/bin/env python3
"""Submit PyFlink fingerprint job to Flink cluster."""

import sys
import time

# Add jobs directory to Python path
sys.path.insert(0, "/opt/flink/jobs")

print("=" * 60)
print("FLINK FINGERPRINT JOB SUBMITTER")
print("=" * 60)

# Wait for Kafka and Flink cluster to be ready
print("Waiting 20 seconds for services to initialize...")
time.sleep(20)

# Import and run the Flink job
print("Importing fingerprint job module...")
from src.flink_jobs.fingerprint_job import run_fingerprint_job

print("Submitting Flink fingerprint job...")
print("=" * 60)

run_fingerprint_job()
