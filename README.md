# Flink Fingerprint Streaming Pipeline

A real-time sensor data processing pipeline using Apache Flink and Kafka.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DOCKER ENVIRONMENT                                 │
│  ┌─────────────┐    ┌─────────────────────────────────────────────────────────┐ │
│  │  Zookeeper  │    │                      KAFKA                              │ │
│  │   :2182     │◄───┤  ┌─────────────────┐      ┌──────────────────────┐      │ │
│  └─────────────┘    │  │  sensor-data    │      │  fingerprint-output  │      │ │
│                     │  │  (input topic)  │      │   (output topic)     │      │ │
│                     │  └────────▲────────┘      └──────────▲───────────┘      │ │
│                     │           │                          │                  │ │
│                     └───────────┼──────────────────────────┼──────────────────┘ │
│                                 │                          │                    │
│                                 │ kafka:9093               │ kafka:9093         │
│                                 │                          │                    │
│  ┌──────────────────────────────┼──────────────────────────┼──────────────────┐ │
│  │                         FLINK CLUSTER                   │                  │ │
│  │  ┌─────────────┐    ┌───────┴───────┐    ┌─────────────┴─────────────┐     │ │
│  │  │ JobManager  │    │ Kafka Source  │    │       Kafka Sink          │     │ │
│  │  │   :8081     │    │               │    │                           │     │ │
│  │  └─────────────┘    └───────┬───────┘    └───────────────▲───────────┘     │ │
│  │                             │                            │                 │ │
│  │  ┌─────────────┐            ▼                            │                 │ │
│  │  │ TaskManager │    ┌───────────────────────────────────┐│                 │ │
│  │  │  (4 slots)  │◄───┤      FINGERPRINT PROCESSING       ├┘                 │ │
│  │  └─────────────┘    │  ┌─────────────────────────────┐  │                  │ │
│  │                     │  │ 1. Parse JSON sensor data   │  │                  │ │
│  │                     │  │ 2. Unnest sensor map        │  │                  │ │
│  │                     │  │ 3. Filter null values       │  │                  │ │
│  │                     │  │ 4. 1-min tumbling window    │  │                  │ │
│  │                     │  │ 5. Aggregate (min/max/avg)  │  │                  │ │
│  │                     │  └─────────────────────────────┘  │                  │ │
│  │                     └───────────────────────────────────┘                  │ │
│  └────────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│    Data      │     │    Kafka     │     │    Flink     │     │    Kafka     │
│  Generator   │────►│ sensor-data  │────►│  Processing  │────►│ fingerprint- │
│  (Python)    │     │   (topic)    │     │    (1 min    │     │   output     │
│              │     │              │     │   windows)   │     │   (topic)    │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
     │                                           │
     │ Generates every 1 sec                     │ Outputs every 1 min
     │ - 5 equipment IDs                         │ - fingerprint_id (UUID)
     │ - 5 sensors each                          │ - min/max/avg/median/std_dev
     │ - Random values 0-100                     │ - per equipment per window
     └───────────────────────────────────────────┘
```

## Input/Output Schema

### Input Message (sensor-data topic)
```json
{
  "equip_id": 110,
  "timestamp": 1706123456789,
  "data": {
    "0001": "45.234",
    "0002": "67.891",
    "0003": "null",
    "0004": "23.456",
    "0005": "89.012"
  }
}
```

### Output Message (fingerprint-output topic)
```json
{
  "fingerprint": {
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "equip_id": "110",
    "type": "kiln",
    "start_ms": 1706123400000,
    "end_ms": 1706123460000,
    "data": {
      "0001": {"min": 12.3, "max": 98.7, "median": 54.3, "mean": 55.1, "std_dev": 15.2},
      "0002": {"min": 10.1, "max": 95.5, "median": 50.2, "mean": 51.8, "std_dev": 18.3}
    }
  }
}
```

---

## Quick Start Guide

### Prerequisites
- Docker & Docker Compose
- Python 3.8+

---

## Step-by-Step Execution

### Step 1: Start Docker Infrastructure

**Terminal 1** - Start all containers:
```bash
docker-compose up -d
```

Verify all containers are running:
```bash
docker-compose ps
```

Expected output: All 4 services (zookeeper, kafka, jobmanager, taskmanager) should be "Up".

---

### Step 2: Create Kafka Topics

**Terminal 1** - Create required topics:
```bash
docker exec flink-fingerprint-main-kafka kafka-topics --bootstrap-server localhost:9093 --create --topic sensor-data --partitions 1 --replication-factor 1
```

```bash
docker exec flink-fingerprint-main-kafka kafka-topics --bootstrap-server localhost:9093 --create --topic fingerprint-output --partitions 1 --replication-factor 1
```

Verify topics created:
```bash
docker exec flink-fingerprint-main-kafka kafka-topics --bootstrap-server localhost:9093 --list
```

---

### Step 3: Start Data Generator

**Terminal 2** - Run the sensor data generator:
```bash
docker exec -it flink-fingerprint-main-jobmanager python /opt/flink/jobs/src/generator/data_generator.py
```

You should see output like:
```
============================================================
Sensor Data Generator
============================================================
Bootstrap servers: kafka:9093
Topic: sensor-data
...
[2024-01-15 10:30:01] Batch #1: Sent 5 messages (equip_ids: 110-114)
[2024-01-15 10:30:02] Batch #2: Sent 5 messages (equip_ids: 110-114)
```

---

### Step 4: Start Flink Job

**Terminal 3** - Run the Flink fingerprint processing job:

# Run in detached mode (returns immediately)

```bash
docker exec -it flink-fingerprint-main-jobmanager flink run -d -py /opt/flink/jobs/src/flink_job/job.py
```

# With parallelism
```bash
docker exec -it flink-fingerprint-main-jobmanager flink run -d -p 2 -py /opt/flink/jobs/src/flink_job/job.py
```

# In local python
```bash
docker exec -it flink-fingerprint-main-jobmanager python /opt/flink/jobs/src/flink_job/job.py
```

Wait for job submission confirmation.

---

### Step 5: Monitor Input Topic (Optional)

**Terminal 4** - View sensor data being produced:
```bash
docker exec flink-fingerprint-main-kafka kafka-console-consumer --bootstrap-server localhost:9093 --topic sensor-data --from-beginning
```

---

### Step 6: Monitor Output Topic

**Terminal 5** - View fingerprint output (wait ~1 minute for first window):
```bash
docker exec flink-fingerprint-main-kafka kafka-console-consumer --bootstrap-server localhost:9093 --topic fingerprint-output --from-beginning
```

---

### Step 7: Save Fingerprints to Files (Optional)

**Terminal 6** - Run the fingerprint saver to save combined input/output JSON files:
```bash
docker exec -it flink-fingerprint-main-jobmanager python /opt/flink/jobs/src/postprocess/save_fingerprints.py
```

Output files are saved to `./output/` folder with format: `{fingerprint_id}_{start_ms}_{end_ms}.json`

---

## Monitoring Commands

### Check Flink Job Status
```bash
docker exec flink-fingerprint-main-jobmanager flink list -a
```

### Check Running Jobs Only
```bash
docker exec flink-fingerprint-main-jobmanager flink list -r
```

### Flink Web UI
Open in browser: **http://localhost:18081**

### Check Kafka Topic Stats
```bash
docker exec flink-fingerprint-main-kafka kafka-topics --bootstrap-server localhost:9093 --describe --topic sensor-data
```

```bash
docker exec flink-fingerprint-main-kafka kafka-topics --bootstrap-server localhost:9093 --describe --topic fingerprint-output
```

### List Consumer Groups
```bash
docker exec flink-fingerprint-main-kafka kafka-consumer-groups --bootstrap-server localhost:9093 --list
```

---

## Shutdown

### Stop All Services
```bash
docker-compose down
```

### Stop and Remove Volumes (Clean Reset)
```bash
docker-compose down -v
```

### Cancel a Running Flink Job
```bash
docker exec flink-fingerprint-main-jobmanager flink cancel <job-id>
```

---

## Project Structure

```
flink-fingerprint/
├── docker-compose.yml          # Infrastructure setup
├── Dockerfile                  # Flink container image
├── README.md                   # This file
├── src/
│   ├── flink_job/
│   │   ├── job.py              # Main Flink streaming job
│   │   ├── kafka_config.py     # Kafka source/sink configuration
│   │   ├── serialization.py    # JSON parsing and building
│   │   ├── aggregations.py     # Statistics computation
│   │   └── models.py           # Data models
│   ├── generator/
│   │   └── data_generator.py   # Sensor data generator
│   ├── postprocess/
│   │   └── save_fingerprints.py # Save fingerprints with input data
│   └── utils/
│       └── config.py           # Configuration settings
└── output/                     # Saved fingerprint JSON files
```

---

## Configuration

Edit `src/utils/config.py` to customize:

| Setting | Default | Description |
|---------|---------|-------------|
| `WINDOW_SIZE_MINUTES` | 1 | Tumbling window duration |
| `WATERMARK_SECONDS` | 10 | Watermark out-of-orderness |
| `EQUIPMENT_IDS` | [110-114] | Equipment IDs to simulate |
| `SENSOR_IDS` | ["0001"-"0005"] | Sensor IDs per equipment |
| `PARALLELISM` | 2 | Flink job parallelism |

---

## Troubleshooting

### Kafka Connection Issues
```bash
docker logs flink-fingerprint-main-kafka
```

### Flink Job Errors
```bash
docker logs flink-fingerprint-main-jobmanager
```

### Check Container Status
```bash
docker-compose ps
```

### Restart All Services
```bash
docker-compose restart
```
