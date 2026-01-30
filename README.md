# Flink Fingerprint Streaming Pipeline

A real-time sensor data processing pipeline using Apache Flink and Kafka.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DOCKER ENVIRONMENT                                  │
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
│  │  ┌─────────────┐    ┌───────┴───────┐    ┌─────────────┴─────────────┐    │ │
│  │  │ JobManager  │    │ Kafka Source  │    │       Kafka Sink          │    │ │
│  │  │   :8081     │    │               │    │                           │    │ │
│  │  └─────────────┘    └───────┬───────┘    └───────────────▲───────────┘    │ │
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
        ▲
        │ localhost:29093
        │
┌───────┴───────────────────────────────────────────────────────────────────────┐
│                            HOST MACHINE                                        │
│  ┌─────────────────────────────────────────────────────────────────────────┐  │
│  │                        DATA GENERATOR                                    │  │
│  │  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────────────┐  │  │
│  │  │ Equipment   │    │  Sensor     │    │     JSON Message            │  │  │
│  │  │ IDs: 110-114│───►│  IDs: 0001- │───►│  {                          │  │  │
│  │  │             │    │  0007, 0150 │    │    "equip_id": 110,         │  │  │
│  │  └─────────────┘    └─────────────┘    │    "timestamp": 1706...,    │  │  │
│  │                                        │    "data": {"0001": 45.2}   │  │  │
│  │                                        │  }                          │  │  │
│  │                                        └─────────────────────────────┘  │  │
│  └─────────────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────────────────┘
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
     │ - 8 sensors each                          │ - min/max/avg values
     │ - Random values 0-100                     │ - count per window
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
    "0004": "23.456"
  }
}
```

### Output Message (fingerprint-output topic)
```json
{
  "fingerprint_id": "550e8400-e29b-41d4-a716-446655440000",
  "equip_id": 110,
  "sensor_id": "0001",
  "window_start": "2024-01-24T10:00:00",
  "window_end": "2024-01-24T10:01:00",
  "min_value": 12.345,
  "max_value": 98.765,
  "avg_value": 54.321,
  "count_value": 60
}
```

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.8+
- Virtual environment with dependencies

### One-Click Start (Windows)

```bash
start-pipeline.bat
```

This opens 4 color-coded terminals automatically:
- **Green** - Input stream (sensor-data)
- **Cyan** - Output stream (fingerprint-output)
- **Yellow** - Data generator
- **Purple** - Flink job

To stop everything:
```bash
stop-pipeline.bat
```

---

### Manual Start (Step-by-Step)

### Step 1: Start Infrastructure
```bash
docker-compose up -d
```

Wait for services to be ready:
```bash
docker-compose ps
```

### Step 2: Run Data Generator (Terminal 1)
```bash
# Windows
.venv\Scripts\activate
python main.py generator

# Linux/Mac
source .venv/bin/activate
python main.py generator
```

### Step 3: Run Flink Job (Terminal 2)
```bash
docker exec -it flink-fingerprint-main-jobmanager bash -c "cd /opt/flink/jobs && python main.py flink"
```

### Step 4: View Streaming Data (Terminal 3)

**View input stream:**
```bash
docker exec -it flink-fingerprint-main-kafka kafka-console-consumer \
  --bootstrap-server localhost:9093 \
  --topic sensor-data \
  --from-beginning
```

**View output stream:**
```bash
docker exec -it flink-fingerprint-main-kafka kafka-console-consumer \
  --bootstrap-server localhost:9093 \
  --topic fingerprint-output \
  --from-beginning
```

## Monitoring

- **Flink Web UI**: http://localhost:18081
- **Kafka Broker**: localhost:29093

## Project Structure

```
flink-fingerprint-main/
├── main.py                 # Entry point (generator/flink modes)
├── docker-compose.yml      # Infrastructure setup
├── Dockerfile              # Flink container image
├── src/
│   ├── generator/
│   │   └── data_generator.py   # Sensor data generator
│   ├── flink_jobs/
│   │   └── fingerprint_job.py  # Flink processing logic
│   └── utils/
│       └── config.py           # Configuration settings
└── output/                 # (Reserved for future file output)
```

## Configuration

Edit `src/utils/config.py` to customize:

| Setting | Default | Description |
|---------|---------|-------------|
| `WINDOW_SIZE_MINUTES` | 1 | Tumbling window duration |
| `EQUIPMENT_IDS` | [110-114] | Equipment to simulate |
| `SENSOR_IDS` | 8 sensors | Sensor IDs per equipment |
| `PARALLELISM` | 2 | Flink job parallelism |

## Shutdown

```bash
docker-compose down
```
