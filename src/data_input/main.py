#!/usr/bin/env python3
import os
import sys
import logging
import traceback
from json import loads
from kafka import KafkaConsumer
import time
from process_odr import publish_process_odr

# ----------------------------
# Logging
# ----------------------------
# Suppress print() in UAT/PROD ECS while keeping logger working
ENV_NAME = os.getenv("ENV_NAME", "local").lower()
if ENV_NAME in ["uat", "prod"]:
    # Redirect print() to devnull in UAT/PROD ECS,
    # but keep a reference to original stdout for logger
    _original_stdout = sys.stdout
    sys.stdout = open(os.devnull, 'w')
else:
    _original_stdout = sys.stdout

logger = logging.getLogger("process-odr-consumer")
# Logger always uses the original stdout (not devnull)
handler = logging.StreamHandler(_original_stdout)
handler.setFormatter(logging.Formatter(
    "[%(asctime)s] %(levelname)s %(name)s - %(message)s"
))
logger.addHandler(handler)
logger.setLevel(logging.INFO)
# Prevent propagation to root logger to avoid duplicate messages
logger.propagate = False

# Suppress verbose Kafka logs
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("kafka.conn").setLevel(logging.WARNING)
logging.getLogger("kafka.coordinator").setLevel(logging.WARNING)
logging.getLogger("kafka.coordinator.consumer").setLevel(logging.WARNING)
logging.getLogger("kafka.coordinator.heartbeat").setLevel(logging.WARNING)
logging.getLogger("kafka.cluster").setLevel(logging.WARNING)
logging.getLogger("kafka.consumer.subscription_state").setLevel(logging.WARNING)

# ----------------------------
# Environment / Config
# ----------------------------
# Topic names (primary topic for this service)
EQUIPMENT_PROCESS_ODR_REQUEST_TOPIC = os.getenv(
    "EQUIPMENT_PROCESS_ODR_REQUEST_TOPIC", "equipment_process_odr_request"
)
PROCESS_ODR_CONSUMER_GROUP_ID = os.getenv(
    "PROCESS_ODR_CONSUMER_GROUP_NAME", "process-odr-consumer"
)

# Kafka endpoints (comma-separated)
KAFKA_ENDPOINTS = os.getenv("KAFKA_ENDPOINTS", "localhost:9092")
BOOTSTRAP_SERVERS = [
    ep.strip() for ep in KAFKA_ENDPOINTS.split(",") if ep.strip()
]

# Consumer tuning
# process one at a time
CONSUMER_MAX_POLL_RECORDS = int(os.getenv("CONSUMER_MAX_POLL_RECORDS", 1))
# 15 min buffer
CONSUMER_MAX_POLL_INTERVAL_MS = int(
    os.getenv("CONSUMER_MAX_POLL_INTERVAL_MS", 900_000)
)
# 45 s (<= broker cap)
CONSUMER_SESSION_TIMEOUT_MS = int(
    os.getenv("CONSUMER_SESSION_TIMEOUT_MS", 45_000)
)
# 3 s (< session/3)
CONSUMER_HEARTBEAT_INTERVAL_MS = int(
    os.getenv("CONSUMER_HEARTBEAT_INTERVAL_MS", 3_000)
)
# 2 min
CONSUMER_REQUEST_TIMEOUT_MS = int(
    os.getenv("CONSUMER_REQUEST_TIMEOUT_MS", 120_000)
)
# 9 min (< broker)
CONSUMER_CONNECTIONS_MAX_IDLE_MS = int(
    os.getenv("CONSUMER_CONNECTIONS_MAX_IDLE_MS", 540_000)
)
CONSUMER_RETRY_BACKOFF_MS = int(
    os.getenv("CONSUMER_RETRY_BACKOFF_MS", 5_000)
)
CONSUMER_RECONNECT_BACKOFF_MS = int(
    os.getenv("CONSUMER_RECONNECT_BACKOFF_MS", 5_000)
)
CONSUMER_RECONNECT_BACKOFF_MAX_MS = int(
    os.getenv("CONSUMER_RECONNECT_BACKOFF_MAX_MS", 60_000)
)
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "latest")
ENABLE_AUTO_COMMIT = os.getenv("ENABLE_AUTO_COMMIT", "true").lower() == "true"

logger.info(
    "Environment Variables:: "
    f"EQUIPMENT_PROCESS_ODR_REQUEST_TOPIC: "
    f"{EQUIPMENT_PROCESS_ODR_REQUEST_TOPIC}, "
    f"PROCESS_ODR_CONSUMER_GROUP_NAME: {PROCESS_ODR_CONSUMER_GROUP_ID}, "
    f"KAFKA_ENDPOINTS: {BOOTSTRAP_SERVERS}"
)


def build_consumer(topic: str, group_id: str) -> KafkaConsumer:
    """Build and configure a Kafka consumer."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset=AUTO_OFFSET_RESET,
        enable_auto_commit=ENABLE_AUTO_COMMIT,
        max_poll_records=CONSUMER_MAX_POLL_RECORDS,
        max_poll_interval_ms=CONSUMER_MAX_POLL_INTERVAL_MS,
        session_timeout_ms=CONSUMER_SESSION_TIMEOUT_MS,
        request_timeout_ms=CONSUMER_REQUEST_TIMEOUT_MS,
        connections_max_idle_ms=CONSUMER_CONNECTIONS_MAX_IDLE_MS,
        retry_backoff_ms=CONSUMER_RETRY_BACKOFF_MS,
        reconnect_backoff_ms=CONSUMER_RECONNECT_BACKOFF_MS,
        reconnect_backoff_max_ms=CONSUMER_RECONNECT_BACKOFF_MAX_MS,
        value_deserializer=lambda b: (
            loads(b.decode("utf-8")) if b is not None else None
        ),
        key_deserializer=lambda b: (
            b.decode("utf-8") if b is not None else None
        ),
    )


def total_lag(consumer):
    """Calculate total lag across all assigned partitions."""
    assigned = consumer.assignment()
    if not assigned:
        return 0
    end_offsets = consumer.end_offsets(list(assigned))
    lag = 0
    for tp in assigned:
        try:
            pos = consumer.position(tp)
        except Exception:
            pos = None
        if pos is None:
            continue
        lag_tp = max(0, end_offsets.get(tp, 0) - pos)
        lag += lag_tp
    return lag


def main():
    """Main consumer loop."""
    LOG_EVERY_SEC = 5 * 60  # every 5 minutes
    _last_log = 0
    try:
        consumer = build_consumer(
            topic=EQUIPMENT_PROCESS_ODR_REQUEST_TOPIC,
            group_id=PROCESS_ODR_CONSUMER_GROUP_ID,
        )
        logger.info(
            f"Kafka Consumer listening on {BOOTSTRAP_SERVERS} for topic "
            f"'{EQUIPMENT_PROCESS_ODR_REQUEST_TOPIC}'"
        )

        # -------------------------------------------------
        # Minimal continuous listen (no shutdown handling)
        # -------------------------------------------------
        for msg in consumer:
            value = msg.value  # JSON-deserialized

            # Log received message
            logger.info(
                f"▶ Processing: {value.get('equipmentName')} | "
                f"{value.get('startTime')} → {value.get('endTime')} | "
                f"offset={msg.offset}"
            )

            # Extract parameters from message
            start = value.get("startTime")
            end = value.get("endTime")
            equipmentName = value.get("equipmentName")
            plantId = value.get("plantId")
            plantName = value.get("plantName")

            # Process ODR
            publish_process_odr(
                start, end, equipmentName, plantId, plantName
            )

            # Log lag periodically
            lag = total_lag(consumer)
            now = time.time()
            if now - _last_log > LOG_EVERY_SEC:
                _last_log = now
                logger.info("Replica lag on assigned partitions: %s", lag)

    except Exception as e:
        logger.error(f"Exception in Kafka Consumer: {e}")
        logger.error(f"TraceBack: {traceback.format_exc()}")


if __name__ == "__main__":
    main()
