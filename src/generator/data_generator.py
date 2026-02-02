import json
import os
import random
import sys
import time
from datetime import datetime, timezone

# Add utils directory to Python path for direct imports
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
utils_dir = os.path.join(src_dir, "utils")

if utils_dir not in sys.path:
    sys.path.insert(0, utils_dir)

from kafka import KafkaProducer
from config import EquipmentConfig, KafkaConfig


def generate_value():
    if random.random() < 0.15:
        return "null"
    return round(random.uniform(0, 100), 3)


def run_generator():
    print("=" * 60)
    print("Sensor Data Generator")
    print("=" * 60)
    print(f"Bootstrap servers: {KafkaConfig.bootstrap_servers()}")
    print(f"Topic: {KafkaConfig.INPUT_TOPIC}")
    print(f"Equipment IDs: {EquipmentConfig.EQUIPMENT_IDS}")
    print(f"Sensor IDs: {EquipmentConfig.SENSOR_IDS}")
    print("=" * 60)
    print("Connecting to Kafka...")

    producer = KafkaProducer(
        bootstrap_servers=KafkaConfig.bootstrap_servers(),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("Connected! Starting data generation...")
    print("-" * 60)

    batch_count = 0
    while True:
        batch_count += 1
        now = datetime.now(timezone.utc)
        timestamp = int(now.timestamp() * 1000)
        timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S")

        for equip_id in EquipmentConfig.EQUIPMENT_IDS:
            message = {
                "equip_id": equip_id,
                "timestamp": timestamp,
                "data": {
                    sid: generate_value()
                    for sid in EquipmentConfig.SENSOR_IDS
                },
            }

            producer.send(KafkaConfig.INPUT_TOPIC, message)

        producer.flush()

        print(f"[{timestamp_str}] Batch #{batch_count}: Sent {len(EquipmentConfig.EQUIPMENT_IDS)} messages "
              f"(equip_ids: {EquipmentConfig.EQUIPMENT_IDS[0]}-{EquipmentConfig.EQUIPMENT_IDS[-1]})")

        time.sleep(1)


if __name__ == "__main__":
    try:
        run_generator()
    except KeyboardInterrupt:
        print("\nGenerator stopped by user.")
    except Exception as e:
        print(f"\nError: {e}")
