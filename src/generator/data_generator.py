import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

from src.utils.config import EquipmentConfig, KafkaConfig


def generate_value():
    if random.random() < 0.15:
        return "null"
    return round(random.uniform(0, 100), 3)


def run_generator():
    producer = KafkaProducer(
        bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS_HOST,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    while True:
        for equip_id in EquipmentConfig.EQUIPMENT_IDS:
            message = {
                "equip_id": equip_id,
                "timestamp": int(datetime.utcnow().timestamp() * 1000),
                "data": {
                    sid: generate_value()
                    for sid in EquipmentConfig.SENSOR_IDS
                },
            }

            producer.send(KafkaConfig.INPUT_TOPIC, message)

        producer.flush()
        time.sleep(1)
