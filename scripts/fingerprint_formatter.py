import json
import math
from collections import defaultdict
from kafka import KafkaConsumer
from src.utils.config import KafkaConfig, EquipmentConfig


def median(values):
    values = sorted(values)
    n = len(values)
    return values[n // 2] if n % 2 else (values[n // 2 - 1] + values[n // 2]) / 2


def std_dev(values, mean):
    return math.sqrt(sum((x - mean) ** 2 for x in values) / len(values))


def run_formatter():
    consumer = KafkaConsumer(
        KafkaConfig.OUTPUT_TOPIC,
        bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS_HOST,
        value_deserializer=lambda v: json.loads(v.decode()),
        auto_offset_reset="latest",
        group_id=None
    )

    fingerprints = defaultdict(lambda: defaultdict(list))

    for msg in consumer:
        r = msg.value

        fingerprint_id = f"fingerprint_{r['equip_id']}_{r['window_start']}"
        sensor_id = f"tag_{r['sensor_id']}"

        fingerprints[fingerprint_id][sensor_id].append(r)

        output = {}

        for fid, sensors in fingerprints.items():
            output[fid] = {}

            for tag, rows in sensors.items():
                values = [x["avg_value"] for x in rows]
                mean_val = sum(values) / len(values)

                meta = EquipmentConfig.EQUIPMENT_META.get(rows[0]["equip_id"], {})

                output[fid][tag] = {
                    "equipment_id": meta.get("equipment_id"),
                    "eq_type": meta.get("eq_type"),
                    "start_ms": int(rows[0]["window_start"].timestamp() * 1000),
                    "end_ms": int(rows[0]["window_end"].timestamp() * 1000),
                    "min": min(x["min_value"] for x in rows),
                    "max": max(x["max_value"] for x in rows),
                    "median": median(values),
                    "mean": mean_val,
                    "std_dev": std_dev(values, mean_val),
                }

        print(json.dumps(output, indent=2))
