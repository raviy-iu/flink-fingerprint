import json
import uuid
from flink_job.models import SensorEvent


def parse_sensor_event(raw: str) -> SensorEvent:
    obj = json.loads(raw)
    return SensorEvent(
        equip_id=obj["equip_id"],
        timestamp=obj["timestamp"],
        data=obj["data"]
    )


def build_fingerprint_json(
    equip_id: int,
    start_ms: int,
    end_ms: int,
    stats: dict
) -> str:
    return json.dumps({
        "fingerprint": {
            "uuid": str(uuid.uuid4()),
            "equip_id": str(equip_id),
            "type": "kiln",
            "start_ms": start_ms,
            "end_ms": end_ms,
            "data": stats
        }
    })
