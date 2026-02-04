import json
import os
import sys
import uuid

# Add current directory to Python path for direct imports
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from models import SensorEvent


def parse_sensor_event(raw: str) -> SensorEvent:
    obj = json.loads(raw)
    return SensorEvent(
        equip_id=obj["equip_id"],
        timestamp=obj["timestamp"],
        data=obj["data"]
    )


def build_fingerprint_json(
    equip_id: str,  # Changed from int to str to support machine_name
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
