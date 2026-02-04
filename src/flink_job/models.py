from dataclasses import dataclass
from typing import Dict


@dataclass
class SensorEvent:
    equip_id: str  # Changed from int to str to support machine_name like "SCL_LINE_2_KILN"
    timestamp: int
    data: Dict[str, str]


@dataclass
class Fingerprint:
    uuid: str
    equip_id: str  # Changed from int to str to support machine_name
    type: str
    start_ms: int
    end_ms: int
    data: Dict[str, Dict[str, float]]
