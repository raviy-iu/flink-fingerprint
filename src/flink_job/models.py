from dataclasses import dataclass
from typing import Dict


@dataclass
class SensorEvent:
    equip_id: int
    timestamp: int
    data: Dict[str, str]


@dataclass
class Fingerprint:
    uuid: str
    equip_id: int
    type: str
    start_ms: int
    end_ms: int
    data: Dict[str, Dict[str, float]]
