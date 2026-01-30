"""Sensor data generator for equipment monitoring with Kafka producer."""

import json
import random
import time
from datetime import datetime
from typing import Any, Dict, Generator, Union

from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.utils.config import EquipmentConfig, KafkaConfig

SensorValue = Union[float, str]


def generate_sensor_value(sensor_id: str) -> SensorValue:
    """Generate a sensor value based on sensor type.

    Args:
        sensor_id: The sensor identifier (0001-0007, 0150)

    Returns:
        Sensor value as float or "null" string
    """
    # 15% chance of null value for any sensor
    if random.random() < 0.15:
        return "null"

    # Generate values based on sensor type with realistic ranges
    sensor_ranges = {
        "0001": (0, 100),
        "0002": (100, 200),
        "0003": (0, 50),
        "0004": (0.0, 1.0),
        "0005": (0.0, 0.01),
        "0006": (0, 100),
        "0007": (20, 50),
        "0150": (0, 20),
    }

    min_val, max_val = sensor_ranges.get(sensor_id, (0, 100))
    return round(random.uniform(min_val, max_val), 3)


def generate_equipment_reading(equip_id: int) -> Dict[str, Any]:
    """Generate a complete sensor reading for one equipment.

    Args:
        equip_id: Equipment identifier

    Returns:
        Dictionary with equipment reading in specified format
    """
    timestamp_ms = int(datetime.now().timestamp() * 1000)

    sensor_data = {
        sensor_id: generate_sensor_value(sensor_id)
        for sensor_id in EquipmentConfig.SENSOR_IDS
    }

    return {"equip_id": equip_id, "timestamp": timestamp_ms, "data": sensor_data}


def stream_readings(interval_seconds: float = 1.0) -> Generator[str, None, None]:
    """Generate continuous stream of equipment readings.

    Args:
        interval_seconds: Time between readings for each equipment cycle

    Yields:
        JSON string of equipment reading
    """
    while True:
        for equip_id in EquipmentConfig.EQUIPMENT_IDS:
            reading = generate_equipment_reading(equip_id)
            yield json.dumps(reading)
        time.sleep(interval_seconds)


class SensorDataProducer:
    """Kafka producer for streaming sensor data."""

    def __init__(
        self,
        bootstrap_servers: str = KafkaConfig.BOOTSTRAP_SERVERS_HOST,
        topic: str = KafkaConfig.TOPIC,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None

    def _create_producer(self) -> KafkaProducer:
        """Create and return a Kafka producer."""
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8") if k else None,
        )

    def start(self, interval_seconds: float = 1.0):
        """Start producing sensor data to Kafka.

        Args:
            interval_seconds: Time between readings for each equipment cycle
        """
        print(f"Connecting to Kafka at {self.bootstrap_servers}...")
        self.producer = self._create_producer()
        print(f"Publishing to topic: {self.topic}")
        print("Press Ctrl+C to stop\n")

        try:
            message_count = 0
            while True:
                for equip_id in EquipmentConfig.EQUIPMENT_IDS:
                    reading = generate_equipment_reading(equip_id)

                    # Use equipment ID as partition key for ordering
                    future = self.producer.send(
                        self.topic, key=equip_id, value=reading
                    )

                    try:
                        # Wait for send to complete
                        future.get(timeout=10)
                        message_count += 1

                        if message_count % 10 == 0:
                            print(
                                f"Sent {message_count} messages. "
                                f"Latest: equip_id={equip_id}, "
                                f"timestamp={reading['timestamp']}"
                            )
                    except KafkaError as e:
                        print(f"Error sending message: {e}")

                time.sleep(interval_seconds)

        except KeyboardInterrupt:
            print(f"\nStopping producer. Total messages sent: {message_count}")
        finally:
            if self.producer:
                self.producer.flush()
                self.producer.close()

    def stop(self):
        """Stop the producer."""
        if self.producer:
            self.producer.flush()
            self.producer.close()


def run_generator():
    """Start the sensor data generator."""
    producer = SensorDataProducer()
    producer.start()
