import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from kafka import KafkaConsumer
from threading import Thread
from queue import Queue

# Add utils directory to Python path for direct imports
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
utils_dir = os.path.join(src_dir, "utils")

if utils_dir not in sys.path:
    sys.path.insert(0, utils_dir)

from config import KafkaConfig


class FingerprintSaver:
    """
    Consumes synthetic-sensor-data and fingerprint-output from Kafka,
    correlates them, and saves combined JSON files.
    """

    def __init__(self, output_dir: str = None):
        self.output_dir = output_dir or self._default_output_dir()
        os.makedirs(self.output_dir, exist_ok=True)

        # Store sensor data by (equip_id, timestamp_ms)
        self.sensor_data_buffer = defaultdict(list)

        # Queue for fingerprints to process
        self.fingerprint_queue = Queue()

    def _default_output_dir(self) -> str:
        """Get default output directory based on environment"""
        if os.environ.get("RUN_ENV") == "docker":
            return "/opt/flink/output"
        return os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            "output"
        )

    def _create_sensor_consumer(self) -> KafkaConsumer:
        """Create Kafka consumer for synthetic-sensor-data topic"""
        return KafkaConsumer(
            KafkaConfig.INPUT_TOPIC,
            bootstrap_servers=KafkaConfig.bootstrap_servers(),
            group_id="fingerprint-saver-sensor",
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

    def _create_fingerprint_consumer(self) -> KafkaConsumer:
        """Create Kafka consumer for fingerprint-output topic"""
        return KafkaConsumer(
            KafkaConfig.OUTPUT_TOPIC,
            bootstrap_servers=KafkaConfig.bootstrap_servers(),
            group_id="fingerprint-saver-output",
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

    def _consume_sensor_data(self):
        """Thread function to consume and buffer sensor data"""
        consumer = self._create_sensor_consumer()
        print(f"[SensorConsumer] Listening to '{KafkaConfig.INPUT_TOPIC}'...")

        for message in consumer:
            data = message.value
            equip_id = data.get("equip_id")
            timestamp = data.get("timestamp")

            if equip_id is not None and timestamp is not None:
                # Buffer sensor data by equip_id
                self.sensor_data_buffer[equip_id].append(data)

                # Clean old data (keep last 5 minutes worth)
                self._cleanup_old_sensor_data(equip_id, timestamp)

    def _cleanup_old_sensor_data(self, equip_id: str, current_timestamp: int):
        """Remove sensor data older than 5 minutes from current timestamp"""
        cutoff = current_timestamp - (5 * 60 * 1000)  # 5 minutes in ms
        self.sensor_data_buffer[equip_id] = [
            d for d in self.sensor_data_buffer[equip_id]
            if d.get("timestamp", 0) >= cutoff
        ]

    def _consume_fingerprints(self):
        """Thread function to consume fingerprints and save files"""
        consumer = self._create_fingerprint_consumer()
        print(f"[FingerprintConsumer] Listening to '{KafkaConfig.OUTPUT_TOPIC}'...")

        for message in consumer:
            fingerprint_data = message.value
            self._process_fingerprint(fingerprint_data)

    def _process_fingerprint(self, fingerprint_data: dict):
        """Process a fingerprint and save with corresponding sensor data"""
        fingerprint = fingerprint_data.get("fingerprint", fingerprint_data)

        fingerprint_id = fingerprint.get("uuid")
        equip_id = fingerprint.get("equip_id")
        start_ms = fingerprint.get("start_ms")
        end_ms = fingerprint.get("end_ms")

        if not all([fingerprint_id, equip_id, start_ms, end_ms]):
            print(f"[Warning] Invalid fingerprint data: {fingerprint_data}")
            return

        # equip_id is now a string (machine_name like "SCL_LINE_2_KILN")
        equip_id_str = str(equip_id)

        # Find corresponding sensor data within the window
        matching_sensor_data = self._find_sensor_data_in_window(
            equip_id_str, start_ms, end_ms
        )

        # Build combined output
        combined_output = {
            "fingerprint": fingerprint,
            "sensor_data": {
                "equip_id": equip_id_str,
                "window": {
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "start_time": datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat(),
                    "end_time": datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).isoformat(),
                },
                "readings": matching_sensor_data,
                "reading_count": len(matching_sensor_data),
            },
        }

        # Save to file
        self._save_to_file(fingerprint_id, start_ms, end_ms, combined_output)

    def _find_sensor_data_in_window(
        self, equip_id: str, start_ms: int, end_ms: int
    ) -> list:
        """Find all sensor data for equip_id within the time window"""
        matching_data = []

        for data in self.sensor_data_buffer.get(equip_id, []):
            timestamp = data.get("timestamp", 0)
            if start_ms <= timestamp < end_ms:
                matching_data.append(data)

        # Sort by timestamp
        matching_data.sort(key=lambda x: x.get("timestamp", 0))
        return matching_data

    def _save_to_file(
        self, fingerprint_id: str, start_ms: int, end_ms: int, data: dict
    ):
        """Save combined data to JSON file"""
        filename = f"{fingerprint_id}_{start_ms}_{end_ms}.json"
        filepath = os.path.join(self.output_dir, filename)

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

        print(f"[Saved] {filename}")

    def run(self):
        """Start consuming from both topics"""
        print(f"[FingerprintSaver] Output directory: {self.output_dir}")
        print(f"[FingerprintSaver] Bootstrap servers: {KafkaConfig.bootstrap_servers()}")

        # Start sensor data consumer thread
        sensor_thread = Thread(target=self._consume_sensor_data, daemon=True)
        sensor_thread.start()

        # Run fingerprint consumer in main thread
        self._consume_fingerprints()


def save_fingerprints(output_dir: str = None):
    """
    Main function to save fingerprints with their corresponding sensor data.

    Args:
        output_dir: Directory to save output files. Defaults to ./output or /opt/flink/output in Docker.
    """
    saver = FingerprintSaver(output_dir)
    saver.run()


if __name__ == "__main__":
    save_fingerprints()
