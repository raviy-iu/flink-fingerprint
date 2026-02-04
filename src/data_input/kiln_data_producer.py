#!/usr/bin/env python3
"""
Kiln Data Producer - Fetches plant data via API and sends to Kafka

This module:
1. Fetches plant data via API using api.py
2. Processes and saves alerts_dataframe to input/ folder
3. Reads row-by-row and sends to Kafka topic 'kiln-process-data' at 1 second intervals

Workflow:
kiln_data_producer.py (fetches via API)
            ↓
    input/alerts_dataframe_*.csv
            ↓
    kiln-process-data (Kafka topic)
            ↓
    Flink processing -> fingerprint-output -> LLM analysis
"""

import os
import sys
import json
import glob
import time
import logging
import pandas as pd
from json import dumps
from kafka import KafkaProducer

# Add parent directories to path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
utils_dir = os.path.join(src_dir, "utils")

if current_dir not in sys.path:
    sys.path.insert(0, current_dir)
if utils_dir not in sys.path:
    sys.path.insert(0, utils_dir)

# Import from available modules (avoid process_odr which has external dependencies)
from api import fetch_parameter_data
from helpers import ensure_datetime
from constants import (
    MAPPING_SHEET,
    GENERIC_COL,
    INDUSTRY_CONFIGS,
    DATABASES,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s - %(message)s"
)
logger = logging.getLogger("kiln-data-producer")


class KilnDataProducer:
    """Fetches plant data via API and produces to Kafka topic."""

    KAFKA_TOPIC = "kiln-process-data"
    INPUT_FOLDER = "input"
    ALERTS_FOLDER = "alerts_generated"

    def __init__(
        self,
        equipment_name: str = "SCL_LINE_2_KILN",
        industry: str = "cement",
        kafka_bootstrap_servers: str = "localhost:9092",
        send_interval_seconds: float = 1.0,
    ):
        self.equipment_name = equipment_name
        self.industry = industry
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.send_interval_seconds = send_interval_seconds
        self.producer = None
        self.alerts_dataframe = None
        self.alerts_dataframe_path = None

        # Load industry config
        self.industry_config = INDUSTRY_CONFIGS.get(industry)
        if not self.industry_config:
            raise ValueError(f"Industry '{industry}' not found in INDUSTRY_CONFIGS")

        # Load equipment database
        db_path = os.path.join(current_dir, f"files/{DATABASES[industry]}")
        with open(db_path, "r", encoding="utf-8") as f:
            self.equipment_db = json.load(f)

        if equipment_name not in self.equipment_db:
            raise ValueError(f"Equipment '{equipment_name}' not found in {industry} database")

        self.equipment_data = self.equipment_db[equipment_name]
        self.application = self.equipment_data["application"]

        logger.info(f"Initialized for {equipment_name} ({industry} - {self.application})")

    def _load_mapping(self) -> pd.DataFrame:
        """Load metadata mapping file for the equipment."""
        mapping_file = self.industry_config["metadata_mapping"].get(self.application)
        if not mapping_file:
            raise ValueError(f"No mapping file for application '{self.application}'")

        mapping_path = os.path.join(
            current_dir,
            f"files/{self.industry_config['base_path']}/{mapping_file}"
        )
        return pd.read_excel(mapping_path, sheet_name=MAPPING_SHEET)

    def _build_tag_mapping(self, map_df: pd.DataFrame) -> dict:
        """Build mapping from generic params to machine tags."""
        generic_to_machine = {}
        for _, row in map_df.iterrows():
            g = row.get(GENERIC_COL)
            mtag = row.get(self.equipment_name)

            if pd.isna(g) or pd.isna(mtag):
                continue

            g = str(g).strip()
            mtag = str(mtag).strip()
            if g and mtag:
                generic_to_machine[g] = mtag

        return generic_to_machine

    def fetch_data(self, start_time: int, end_time: int) -> pd.DataFrame:
        """
        Fetch plant data from API for the given time period.

        Args:
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds

        Returns:
            DataFrame with timestamp as index and tag values as columns
        """
        logger.info(f"Fetching data for {self.equipment_name}")
        logger.info(f"Time range: {pd.to_datetime(start_time, unit='ms')} to {pd.to_datetime(end_time, unit='ms')}")

        # Load mapping
        map_df = self._load_mapping()
        generic_to_machine = self._build_tag_mapping(map_df)

        # Get all parameters from mapping (those that have machine tags)
        required_generic = set(generic_to_machine.keys())

        # Build filtered mapping for API fetch
        filtered_mapping_for_fetch = {
            param_name: {
                "equipment": self.equipment_name,
                "tag": machine_tag
            }
            for param_name, machine_tag in generic_to_machine.items()
        }

        logger.info(f"Fetching {len(filtered_mapping_for_fetch)} parameters from API")

        # Setup API configuration
        api_url = self.industry_config["url"]
        api_key = os.getenv(
            self.industry_config["api_key_env"],
            self.industry_config["default_api_key"]
        )
        api_headers = {"X-API-key": api_key, "Content-Type": "application/json"}

        # Fetch data using api.py
        master_df = fetch_parameter_data(
            self.equipment_name,
            filtered_mapping_for_fetch,
            required_generic,
            start_time,
            end_time,
            api_url,
            api_headers,
        )

        if master_df.empty:
            logger.warning("No data retrieved from API")
            return pd.DataFrame()

        # Prepare DataFrame
        df_raw = master_df.copy()
        df_raw = ensure_datetime(df_raw, "timestamp")

        # Pivot to wide format (timestamp as index, params as columns)
        df = df_raw.pivot_table(
            index="timestamp", columns="param", values="value"
        ).reset_index()
        df = ensure_datetime(df, "timestamp")
        df = df.set_index("timestamp")

        # Resample to 1-minute intervals (like process_odr does)
        df = df.resample("1min").mean()

        logger.info(f"Fetched and processed {len(df)} data points with {len(df.columns)} parameters")

        return df

    def save_alerts_dataframe(self, df: pd.DataFrame) -> str:
        """
        Save the alerts dataframe to input folder.

        Args:
            df: DataFrame to save

        Returns:
            Path to saved file
        """
        os.makedirs(self.INPUT_FOLDER, exist_ok=True)

        timestamp_now = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        filename = f"alerts_dataframe_{self.equipment_name}_{timestamp_now}.csv"
        filepath = os.path.join(self.INPUT_FOLDER, filename)

        # Add IST offset for display (like process_odr does)
        df_save = df.copy()
        df_save.index = df_save.index + pd.Timedelta(hours=5, minutes=30)
        df_save.to_csv(filepath)

        self.alerts_dataframe = df
        self.alerts_dataframe_path = filepath

        logger.info(f"Alerts dataframe saved to {filepath}")
        logger.info(f"Shape: {df.shape[0]} rows x {df.shape[1]} columns")

        return filepath

    def find_latest_alerts_dataframe(self) -> str:
        """
        Find the most recently created alerts_dataframe file for this equipment.

        Returns:
            Path to the most recent file, or None if not found
        """
        # Check both input/ and alerts_generated/ folders
        patterns = [
            os.path.join(self.INPUT_FOLDER, f"alerts_dataframe_{self.equipment_name}_*.csv"),
            os.path.join(self.ALERTS_FOLDER, f"alerts_dataframe_{self.equipment_name}_*.csv"),
        ]

        all_files = []
        for pattern in patterns:
            all_files.extend(glob.glob(pattern))

        if not all_files:
            return None

        # Sort by modification time, get most recent
        latest = max(all_files, key=os.path.getmtime)
        return latest

    def load_alerts_dataframe(self, filepath: str = None) -> pd.DataFrame:
        """
        Load alerts_dataframe from file.

        Args:
            filepath: Path to CSV file. If None, loads most recent file.

        Returns:
            DataFrame with timestamp index
        """
        if filepath is None:
            filepath = self.find_latest_alerts_dataframe()
            if filepath is None:
                raise FileNotFoundError(
                    f"No alerts_dataframe files found for {self.equipment_name} "
                    f"in {self.INPUT_FOLDER}/ or {self.ALERTS_FOLDER}/"
                )

        df = pd.read_csv(filepath, index_col=0, parse_dates=True)

        # Convert back from IST to UTC (process_odr adds IST offset when saving)
        df.index = df.index - pd.Timedelta(hours=5, minutes=30)

        # Remove RUNNING_FLAG column if present (not needed for Kafka messages)
        if "RUNNING_FLAG" in df.columns:
            df = df.drop(columns=["RUNNING_FLAG"])

        self.alerts_dataframe = df
        self.alerts_dataframe_path = filepath

        logger.info(f"Loaded alerts_dataframe from {filepath}")
        logger.info(f"Shape: {df.shape[0]} rows x {df.shape[1]} columns")
        logger.info(f"Columns: {list(df.columns)}")

        return df

    def _create_producer(self):
        """Create Kafka producer."""
        if self.producer is None:
            self.producer = KafkaProducer(
                bootstrap_servers=[self.kafka_bootstrap_servers],
                value_serializer=lambda x: dumps(x).encode("utf-8")
            )
            logger.info(f"Kafka producer connected to {self.kafka_bootstrap_servers}")

    def _row_to_message(self, timestamp: pd.Timestamp, row: pd.Series) -> dict:
        """
        Convert a DataFrame row to Kafka message format.

        Args:
            timestamp: Row timestamp
            row: DataFrame row with tag values

        Returns:
            Message dict in required format:
            {
                "equip_id": "SCL_LINE_2_KILN",  # machine_name
                "timestamp": 1706123456789,
                "data": {
                    "TAG_NAME_1": "45.234",
                    "TAG_NAME_2": "67.891",
                    ...
                }
            }
        """
        # Convert timestamp to milliseconds
        ts_ms = int(timestamp.timestamp() * 1000)

        # Build data dict with actual tag names
        data = {}
        for tag_name, value in row.items():
            if pd.isna(value):
                data[tag_name] = "null"
            else:
                data[tag_name] = str(round(float(value), 3))

        return {
            "equip_id": self.equipment_name,  # Use machine_name
            "timestamp": ts_ms,
            "data": data
        }

    def send_to_kafka(self, df: pd.DataFrame = None):
        """
        Send alerts_dataframe row-by-row to Kafka at configured intervals.

        Args:
            df: Optional DataFrame to send. Uses self.alerts_dataframe if not provided.
        """
        if df is None:
            df = self.alerts_dataframe

        if df is None or df.empty:
            logger.error("No data to send. Fetch or load data first.")
            return

        self._create_producer()

        logger.info("=" * 60)
        logger.info(f"Starting Kafka producer to topic: {self.KAFKA_TOPIC}")
        logger.info(f"Equipment: {self.equipment_name}")
        logger.info(f"Total rows to send: {len(df)}")
        logger.info(f"Tags: {list(df.columns)}")
        logger.info(f"Send interval: {self.send_interval_seconds}s")
        logger.info("=" * 60)

        sent_count = 0

        try:
            for timestamp, row in df.iterrows():
                message = self._row_to_message(timestamp, row)

                self.producer.send(self.KAFKA_TOPIC, value=message)
                self.producer.flush()

                sent_count += 1
                ts_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")

                # Log every message
                non_null_count = sum(1 for v in message["data"].values() if v != "null")
                logger.info(
                    f"[{ts_str}] Sent #{sent_count}/{len(df)}: "
                    f"{non_null_count}/{len(message['data'])} values"
                )

                # Wait before sending next message
                time.sleep(self.send_interval_seconds)

        except KeyboardInterrupt:
            logger.info(f"\nStopped by user after sending {sent_count} messages")
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            raise
        finally:
            if self.producer:
                self.producer.close()
                self.producer = None

        logger.info(f"Completed! Sent {sent_count} messages to {self.KAFKA_TOPIC}")

    def run(self, start_time: int, end_time: int):
        """
        Full workflow: Fetch data via API, save alerts_dataframe, send to Kafka.

        Args:
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
        """
        # Fetch data from API
        df = self.fetch_data(start_time, end_time)

        if df.empty:
            logger.error("No data fetched. Aborting.")
            return

        # Save alerts dataframe
        self.save_alerts_dataframe(df)

        # Send to Kafka
        self.send_to_kafka(df)


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch plant data via API and send to Kafka topic 'kiln-process-data'"
    )
    parser.add_argument(
        "--equipment", "-e",
        default="SCL_LINE_2_KILN",
        help="Equipment name (default: SCL_LINE_2_KILN)"
    )
    parser.add_argument(
        "--industry",
        default="cement",
        help="Industry type (default: cement)"
    )
    parser.add_argument(
        "--start", "-s",
        default="2026-01-27 00:00:00",
        help="Start time (default: 2026-01-27 00:00:00)"
    )
    parser.add_argument(
        "--end", "-n",
        default="2026-01-28 00:00:00",
        help="End time (default: 2026-01-28 00:00:00)"
    )
    parser.add_argument(
        "--kafka", "-k",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)"
    )
    parser.add_argument(
        "--interval", "-i",
        type=float,
        default=1.0,
        help="Send interval in seconds (default: 1.0)"
    )
    parser.add_argument(
        "--load-file", "-l",
        help="Load alerts_dataframe from specific file instead of fetching from API"
    )
    parser.add_argument(
        "--load-latest",
        action="store_true",
        help="Load most recent alerts_dataframe file without fetching new data"
    )

    args = parser.parse_args()

    producer = KilnDataProducer(
        equipment_name=args.equipment,
        industry=args.industry,
        kafka_bootstrap_servers=args.kafka,
        send_interval_seconds=args.interval,
    )

    if args.load_file:
        # Load from specific file
        producer.load_alerts_dataframe(args.load_file)
        producer.send_to_kafka()
    elif args.load_latest:
        # Load most recent file
        producer.load_alerts_dataframe()
        producer.send_to_kafka()
    else:
        # Full workflow: fetch from API, save, then send to Kafka
        start_time = int(pd.to_datetime(args.start).timestamp() * 1000)
        end_time = int(pd.to_datetime(args.end).timestamp() * 1000)
        producer.run(start_time, end_time)


if __name__ == "__main__":
    main()
