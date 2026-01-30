"""Main Flink job for fingerprint generation using tumbling windows."""

import os
import uuid
from datetime import datetime

from src.utils.config import FlinkConfig, KafkaConfig, OutputConfig


def run_fingerprint_job():
    """Execute the main fingerprint processing job (requires pyflink)."""
    # Import pyflink only when this function is called
    from pyflink.common import Types, WatermarkStrategy
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import (
        DataTypes,
        EnvironmentSettings,
        StreamTableEnvironment,
    )
    from pyflink.table.udf import udf

    @udf(result_type=DataTypes.STRING())
    def generate_uuid():
        """Generate a unique fingerprint UUID."""
        return str(uuid.uuid4())

    def create_environments():
        """Create and configure Flink Stream and Table environments."""
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(FlinkConfig.PARALLELISM)

        env_settings = EnvironmentSettings.in_streaming_mode()
        t_env = StreamTableEnvironment.create(env, environment_settings=env_settings)

        t_env.get_config().set(
            "execution.checkpointing.interval",
            str(FlinkConfig.CHECKPOINT_INTERVAL_MS),
        )

        return env, t_env

    def register_kafka_source(t_env):
        """Register Kafka source table for sensor data."""
        source_ddl = f"""
            CREATE TABLE sensor_source (
                equip_id INT,
                `timestamp` BIGINT,
                `data` MAP<STRING, STRING>,
                event_time AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
                WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{KafkaConfig.TOPIC}',
                'properties.bootstrap.servers' = '{KafkaConfig.BOOTSTRAP_SERVERS_DOCKER}',
                'properties.group.id' = '{KafkaConfig.GROUP_ID}',
                'scan.startup.mode' = 'latest-offset',
                'format' = 'json',
                'json.ignore-parse-errors' = 'true'
            )
        """
        t_env.execute_sql(source_ddl)

    def register_sink_table(t_env, output_path: str):
        """Register filesystem sink table for fingerprint output."""
        sink_ddl = f"""
            CREATE TABLE fingerprint_sink (
                fingerprint_id STRING,
                equip_id INT,
                window_start TIMESTAMP(3),
                window_end TIMESTAMP(3),
                sensor_id STRING,
                min_value DOUBLE,
                max_value DOUBLE,
                mean_value DOUBLE,
                median_value DOUBLE,
                variance_value DOUBLE,
                sample_count BIGINT
            ) WITH (
                'connector' = 'filesystem',
                'path' = '{output_path}',
                'format' = 'csv',
                'csv.field-delimiter' = ','
            )
        """
        t_env.execute_sql(sink_ddl)

    # Main job logic
    print("Initializing Flink environment...")
    env, t_env = create_environments()

    t_env.create_temporary_function("gen_uuid", generate_uuid)

    print(f"Connecting to Kafka topic: {KafkaConfig.TOPIC}")
    register_kafka_source(t_env)

    output_path = os.path.abspath(OutputConfig.OUTPUT_DIR)
    print(f"Output directory: {output_path}")
    register_sink_table(t_env, output_path)

    flatten_query = """
        SELECT
            equip_id,
            event_time,
            sensor_entry.key as sensor_id,
            CASE
                WHEN sensor_entry.value = 'null' THEN CAST(NULL AS DOUBLE)
                ELSE CAST(sensor_entry.value AS DOUBLE)
            END as sensor_value
        FROM sensor_source
        CROSS JOIN UNNEST(`data`) AS sensor_entry(key, value)
    """

    flattened = t_env.sql_query(flatten_query)
    t_env.create_temporary_view("flattened_sensors", flattened)

    fingerprint_query = f"""
        WITH window_data AS (
            SELECT
                equip_id,
                sensor_id,
                window_start,
                window_end,
                sensor_value,
                COUNT(*) OVER w as sample_count,
                AVG(sensor_value) OVER w as mean_val,
                MIN(sensor_value) OVER w as min_val,
                MAX(sensor_value) OVER w as max_val
            FROM TABLE(
                TUMBLE(
                    TABLE flattened_sensors,
                    DESCRIPTOR(event_time),
                    INTERVAL '{FlinkConfig.WINDOW_SIZE_MINUTES}' MINUTES
                )
            )
            WHERE sensor_value IS NOT NULL
            WINDOW w AS (PARTITION BY equip_id, sensor_id, window_start, window_end)
        ),
        aggregated AS (
            SELECT DISTINCT
                equip_id,
                sensor_id,
                window_start,
                window_end,
                min_val,
                max_val,
                mean_val,
                sample_count
            FROM window_data
        )
        SELECT
            gen_uuid() as fingerprint_id,
            equip_id,
            window_start,
            window_end,
            sensor_id,
            min_val as min_value,
            max_val as max_value,
            mean_val as mean_value,
            mean_val as median_value,
            POWER(max_val - min_val, 2) / 12 as variance_value,
            sample_count
        FROM aggregated
    """

    print("Starting fingerprint processing...")
    print(f"Window size: {FlinkConfig.WINDOW_SIZE_MINUTES} minute(s)")

    result = t_env.sql_query(fingerprint_query)
    result.execute_insert("fingerprint_sink").wait()


def run_fingerprint_job_with_pandas():
    """Alternative implementation using Pandas for accurate median/variance."""
    import json
    import pandas as pd
    from collections import defaultdict
    from kafka import KafkaConsumer

    print("Starting fingerprint job with Pandas aggregation...")
    print(f"Connecting to Kafka: {KafkaConfig.BOOTSTRAP_SERVERS_HOST}")
    print(f"Topic: {KafkaConfig.TOPIC}")
    print(f"Window size: {FlinkConfig.WINDOW_SIZE_MINUTES} minute(s)")

    bootstrap = os.environ.get(
        "KAFKA_BOOTSTRAP_SERVERS", KafkaConfig.BOOTSTRAP_SERVERS_HOST
    )

    consumer = KafkaConsumer(
        KafkaConfig.TOPIC,
        bootstrap_servers=bootstrap,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        group_id=KafkaConfig.GROUP_ID,
    )

    window_buffer = defaultdict(list)
    window_size_ms = FlinkConfig.WINDOW_SIZE_MINUTES * 60 * 1000

    output_path = os.path.join(OutputConfig.OUTPUT_DIR, "fingerprints.csv")
    os.makedirs(OutputConfig.OUTPUT_DIR, exist_ok=True)

    header_written = os.path.exists(output_path)

    print(f"Output file: {output_path}")
    print("Processing messages... (Press Ctrl+C to stop)")

    try:
        for message in consumer:
            data = message.value
            equip_id = data["equip_id"]
            timestamp = data["timestamp"]
            sensor_data = data["data"]

            window_start = (timestamp // window_size_ms) * window_size_ms
            window_end = window_start + window_size_ms

            for sensor_id, value in sensor_data.items():
                if value != "null":
                    key = (equip_id, sensor_id, window_start)
                    window_buffer[key].append(
                        {
                            "value": float(value),
                            "window_end": window_end,
                        }
                    )

            current_time = datetime.now().timestamp() * 1000
            completed_windows = []

            for key, values in window_buffer.items():
                equip_id, sensor_id, window_start = key
                window_end = values[0]["window_end"]

                if current_time > window_end + 5000:
                    completed_windows.append(key)

                    vals = [v["value"] for v in values]
                    df = pd.DataFrame({"value": vals})

                    fingerprint = {
                        "fingerprint_id": str(uuid.uuid4()),
                        "equip_id": equip_id,
                        "window_start": datetime.fromtimestamp(
                            window_start / 1000
                        ).isoformat(),
                        "window_end": datetime.fromtimestamp(
                            window_end / 1000
                        ).isoformat(),
                        "sensor_id": sensor_id,
                        "min_value": df["value"].min(),
                        "max_value": df["value"].max(),
                        "mean_value": df["value"].mean(),
                        "median_value": df["value"].median(),
                        "variance_value": df["value"].var(),
                        "sample_count": len(vals),
                    }

                    fp_df = pd.DataFrame([fingerprint])
                    fp_df.to_csv(
                        output_path,
                        mode="a",
                        header=not header_written,
                        index=False,
                    )
                    header_written = True

                    print(
                        f"Fingerprint: equip={equip_id}, sensor={sensor_id}, "
                        f"samples={len(vals)}, mean={fingerprint['mean_value']:.3f}"
                    )

            for key in completed_windows:
                del window_buffer[key]

    except KeyboardInterrupt:
        print("\nStopping fingerprint job...")
    finally:
        consumer.close()
        print(f"Results saved to: {output_path}")
