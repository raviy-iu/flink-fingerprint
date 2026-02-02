import uuid

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    EnvironmentSettings,
    StreamTableEnvironment,
    DataTypes
)
from pyflink.table.udf import udf

from utils.config import FlinkConfig, KafkaConfig


def run_fingerprint_job():
    """
    Fingerprint aggregation pipeline
    PyFlink 1.18 compatible (PROCESSING TIME)
    """

    # ---------------------------------------------------------
    # Stream + Table Environment (CORRECT for 1.18)
    # ---------------------------------------------------------
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(FlinkConfig.PARALLELISM)

    settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(
        stream_execution_environment=env,
        environment_settings=settings
    )

    t_env.get_config().set(
        "execution.checkpointing.interval",
        str(FlinkConfig.CHECKPOINT_INTERVAL_MS)
    )

    # ---------------------------------------------------------
    # UUID UDF
    # ---------------------------------------------------------
    @udf(result_type=DataTypes.STRING())
    def gen_uuid():
        return str(uuid.uuid4())

    t_env.create_temporary_function("gen_uuid", gen_uuid)

    # ---------------------------------------------------------
    # Kafka Source (PROCESSING TIME → GUARANTEED OUTPUT)
    # ---------------------------------------------------------
    t_env.execute_sql(f"""
        CREATE TABLE sensor_source (
            equip_id INT,
            `timestamp` BIGINT,
            `data` MAP<STRING, STRING>,
            proc_time AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KafkaConfig.INPUT_TOPIC}',
            'properties.bootstrap.servers' = '{KafkaConfig.BOOTSTRAP_SERVERS_DOCKER}',
            'properties.group.id' = '{KafkaConfig.GROUP_ID}',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # ---------------------------------------------------------
    # Kafka Sink
    # ---------------------------------------------------------
    t_env.execute_sql(f"""
        CREATE TABLE fingerprint_output (
            fingerprint_id STRING,
            equip_id INT,
            sensor_id STRING,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            min_value DOUBLE,
            max_value DOUBLE,
            avg_value DOUBLE,
            count_value BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KafkaConfig.OUTPUT_TOPIC}',
            'properties.bootstrap.servers' = '{KafkaConfig.BOOTSTRAP_SERVERS_DOCKER}',
            'format' = 'json'
        )
    """)

    # ---------------------------------------------------------
    # Aggregation (1-minute tumbling window)
    # ---------------------------------------------------------
    t_env.execute_sql(f"""
        INSERT INTO fingerprint_output
        SELECT
            gen_uuid() AS fingerprint_id,
            equip_id,
            sensor_id,
            TUMBLE_START(proc_time, INTERVAL '{FlinkConfig.WINDOW_SIZE_MINUTES}' MINUTES),
            TUMBLE_END(proc_time, INTERVAL '{FlinkConfig.WINDOW_SIZE_MINUTES}' MINUTES),
            MIN(sensor_value),
            MAX(sensor_value),
            AVG(sensor_value),
            COUNT(*)
        FROM (
            SELECT
                equip_id,
                sensor_entry.`key` AS sensor_id,
                TRY_CAST(sensor_entry.`value` AS DOUBLE) AS sensor_value,
                proc_time
            FROM sensor_source
            CROSS JOIN UNNEST(`data`) AS sensor_entry (`key`, `value`)
        )
        WHERE sensor_value IS NOT NULL
        GROUP BY
            equip_id,
            sensor_id,
            TUMBLE(proc_time, INTERVAL '{FlinkConfig.WINDOW_SIZE_MINUTES}' MINUTES)
    """)

    print("✅ Fingerprint streaming job started successfully")
