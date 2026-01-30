"""Configuration constants for the fingerprint project."""


class EquipmentConfig:
    """Equipment and sensor configuration."""

    EQUIPMENT_IDS: tuple = (110, 111, 112, 113, 114)
    SENSOR_IDS: tuple = ("0001", "0002", "0003", "0004", "0005", "0006", "0007", "0150")


class FlinkConfig:
    """Flink job configuration."""

    WINDOW_SIZE_MINUTES: int = 1
    CHECKPOINT_INTERVAL_MS: int = 30000
    PARALLELISM: int = 2


class KafkaConfig:
    """Kafka configuration."""

    # For producer running on host
    BOOTSTRAP_SERVERS_HOST: str = "localhost:29093"
    # For Flink consumer running in Docker
    BOOTSTRAP_SERVERS_DOCKER: str = "kafka:9093"
    TOPIC: str = "sensor-data"
    GROUP_ID: str = "fingerprint-consumer"


class OutputConfig:
    """Output configuration."""

    OUTPUT_DIR: str = "output"
    CSV_PREFIX: str = "fingerprint"
