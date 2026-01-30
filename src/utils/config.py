class EquipmentConfig:
    EQUIPMENT_IDS = [110, 111, 112, 113, 114]
    SENSOR_IDS = [
        "0001",
        "0002",
        "0003",
        "0004",
        "0005",
        "0006",
        "0007",
        "0150",
    ]


class FlinkConfig:
    WINDOW_SIZE_MINUTES = 1
    WATERMARK_SECONDS = 10
    ALLOWED_LATENESS_MINUTES = 1
    CHECKPOINT_INTERVAL_MS = 60000
    PARALLELISM = 2


class KafkaConfig:
    BOOTSTRAP_SERVERS_HOST = "localhost:29093"
    BOOTSTRAP_SERVERS_DOCKER = "kafka:9093"

    INPUT_TOPIC = "sensor-data"
    OUTPUT_TOPIC = "fingerprint-output"

    GROUP_ID = "fingerprint-consumer"
