import os


class EquipmentConfig:
    EQUIPMENT_IDS = [110, 111, 112, 113, 114]
    SENSOR_IDS = ["0001", "0002", "0003", "0004", "0005"]


class KafkaConfig:
    BOOTSTRAP_SERVERS_HOST = "localhost:29093"
    BOOTSTRAP_SERVERS_DOCKER = "kafka:9093"

    INPUT_TOPIC = "synthetic-sensor-data"
    OUTPUT_TOPIC = "fingerprint-output"

    GROUP_ID = "fingerprint-consumer"

    @staticmethod
    def bootstrap_servers():
        return (
            KafkaConfig.BOOTSTRAP_SERVERS_DOCKER
            if os.getenv("RUN_ENV") == "docker"
            else KafkaConfig.BOOTSTRAP_SERVERS_HOST
        )


class FlinkConfig:
    WINDOW_SIZE_MINUTES = 1
    WATERMARK_SECONDS = 10
    PARALLELISM = 2
