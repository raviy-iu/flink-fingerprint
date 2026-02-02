from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Duration

from utils.config import KafkaConfig, FlinkConfig


def kafka_source():
    return KafkaSource.builder() \
        .set_bootstrap_servers(KafkaConfig.bootstrap_servers()) \
        .set_topics(KafkaConfig.INPUT_TOPIC) \
        .set_group_id(KafkaConfig.GROUP_ID) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()


def kafka_sink():
    return KafkaSink.builder() \
        .set_bootstrap_servers(KafkaConfig.bootstrap_servers()) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(KafkaConfig.OUTPUT_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .build()


watermark_strategy = WatermarkStrategy \
    .for_bounded_out_of_orderness(
        Duration.of_seconds(FlinkConfig.WATERMARK_SECONDS)
    )
