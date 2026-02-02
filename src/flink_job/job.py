import os
import sys

# Add directories to Python path for direct imports (no __init__.py needed)
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
utils_dir = os.path.join(src_dir, "utils")

for path in [current_dir, src_dir, utils_dir]:
    if path not in sys.path:
        sys.path.insert(0, path)

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import TumblingEventTimeWindows

from kafka_config import kafka_source, kafka_sink, watermark_strategy
from serialization import parse_sensor_event, build_fingerprint_json
from aggregations import compute_stats
from config import KafkaConfig, FlinkConfig


def main():
    print("=" * 60)
    print("Flink Fingerprint Generator Job")
    print("=" * 60)
    print(f"Kafka Bootstrap: {KafkaConfig.bootstrap_servers()}")
    print(f"Input Topic:     {KafkaConfig.INPUT_TOPIC}")
    print(f"Output Topic:    {KafkaConfig.OUTPUT_TOPIC}")
    print(f"Consumer Group:  {KafkaConfig.GROUP_ID}")
    print(f"Window Size:     {FlinkConfig.WINDOW_SIZE_MINUTES} minute(s)")
    print(f"Watermark:       {FlinkConfig.WATERMARK_SECONDS} seconds")
    print("=" * 60)

    print("[1/6] Creating execution environment...")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    print("[2/6] Configuring Kafka source...")
    source = kafka_source()

    stream = env.from_source(
        source,
        watermark_strategy,
        "sensor-source"
    )

    print("[3/6] Setting up JSON parser...")
    parsed = stream.map(
        parse_sensor_event,
        output_type=Types.PICKLED_BYTE_ARRAY()
    )

    print("[4/6] Configuring data flattening...")
    flattened = parsed.flat_map(
        lambda e: [
            (e.equip_id, sid, float(val), e.timestamp)
            for sid, val in e.data.items()
            if val != "null"
        ],
        output_type=Types.TUPLE([
            Types.INT(),
            Types.STRING(),
            Types.FLOAT(),
            Types.LONG()
        ])
    )

    print("[5/6] Setting up windowed aggregation...")
    windowed = flattened \
        .key_by(lambda x: x[0]) \
        .window(TumblingEventTimeWindows.of(Time.minutes(1))) \
        .process(FingerprintWindowFunction(),
                 output_type=Types.STRING())

    print("[6/6] Configuring Kafka sink...")
    windowed.sink_to(
        kafka_sink()
    )

    print("-" * 60)
    print("Submitting job to Flink cluster...")
    print("-" * 60)
    env.execute("Fingerprint Generator")
    print("Job completed.")


# -------------------------
# Window Function
# -------------------------
from pyflink.datastream.functions import ProcessWindowFunction
from collections import defaultdict


class FingerprintWindowFunction(ProcessWindowFunction):

    def process(self, key, context, elements):
        """PyFlink 1.18 uses generator pattern - yield results instead of out.collect()"""
        sensor_values = defaultdict(list)

        for e in elements:
            _, sensor_id, value, _ = e
            sensor_values[sensor_id].append(value)

        stats = {
            sensor_id: compute_stats(values)
            for sensor_id, values in sensor_values.items()
        }

        result = build_fingerprint_json(
            equip_id=key,
            start_ms=context.window().start,
            end_ms=context.window().end,
            stats=stats
        )

        yield result


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nJob cancelled by user.")
    except Exception as e:
        print(f"\n[ERROR] Job failed: {e}")
        raise
