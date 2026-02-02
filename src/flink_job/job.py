import os
import sys

# Add the src directory to Python path for package imports
# In Docker: /opt/flink/jobs/src, locally: <project>/src
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import TumblingEventTimeWindows

from flink_job.kafka_config import kafka_source, kafka_sink, watermark_strategy
from flink_job.serialization import parse_sensor_event, build_fingerprint_json
from flink_job.aggregations import compute_stats


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # -------------------------
    # Source
    # -------------------------
    source = kafka_source()

    stream = env.from_source(
        source,
        watermark_strategy,
        "sensor-source"
    )

    # -------------------------
    # Parse JSON
    # -------------------------
    parsed = stream.map(
        parse_sensor_event,
        output_type=Types.PICKLED_BYTE_ARRAY()
    )

    # -------------------------
    # Flatten: one record per sensor
    # (equip_id, sensor_id, value, timestamp)
    # -------------------------
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

    # -------------------------
    # Window aggregation
    # -------------------------
    windowed = flattened \
        .key_by(lambda x: x[0]) \
        .window(TumblingEventTimeWindows.of(Time.minutes(1))) \
        .process(FingerprintWindowFunction(),
                 output_type=Types.STRING())

    # -------------------------
    # Sink
    # -------------------------
    windowed.sink_to(
        kafka_sink()
    )

    env.execute("Fingerprint Generator")


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
    main()
