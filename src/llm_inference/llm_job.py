import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

# Add parent directories to Python path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
root_dir = os.path.dirname(src_dir)
utils_dir = os.path.join(src_dir, "utils")

for path in [src_dir, root_dir, utils_dir]:
    if path not in sys.path:
        sys.path.insert(0, path)

from kafka import KafkaConsumer

from config import KafkaConfig
from generate_fingerprints import prepare_tag_files, write_fingerprints_jsonl
from llm_equip_analyse import run_equip_analyse
from llm_snapshot_test import load_prompt as load_snapshot_prompt
from llm_snapshot_test import run_snapshot

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("llm_job")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _load_state(path: str) -> Dict[str, Any]:
    if not os.path.isfile(path):
        return {}
    with open(path, "r", encoding="utf-8-sig") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return {}


def _save_state(path: str, state: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
        f.write("\n")


def _should_run_equip_analyse(
    state: Dict[str, Any],
    equip_id: str,
    min_interval: timedelta,
) -> bool:
    last_run = state.get("last_run_by_equip", {}).get(str(equip_id))
    if not last_run:
        return True
    try:
        last_dt = datetime.fromisoformat(last_run)
    except ValueError:
        return True
    return datetime.now(timezone.utc) - last_dt >= min_interval


def create_fingerprint_consumer(
    group_id: str = "llm-inference-consumer",
    auto_offset_reset: str = "latest",
    consumer_timeout_ms: int = 120000,  # 2 minutes default timeout
) -> KafkaConsumer:
    """Create Kafka consumer for fingerprint-output topic."""
    return KafkaConsumer(
        KafkaConfig.OUTPUT_TOPIC,
        bootstrap_servers=KafkaConfig.bootstrap_servers(),
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=consumer_timeout_ms,
    )


def consume_fingerprints(
    consumer: KafkaConsumer,
    max_count: Optional[int] = None,
    timeout_seconds: Optional[int] = None,
):
    """
    Consume fingerprints from Kafka topic.

    Args:
        consumer: KafkaConsumer instance
        max_count: Maximum number of fingerprints to consume (None = unlimited)
        timeout_seconds: Stop consuming after this many seconds (None = unlimited)

    Yields:
        Dict containing fingerprint data
    """
    start_time = time.time()
    count = 0

    logger.info(f"Listening to '{KafkaConfig.OUTPUT_TOPIC}' on {KafkaConfig.bootstrap_servers()}...")

    for message in consumer:
        fingerprint_data = message.value

        # Extract fingerprint (handle both wrapped and unwrapped format)
        if "fingerprint" in fingerprint_data:
            yield fingerprint_data
        else:
            yield {"fingerprint": fingerprint_data}

        count += 1

        if max_count is not None and count >= max_count:
            logger.info(f"Reached max count ({max_count})")
            break

        if timeout_seconds is not None and (time.time() - start_time) >= timeout_seconds:
            logger.info(f"Timeout reached ({timeout_seconds}s)")
            break


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Consume fingerprints from Kafka and run LLM analysis."
    )
    parser.add_argument(
        "--excel",
        default="Kiln_mapping.xlsx",
        help="Path to Kiln mapping Excel (default: Kiln_mapping.xlsx)",
    )
    parser.add_argument(
        "--sheet",
        default=None,
        help="Excel sheet name (default: first sheet)",
    )
    parser.add_argument(
        "--equipment-type",
        default="Kiln",
        help='Equipment Type label (default: "Kiln")',
    )
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Directory to save outputs (default: outputs)",
    )
    parser.add_argument(
        "--snapshot-prompt",
        default="prompt.txt",
        help="Prompt file for snapshot LLM (default: prompt.txt)",
    )
    parser.add_argument(
        "--equip-prompt",
        default="prompt.txt",
        help="Prompt file for equipment LLM (default: prompt.txt)",
    )
    parser.add_argument(
        "--model",
        default=None,
        help="OpenAI model (default: env OPENAI_MODEL or gpt-5)",
    )
    parser.add_argument(
        "--analysis-interval-hours",
        type=int,
        default=24,
        help="Min hours between equipment analyses (default: 24)",
    )
    parser.add_argument(
        "--tag-id-mode",
        default="seq",
        choices=["seq", "uuid", "tag"],
        help="Tag ID mode for fingerprints: seq=0001, uuid, tag (default: seq)",
    )
    parser.add_argument(
        "--max-fingerprints",
        type=int,
        default=None,
        help="Maximum fingerprints to process (default: unlimited, runs continuously)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=None,
        help="Stop consuming after N seconds (default: unlimited)",
    )
    parser.add_argument(
        "--consumer-group",
        default="llm-inference-consumer",
        help="Kafka consumer group ID (default: llm-inference-consumer)",
    )
    parser.add_argument(
        "--offset-reset",
        default="latest",
        choices=["earliest", "latest"],
        help="Kafka offset reset strategy (default: latest)",
    )
    parser.add_argument(
        "--poll-timeout",
        type=int,
        default=120,
        help="Kafka poll timeout in seconds - how long to wait for messages (default: 120)",
    )
    args = parser.parse_args()

    # Prepare tag files from Excel mapping
    tag_map_path = os.path.join(args.output_dir, "tag_uuid_map.json")
    metadata_path = os.path.join(args.output_dir, "tag_metadata.json")

    tag_ids, metadata = prepare_tag_files(
        excel_path=args.excel,
        equipment_type=args.equipment_type,
        sheet=args.sheet,
        map_path=tag_map_path,
        metadata_path=metadata_path,
        tag_id_mode=args.tag_id_mode,
    )

    # Load snapshot prompt
    snapshot_prompt = load_snapshot_prompt(args.snapshot_prompt)

    # State management for equipment analysis
    state_path = os.path.join(args.output_dir, "equip_analyse_state.json")
    state = _load_state(state_path)
    min_interval = timedelta(hours=args.analysis_interval_hours)

    # Create Kafka consumer
    consumer = create_fingerprint_consumer(
        group_id=args.consumer_group,
        auto_offset_reset=args.offset_reset,
        consumer_timeout_ms=args.poll_timeout * 1000,  # Convert seconds to milliseconds
    )

    # Track processed fingerprints for saving
    processed_fingerprints = []
    equip_ids_seen = set()

    logger.info("=" * 70)
    logger.info("LLM JOB - STARTING")
    logger.info("=" * 70)
    logger.info(f"Output directory: {args.output_dir}")
    logger.info(f"Max fingerprints: {args.max_fingerprints or 'unlimited'}")
    logger.info(f"Timeout: {args.timeout or 'unlimited'}s")
    logger.info(f"Poll timeout: {args.poll_timeout}s (waiting for Kafka messages)")
    logger.info(f"Consumer group: {args.consumer_group}")
    logger.info(f"Offset reset: {args.offset_reset}")

    try:
        for fp_data in consume_fingerprints(
            consumer,
            max_count=args.max_fingerprints,
            timeout_seconds=args.timeout,
        ):
            fp = fp_data.get("fingerprint", fp_data)
            equip_id = str(fp.get("equip_id", "unknown"))
            fp_uuid = fp.get("uuid", "unknown")

            logger.info("*" * 70)
            logger.info(f"PROCESSING FINGERPRINT: {fp_uuid}")
            logger.info(f"  Equipment ID: {equip_id}")
            logger.info("*" * 70)

            # Track if this fingerprint was processed (for saving even on partial failures)
            fp_processed = False

            # Run equipment analysis if needed (once per equipment per interval)
            if equip_id not in equip_ids_seen:
                equip_ids_seen.add(equip_id)

                if _should_run_equip_analyse(state, equip_id, min_interval):
                    logger.info(f"Running equipment analysis for {equip_id}...")
                    try:
                        out_path, _ = run_equip_analyse(
                            excel=args.excel,
                            sheet=args.sheet,
                            industry="Cement Industry",
                            equipment_type=args.equipment_type,
                            prompt_path=args.equip_prompt,
                            output_dir=args.output_dir,
                            model=args.model,
                        )
                        state.setdefault("last_run_by_equip", {})[equip_id] = datetime.now(
                            timezone.utc
                        ).isoformat()
                        state.setdefault("last_output_by_equip", {})[equip_id] = out_path
                        _save_state(state_path, state)
                    except Exception as e:
                        logger.error(f"Equipment analysis failed for {equip_id}: {type(e).__name__}: {e}")
                        logger.warning("Continuing with snapshot analysis...")
                else:
                    logger.info(f"Skipping equipment analysis for {equip_id} (already run within interval)")

            # Run snapshot analysis for each fingerprint
            logger.info(f"Running snapshot analysis for {fp_uuid}...")
            try:
                out_path, _ = run_snapshot(
                    snapshot=fp,
                    metadata=metadata,
                    prompt=snapshot_prompt,
                    output_dir=args.output_dir,
                    model=args.model,
                )
                fp_processed = True
            except Exception as e:
                logger.error(f"Snapshot analysis failed for {fp_uuid}: {type(e).__name__}: {e}")
                fp_processed = False

            # Always save the fingerprint (even if LLM failed) so we don't lose data
            processed_fingerprints.append(fp_data)
            logger.info(f"Fingerprints processed so far: {len(processed_fingerprints)} (LLM success: {fp_processed})")

    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed")

    # Save processed fingerprints to JSONL file
    logger.info("=" * 70)
    logger.info("LLM JOB - SUMMARY")
    logger.info("=" * 70)
    if processed_fingerprints:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        fp_out_path = os.path.join(args.output_dir, f"fingerprints_{timestamp}.jsonl")
        write_fingerprints_jsonl(processed_fingerprints, fp_out_path)
        logger.info(f"Total fingerprints processed: {len(processed_fingerprints)}")
        logger.info(f"Fingerprints saved to: {fp_out_path}")
    else:
        logger.warning("No fingerprints received from Kafka")
    logger.info("=" * 70)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
