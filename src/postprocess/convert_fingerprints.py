"""Post-processing script to convert flat Flink JSON output to nested format.

Input (Flat JSON from Flink):
    {"fingerprint_id": "...", "tag_id": "tag_001", "equipment_id": "eq_110", ...}
    {"fingerprint_id": "...", "tag_id": "tag_002", "equipment_id": "eq_111", ...}

Output (Nested JSON):
    {
        "fingerprint_001": {
            "tag_001": {
                "equipment_id": "eq_110",
                "eq_type": "kiln",
                ...
            },
            "tag_002": { ... }
        }
    }
"""

import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Any

from src.utils.config import OutputConfig


def read_flat_json_files(input_dir: str) -> List[Dict[str, Any]]:
    """Read all flat JSON files from the Flink output directory.

    Flink outputs files in subdirectories with part-* naming convention.

    Args:
        input_dir: Directory containing Flink output files

    Returns:
        List of parsed JSON records
    """
    records = []
    input_path = Path(input_dir)

    # Flink creates files like: output/part-*.json or nested directories
    for json_file in input_path.rglob("*"):
        if json_file.is_file() and not json_file.name.startswith("."):
            try:
                with open(json_file, "r") as f:
                    content = f.read().strip()
                    if not content:
                        continue
                    # Handle both single JSON objects and JSON lines
                    for line in content.split("\n"):
                        line = line.strip()
                        if line:
                            try:
                                record = json.loads(line)
                                records.append(record)
                            except json.JSONDecodeError:
                                # Try parsing as single JSON if JSONL fails
                                pass
            except Exception as e:
                print(f"Warning: Could not read {json_file}: {e}")

    return records


def group_by_window(records: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Group records by their window (start_ms, end_ms).

    Args:
        records: List of flat JSON records

    Returns:
        Dictionary mapping window key to list of records
    """
    grouped = defaultdict(list)

    for record in records:
        # Create a unique key for each window
        window_key = f"{record.get('start_ms', 0)}_{record.get('end_ms', 0)}"
        grouped[window_key].append(record)

    return grouped


def convert_to_nested_format(
    records: List[Dict[str, Any]],
    fingerprint_id: str
) -> Dict[str, Dict[str, Any]]:
    """Convert flat records to nested fingerprint format.

    Args:
        records: List of flat JSON records for a single window
        fingerprint_id: ID to use for this fingerprint

    Returns:
        Nested dictionary in required format
    """
    fingerprint_data = {}

    for i, record in enumerate(records, start=1):
        tag_id = record.get("tag_id", f"tag_{i:03d}")

        fingerprint_data[tag_id] = {
            "equipment_id": record.get("equipment_id", ""),
            "eq_type": record.get("eq_type", "unknown"),
            "sensor_id": record.get("sensor_id", ""),
            "start_ms": record.get("start_ms", 0),
            "end_ms": record.get("end_ms", 0),
            "min": record.get("min_value", 0),
            "max": record.get("max_value", 0),
            "median": record.get("median_value", 0),
            "mean": record.get("mean_value", 0),
            "std_dev": record.get("std_dev", 0),
        }

    return {fingerprint_id: fingerprint_data}


def process_flink_output(
    input_dir: str = None,
    output_dir: str = None,
    single_file: bool = False
) -> List[str]:
    """Process Flink flat JSON output and convert to nested format.

    Args:
        input_dir: Directory containing Flink output (default: output/)
        output_dir: Directory to write nested JSON (default: output/nested/)
        single_file: If True, write all fingerprints to one file

    Returns:
        List of output file paths
    """
    input_dir = input_dir or OutputConfig.OUTPUT_DIR
    output_dir = output_dir or os.path.join(OutputConfig.OUTPUT_DIR, "nested")

    print("=" * 60)
    print("POST-PROCESSING: Converting Flat JSON to Nested Format")
    print("=" * 60)
    print(f"Input directory: {input_dir}")
    print(f"Output directory: {output_dir}")

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    # Read all flat JSON records
    print("\nReading Flink output files...")
    records = read_flat_json_files(input_dir)
    print(f"Found {len(records)} records")

    if not records:
        print("No records found. Make sure Flink job has produced output.")
        return []

    # Group by window
    print("\nGrouping by window...")
    grouped = group_by_window(records)
    print(f"Found {len(grouped)} unique windows")

    # Convert each window to nested format
    print("\nConverting to nested format...")
    output_files = []
    all_fingerprints = {}

    for i, (window_key, window_records) in enumerate(sorted(grouped.items()), start=1):
        fingerprint_id = f"fingerprint_{i:03d}"
        nested = convert_to_nested_format(window_records, fingerprint_id)

        if single_file:
            all_fingerprints.update(nested)
        else:
            # Write individual file per fingerprint
            output_path = os.path.join(output_dir, f"{fingerprint_id}.json")
            with open(output_path, "w") as f:
                json.dump(nested, f, indent=4)
            output_files.append(output_path)
            print(f"  Created: {fingerprint_id}.json ({len(window_records)} tags)")

    if single_file:
        # Write all fingerprints to single file
        output_path = os.path.join(output_dir, "all_fingerprints.json")
        with open(output_path, "w") as f:
            json.dump(all_fingerprints, f, indent=4)
        output_files.append(output_path)
        print(f"  Created: all_fingerprints.json ({len(all_fingerprints)} fingerprints)")

    print("\n" + "=" * 60)
    print(f"Post-processing complete! {len(output_files)} file(s) created.")
    print(f"Output location: {output_dir}")
    print("=" * 60)

    return output_files


def run_postprocess():
    """Entry point for post-processing."""
    process_flink_output()


if __name__ == "__main__":
    run_postprocess()
