import argparse
import json
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

try:
    from openai import OpenAI
    from dotenv import load_dotenv
except Exception as exc:
    raise SystemExit(
        "Missing dependency. Install with: pip install -r requirements.txt"
    ) from exc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("llm_snapshot_test")


def _ensure_env() -> None:
    dotenv_path = os.environ.get("DOTENV_PATH", ".env")
    load_dotenv(dotenv_path=dotenv_path)
    if not os.environ.get("OPENAI_API_KEY") and os.path.isfile(dotenv_path):
        with open(dotenv_path, "r", encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                if key == "OPENAI_API_KEY" and value:
                    os.environ["OPENAI_API_KEY"] = value
                elif key == "OPENAI_MODEL" and value:
                    os.environ.setdefault("OPENAI_MODEL", value)

    if not os.environ.get("OPENAI_API_KEY"):
        raise SystemExit(
            "OPENAI_API_KEY not set. Add it to .env or set the environment variable."
        )


def _load_json(path: str) -> Dict[str, Any]:
    # Handle BOM-marked JSON files from Windows editors
    with open(path, "r", encoding="utf-8-sig") as f:
        return json.load(f)


def _schema() -> Dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "severity": {
                "type": "string",
                "enum": ["normal", "mild", "critical"],
                "description": "Overall process severity",
            },
            "diagnostics": {"type": "array", "items": {"type": "string"}},
            "recommendations": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["severity", "diagnostics", "recommendations"],
    }


def _load_prompt(path: str) -> str:
    if not path or not os.path.isfile(path):
        raise SystemExit("Prompt file not found. Provide --prompt prompt.txt")
    with open(path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as exc:
            raise SystemExit("Prompt file must be JSON with key 'llm_snapshot_test'.") from exc
    prompt = data.get("llm_snapshot_test") if isinstance(data, dict) else None
    if not prompt or not isinstance(prompt, str) or not prompt.strip():
        raise SystemExit("Prompt key 'llm_snapshot_test' missing or empty in prompt.txt")
    return prompt.strip()


def load_prompt(path: str) -> str:
    return _load_prompt(path)


def _enforce_rules(result: Dict[str, Any]) -> Dict[str, Any]:
    severity = str(result.get("severity", "")).lower().strip()
    if severity == "normal":
        result["diagnostics"] = []
        result["recommendations"] = []
    return result


def run_snapshot(
    snapshot: Dict[str, Any],
    metadata: Dict[str, Any],
    prompt: str,
    output_dir: str,
    model: Optional[str] = None,
) -> Tuple[str, str]:
    _ensure_env()

    model_name = model or os.environ.get("OPENAI_MODEL", "gpt-5")

    # Extract fingerprint info for logging
    fp_uuid = snapshot.get("uuid", "unknown")
    equip_id = snapshot.get("equip_id", "unknown")
    start_ms = snapshot.get("start_ms", 0)
    end_ms = snapshot.get("end_ms", 0)
    data_tags = list(snapshot.get("data", {}).keys())

    logger.info("=" * 60)
    logger.info("SNAPSHOT ANALYSIS - START")
    logger.info("=" * 60)

    # Log LLM Input
    logger.info("-" * 40)
    logger.info("LLM INPUT:")
    logger.info(f"  Model: {model_name}")
    logger.info(f"  Fingerprint UUID: {fp_uuid}")
    logger.info(f"  Equipment ID: {equip_id}")
    logger.info(f"  Time Window: {start_ms} - {end_ms}")
    logger.info(f"  Tags in Fingerprint: {len(data_tags)}")
    logger.info(f"  Tags in Metadata: {len(metadata)}")
    logger.info(f"  System Prompt: {prompt[:100]}..." if len(prompt) > 100 else f"  System Prompt: {prompt}")
    logger.debug(f"  Fingerprint Data:\n{json.dumps(snapshot, indent=2)}")
    logger.debug(f"  Metadata:\n{json.dumps(metadata, indent=2)}")
    logger.info("-" * 40)

    client = OpenAI()

    try:
        logger.info("Sending request to OpenAI API...")
        response = client.responses.create(
            model=model_name,
            input=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": "Fingerprint JSON:\n" + json.dumps(snapshot)},
                {"role": "user", "content": "Tag metadata JSON:\n" + json.dumps(metadata)},
            ],
            text={
                "format": {
                    "type": "json_schema",
                    "name": "kiln_snapshot_diagnosis",
                    "schema": _schema(),
                    "strict": True,
                }
            },
        )

        output_text = response.output_text
        parsed = json.loads(output_text)
        parsed = _enforce_rules(parsed)
        output_text = json.dumps(parsed, ensure_ascii=False, indent=2)

        # Log LLM Output - SUCCESS
        logger.info("-" * 40)
        logger.info("LLM OUTPUT (SUCCESS):")
        logger.info(f"  Severity: {parsed.get('severity', 'unknown')}")
        logger.info(f"  Diagnostics Count: {len(parsed.get('diagnostics', []))}")
        logger.info(f"  Recommendations Count: {len(parsed.get('recommendations', []))}")
        if parsed.get('diagnostics'):
            logger.info(f"  Diagnostics: {parsed.get('diagnostics')}")
        if parsed.get('recommendations'):
            logger.info(f"  Recommendations: {parsed.get('recommendations')}")
        logger.info(f"  Full Response:\n{output_text}")
        logger.info("-" * 40)

    except Exception as e:
        # Log LLM Output - FAILURE
        logger.error("-" * 40)
        logger.error("LLM OUTPUT (FAILED):")
        logger.error(f"  Fingerprint UUID: {fp_uuid}")
        logger.error(f"  Error Type: {type(e).__name__}")
        logger.error(f"  Error Message: {str(e)}")
        logger.error("-" * 40)
        raise

    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    out_path = os.path.join(output_dir, f"llm_output_{timestamp}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(output_text)
        f.write("\n")

    logger.info(f"Output saved to: {out_path}")
    logger.info("=" * 60)
    logger.info("SNAPSHOT ANALYSIS - END")
    logger.info("=" * 60)

    return out_path, output_text


def main() -> int:
    _ensure_env()

    parser = argparse.ArgumentParser(description="Send kiln fingerprint to OpenAI and return JSON diagnosis.")
    parser.add_argument(
        "--snapshot",
        default="sample_snapshot.json",
        help="Path to fingerprint snapshot JSON (default: sample_snapshot.json)",
    )
    parser.add_argument(
        "--metadata",
        default="sample_metadata.json",
        help="Path to tag metadata JSON (default: sample_metadata.json)",
    )
    parser.add_argument("--prompt", default="prompt.txt", help="Path to prompt file (default: prompt.txt)")
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Directory to save JSON output files (default: outputs)",
    )
    parser.add_argument("--model", default=None, help="OpenAI model (default: env OPENAI_MODEL or gpt-5)")
    args = parser.parse_args()

    snapshot = _load_json(args.snapshot)
    metadata = _load_json(args.metadata)

    prompt = _load_prompt(args.prompt)
    out_path, output_text = run_snapshot(
        snapshot=snapshot,
        metadata=metadata,
        prompt=prompt,
        output_dir=args.output_dir,
        model=args.model,
    )
    print(output_text)
    print(f"Saved output to: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
