import argparse
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

try:
    from openai import OpenAI
    from dotenv import load_dotenv
except Exception as exc:
    raise SystemExit("Missing dependency. Install with: pip install -r requirements.txt") from exc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("llm_equip_analyse")


def _ensure_env() -> None:
    dotenv_path = os.environ.get("DOTENV_PATH", ".env")
    load_dotenv(dotenv_path=dotenv_path)

    # Backfill env vars if .env exists but load_dotenv didn't populate (Windows BOM / encoding edge cases)
    if (not os.environ.get("OPENAI_API_KEY")) and os.path.isfile(dotenv_path):
        with open(dotenv_path, "r", encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key == "OPENAI_API_KEY" and value:
                    os.environ["OPENAI_API_KEY"] = value
                elif key == "OPENAI_MODEL" and value:
                    os.environ.setdefault("OPENAI_MODEL", value)

    if not os.environ.get("OPENAI_API_KEY"):
        raise SystemExit("OPENAI_API_KEY not set. Add it to .env or set the environment variable.")


def _schema() -> Dict[str, Any]:
    # Minimal output + counts (no why/confidence)
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "industry": {"type": "string"},
            "equipment_type": {"type": "string"},
            "in_scope_count": {"type": "integer", "minimum": 0},
            "out_of_scope_count": {"type": "integer", "minimum": 0},
            "total_tags_count": {"type": "integer", "minimum": 0},
            "failure_modes_in_scope": {"type": "array", "items": {"type": "string"}},
            "failure_modes_out_of_scope": {"type": "array", "items": {"type": "string"}},
            "important_parameter_tags": {"type": "array", "items": {"type": "string"}},
        },
        "required": [
            "industry",
            "equipment_type",
            "in_scope_count",
            "out_of_scope_count",
            "total_tags_count",
            "failure_modes_in_scope",
            "failure_modes_out_of_scope",
            "important_parameter_tags",
        ],
    }


def _load_prompt(path: str) -> str:
    if not path or not os.path.isfile(path):
        raise SystemExit("Prompt file not found. Provide --prompt prompt.txt")
    with open(path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as exc:
            raise SystemExit("Prompt file must be JSON with key 'llm_equip_analyse'.") from exc
    prompt = data.get("llm_equip_analyse") if isinstance(data, dict) else None
    if not prompt or not isinstance(prompt, str) or not prompt.strip():
        raise SystemExit("Prompt key 'llm_equip_analyse' missing or empty in prompt.txt")
    return prompt.strip()


def _pick_column(cols: List[str], candidates: List[str]) -> Optional[str]:
    cols_norm = {c.strip().lower(): c for c in cols}
    for cand in candidates:
        key = cand.strip().lower()
        if key in cols_norm:
            return cols_norm[key]
    return None


def _load_kiln_mapping(
    excel_path: str,
    equipment_type: str,
    sheet: Optional[str] = None,
    max_cell_chars: int = 400,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Mapping file loader for Kiln_mapping.xlsx style.
    Expected columns like: S.No, KILN, Metadata, UOM (case-insensitive).
    Returns:
      - parameters: [{tag, metadata, uom}, ...]
      - summary: mapping_summary dict
    """
    if not os.path.isfile(excel_path):
        raise SystemExit(f"Excel file not found: {excel_path}")

    try:
        import pandas as pd  # type: ignore
    except Exception as exc:
        raise SystemExit("Missing excel reader. Install pandas or openpyxl.") from exc

    xls = pd.ExcelFile(excel_path)
    sheet_name = sheet or (xls.sheet_names[0] if xls.sheet_names else None)
    if not sheet_name:
        raise SystemExit("No sheets found in the Excel file.")
    if sheet_name not in xls.sheet_names:
        raise SystemExit(f'Sheet "{sheet_name}" not found. Available: {xls.sheet_names}')

    df = pd.read_excel(xls, sheet_name=sheet_name, dtype=object)
    df = df.where(df.notna(), None)
    cols = [str(c) for c in df.columns.tolist()]

    # In your file, tag column is typically "KILN" (equipment_type upper)
    tag_col = _pick_column(cols, [equipment_type.upper(), equipment_type, "tag", "tags", "parameter", "parameters"])
    meta_col = _pick_column(cols, ["metadata", "description", "desc", "meaning"])
    uom_col = _pick_column(cols, ["uom", "unit", "units"])

    if not tag_col:
        raise SystemExit(
            f"Could not identify tag column. Looked for [{equipment_type.upper()}, {equipment_type}, tag/parameter]. "
            f"Columns present: {cols}"
        )
    if not meta_col:
        meta_col = ""
    if not uom_col:
        uom_col = ""

    parameters: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        tag = r.get(tag_col)
        if tag is None or (isinstance(tag, str) and not tag.strip()):
            continue

        md = r.get(meta_col) if meta_col else None
        uom = r.get(uom_col) if uom_col else None

        if isinstance(tag, str) and len(tag) > max_cell_chars:
            tag = tag[:max_cell_chars] + "…"
        if isinstance(md, str) and len(md) > max_cell_chars:
            md = md[:max_cell_chars] + "…"
        if isinstance(uom, str) and len(uom) > max_cell_chars:
            uom = uom[:max_cell_chars] + "…"

        parameters.append(
            {
                "tag": str(tag).strip() if isinstance(tag, str) else str(tag),
                "metadata": (str(md).strip() if isinstance(md, str) else (md or "")),
                "uom": (str(uom).strip() if isinstance(uom, str) else (uom or "")),
            }
        )

    summary = {
        "file_name": os.path.basename(excel_path),
        "sheet": sheet_name,
        "total_rows": int(df.shape[0]),
        "total_columns": int(df.shape[1]),
        "columns_seen": cols,
        "tag_column": tag_col,
        "metadata_column": meta_col or "",
        "uom_column": uom_col or "",
        "tags_count": len(parameters),
    }
    return parameters, summary


def run_equip_analyse(
    excel: str = "Kiln_mapping.xlsx",
    sheet: Optional[str] = None,
    industry: str = "Cement Industry",
    equipment_type: str = "Kiln",
    prompt_path: str = "",
    output_dir: str = "outputs",
    model: Optional[str] = None,
) -> Tuple[str, str]:
    _ensure_env()

    logger.info("=" * 60)
    logger.info("EQUIPMENT ANALYSIS - START")
    logger.info("=" * 60)

    parameters, mapping_summary = _load_kiln_mapping(
        excel_path=excel,
        equipment_type=equipment_type,
        sheet=sheet,
    )

    prompt = _load_prompt(prompt_path)

    model_name = model or os.environ.get("OPENAI_MODEL", "gpt-5")

    payload = {
        "industry": industry,
        "equipment_type": equipment_type,
        "mapping_summary": mapping_summary,
        "parameters": parameters,
    }

    # Log LLM Input
    logger.info("-" * 40)
    logger.info("LLM INPUT:")
    logger.info(f"  Model: {model_name}")
    logger.info(f"  Industry: {industry}")
    logger.info(f"  Equipment Type: {equipment_type}")
    logger.info(f"  Excel File: {excel}")
    logger.info(f"  Total Tags: {mapping_summary.get('tags_count', 0)}")
    logger.info(f"  System Prompt: {prompt[:100]}..." if len(prompt) > 100 else f"  System Prompt: {prompt}")
    logger.debug(f"  Full Payload:\n{json.dumps(payload, indent=2, ensure_ascii=False)}")
    logger.info("-" * 40)

    client = OpenAI()

    try:
        logger.info("Sending request to OpenAI API...")
        response = client.responses.create(
            model=model_name,
            input=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": "Kiln mapping parameters JSON:\n" + json.dumps(payload, ensure_ascii=False)},
            ],
            text={
                "format": {
                    "type": "json_schema",
                    "name": "kiln_mapping_failure_mode_inference_minimal",
                    "schema": _schema(),
                    "strict": True,
                }
            },
        )

        output_text = response.output_text
        parsed = json.loads(output_text)
        parsed["total_tags_count"] = mapping_summary.get("tags_count", 0)
        output_text = json.dumps(parsed, ensure_ascii=False, indent=2)

        # Log LLM Output - SUCCESS
        logger.info("-" * 40)
        logger.info("LLM OUTPUT (SUCCESS):")
        logger.info(f"  In-Scope Failure Modes: {len(parsed.get('failure_modes_in_scope', []))}")
        logger.info(f"  Out-of-Scope Failure Modes: {len(parsed.get('failure_modes_out_of_scope', []))}")
        logger.info(f"  Important Tags: {len(parsed.get('important_parameter_tags', []))}")
        logger.info(f"  Full Response:\n{output_text}")
        logger.info("-" * 40)

    except Exception as e:
        # Log LLM Output - FAILURE
        logger.error("-" * 40)
        logger.error("LLM OUTPUT (FAILED):")
        logger.error(f"  Error Type: {type(e).__name__}")
        logger.error(f"  Error Message: {str(e)}")
        logger.error("-" * 40)
        raise

    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    out_path = os.path.join(output_dir, f"kiln_mapping_llm_output_{timestamp}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(output_text)
        f.write("\n")

    logger.info(f"Output saved to: {out_path}")
    logger.info("=" * 60)
    logger.info("EQUIPMENT ANALYSIS - END")
    logger.info("=" * 60)

    return out_path, output_text


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Send Kiln mapping Excel to OpenAI and return JSON: in-scope/out-of-scope failure modes + important tags."
    )
    parser.add_argument("--excel", default="Kiln_mapping.xlsx", help="Path to Kiln mapping Excel (default: Kiln_mapping.xlsx)")
    parser.add_argument("--sheet", default=None, help="Excel sheet name (default: first sheet)")
    parser.add_argument("--industry", default="Cement Industry", help='Industry label to analyze (default: "Cement Industry")')
    parser.add_argument("--equipment-type", default="Kiln", help='Equipment Type to analyze (default: "Kiln")')
    parser.add_argument("--prompt", default="prompt.txt", help="Path to prompt file (default: prompt.txt)")
    parser.add_argument("--output-dir", default="outputs", help="Directory to save JSON output files (default: outputs)")
    parser.add_argument("--model", default=None, help="OpenAI model (default: env OPENAI_MODEL or gpt-5)")
    args = parser.parse_args()

    out_path, output_text = run_equip_analyse(
        excel=args.excel,
        sheet=args.sheet,
        industry=args.industry,
        equipment_type=args.equipment_type,
        prompt_path=args.prompt,
        output_dir=args.output_dir,
        model=args.model,
    )

    print(output_text)
    print(f"Saved output to: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
