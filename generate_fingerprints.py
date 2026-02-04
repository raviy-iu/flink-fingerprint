import json
import os
import random
import uuid
from typing import Any, Dict, List, Optional, Tuple


def generate_fingerprints(
    count: int = 100,
    equip_id: str = "111",
    process_type: str = "kiln",
    tag_ids: List[str] = None,
    start_ts_ms: int = 1770037320000,
    window_ms: int = 60_000,
) -> List[Dict]:
    if tag_ids is None:
        tag_ids = ["0001", "0002", "0003", "0004", "0005"]

    fingerprints = []

    for i in range(count):
        start_ms = start_ts_ms + i * window_ms
        end_ms = start_ms + window_ms

        data = {}
        for tag in tag_ids:
            min_val = random.uniform(0, 5)
            max_val = random.uniform(90, 100)
            mean_val = random.uniform(min_val + 10, max_val - 10)
            median_val = random.uniform(min_val + 10, max_val - 10)
            std_dev = random.uniform(5, 30)

            data[tag] = {
                "min": round(min_val, 6),
                "max": round(max_val, 6),
                "median": round(median_val, 6),
                "mean": round(mean_val, 6),
                "std_dev": round(std_dev, 6),
            }

        fingerprints.append(
            {
                "fingerprint": {
                    "uuid": str(uuid.uuid4()),
                    "equip_id": equip_id,
                    "type": process_type,
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "data": data,
                }
            }
        )

    return fingerprints


def _pick_column(cols: List[str], candidates: List[str]) -> Optional[str]:
    cols_norm = {c.strip().lower(): c for c in cols}
    for cand in candidates:
        key = cand.strip().lower()
        if key in cols_norm:
            return cols_norm[key]
    return None


def _load_kiln_mapping_tags(
    excel_path: str,
    equipment_type: str = "Kiln",
    sheet: Optional[str] = None,
    max_cell_chars: int = 400,
) -> List[Dict[str, Any]]:
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
            tag = tag[:max_cell_chars] + "..."
        if isinstance(md, str) and len(md) > max_cell_chars:
            md = md[:max_cell_chars] + "..."
        if isinstance(uom, str) and len(uom) > max_cell_chars:
            uom = uom[:max_cell_chars] + "..."

        parameters.append(
            {
                "tag": str(tag).strip() if isinstance(tag, str) else str(tag),
                "metadata": (str(md).strip() if isinstance(md, str) else (md or "")),
                "uom": (str(uom).strip() if isinstance(uom, str) else (uom or "")),
            }
        )

    return parameters


def _load_tag_uuid_map(path: str) -> List[Dict[str, Any]]:
    if not path or not os.path.isfile(path):
        return []
    with open(path, "r", encoding="utf-8-sig") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    return []


def build_tag_uuid_map(
    parameters: List[Dict[str, Any]],
    existing: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    existing = existing or []
    existing_by_tag = {e.get("tag"): e for e in existing if isinstance(e, dict)}
    merged: List[Dict[str, Any]] = []
    for p in parameters:
        tag = p.get("tag")
        if not tag:
            continue
        if tag in existing_by_tag and existing_by_tag[tag].get("uuid"):
            uuid_val = existing_by_tag[tag]["uuid"]
        else:
            uuid_val = str(uuid.uuid4())
        merged.append(
            {
                "tag": tag,
                "uuid": uuid_val,
                "metadata": p.get("metadata", ""),
                "uom": p.get("uom", ""),
            }
        )
    return merged


def prepare_tag_files(
    excel_path: str = "Kiln_mapping.xlsx",
    equipment_type: str = "Kiln",
    sheet: Optional[str] = None,
    map_path: str = os.path.join("outputs", "tag_uuid_map.json"),
    metadata_path: str = os.path.join("outputs", "tag_metadata.json"),
    tag_id_mode: str = "seq",
) -> Tuple[List[str], Dict[str, Any]]:
    parameters = _load_kiln_mapping_tags(excel_path, equipment_type, sheet)
    existing_map = _load_tag_uuid_map(map_path)
    if tag_id_mode == "uuid":
        tag_map = build_tag_uuid_map(parameters, existing_map)
    else:
        existing_by_tag = {e.get("tag"): e for e in existing_map if isinstance(e, dict)}
        tag_map: List[Dict[str, Any]] = []
        for idx, p in enumerate(parameters, start=1):
            tag = p.get("tag")
            if not tag:
                continue
            if tag_id_mode == "tag":
                tag_id = str(tag)
            else:
                tag_id = str(idx).zfill(4)
            prev = existing_by_tag.get(tag, {})
            tag_map.append(
                {
                    "tag": tag,
                    "tag_id": prev.get("tag_id", tag_id),
                    "metadata": p.get("metadata", ""),
                    "uom": p.get("uom", ""),
                }
            )

    os.makedirs(os.path.dirname(map_path) or ".", exist_ok=True)
    with open(map_path, "w", encoding="utf-8") as f:
        json.dump(tag_map, f, indent=2)
        f.write("\n")

    metadata: Dict[str, Any] = {}
    for entry in tag_map:
        tag_id = entry.get("tag_id") or entry.get("uuid")
        if not tag_id:
            continue
        metadata[tag_id] = {
            "tag": entry.get("tag", ""),
            "uom": entry.get("uom", ""),
            "metaData": entry.get("metadata", ""),
        }

    os.makedirs(os.path.dirname(metadata_path) or ".", exist_ok=True)
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)
        f.write("\n")

    tag_ids = [entry.get("tag_id") or entry.get("uuid") for entry in tag_map if entry.get("tag_id") or entry.get("uuid")]
    return tag_ids, metadata


def write_fingerprints_jsonl(fingerprints: List[Dict[str, Any]], path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for fp in fingerprints:
            f.write(json.dumps(fp) + "\n")


if __name__ == "__main__":
    fps = generate_fingerprints(100)
    write_fingerprints_jsonl(fps, "data/fingerprints.jsonl")
    print("Generated 100 fingerprints -> data/fingerprints.jsonl")
