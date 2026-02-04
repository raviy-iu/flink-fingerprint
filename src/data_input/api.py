"""
API Module - Handles configuration setup and data fetching
Responsibilities:
1. Fetch parameter data from external APIs with retry logic
2. Create and return DataFrames from API responses
"""

import pandas as pd
import requests
import json
import time
import logging

# Get logger
logger = logging.getLogger("process-odr.api")


def fetch_parameter_data(equipmentName, filtered_mapping, required_generic, start, end, url, headers):
    """
    Fetch data for all parameters in the filtered_mapping.

    Args:
        equipmentName: Name of the equipment
        filtered_mapping: Dict mapping generic params to equipment tags
        required_generic: Set of required generic parameters
        start: Start timestamp
        end: End timestamp
        url: API endpoint URL
        headers: API request headers

    Returns:
        DataFrame with columns: timestamp, value, param
    """
    master_df = pd.DataFrame()
    MAX_RETRIES = 3
    BASE_DELAY = 1.0

    # Group generic params by physical tag to avoid duplicate API calls
    items = list(filtered_mapping.items())
    tag_groups = {}
    for generic_param, equipment_tag in items:
        if isinstance(equipment_tag, dict):
            tag_key = (equipment_tag["equipment"], equipment_tag["tag"])
        else:
            tag_key = (equipmentName, equipment_tag)

        group = tag_groups.get(tag_key)
        if group is None:
            tag_groups[tag_key] = {
                "equipment_tag": equipment_tag,
                "generic_params": [generic_param],
            }
        else:
            group["generic_params"].append(generic_param)

    failed_tags = set()

    # Fetch data for each physical tag
    for tag_key, group in tag_groups.items():
        attempt = 0
        success = False
        equipment_tag = group["equipment_tag"]
        generic_params = group["generic_params"]
        primary_param = generic_params[0]

        while attempt < MAX_RETRIES:
            try:
                if isinstance(equipment_tag, dict):
                    machine_name = equipment_tag["equipment"]
                    parameter_name = equipment_tag["tag"]
                else:
                    machine_name = equipmentName
                    parameter_name = equipment_tag

                payload = json.dumps({
                    "machineName": machine_name,
                    "parameterName": parameter_name,
                    "min": start,
                    "max": end
                })
                response = requests.post(url, headers=headers, data=payload)

                # Handle rate limiting
                if response.status_code == 429:
                    delay = BASE_DELAY * (2 ** attempt)
                    logger.warning(
                        f"{primary_param}: HTTP 429 rate limit; "
                        f"retrying in {delay:.1f}s (attempt {attempt+1}/{MAX_RETRIES})"
                    )
                    time.sleep(delay)
                    attempt += 1
                    continue

                # Handle non-200 responses
                if response.status_code != 200:
                    logger.error(f"{primary_param}: HTTP {response.status_code} -> {response.text}")
                    break

                resp_json = response.json()

                # Handle both single record (dict) and multiple records (list)
                if isinstance(resp_json, dict):
                    if "error" in resp_json:
                        logger.error(f"{primary_param}: API error -> {resp_json.get('error')}")
                        break
                    df = pd.DataFrame([resp_json])
                else:
                    df = pd.DataFrame(resp_json)

                if df.empty:
                    logger.warning(f"{primary_param}: empty response")
                    break

                for param_name in generic_params:
                    df_copy = df.copy()
                    df_copy["param"] = param_name
                    master_df = pd.concat([master_df, df_copy], ignore_index=True)
                if len(generic_params) > 1:
                    logger.info(
                        "Using one tag for multiple parameters: equipment=%s tag=%s params=[%s]",
                        machine_name,
                        parameter_name,
                        ", ".join(generic_params),
                    )

                success = True
                break

            except Exception as e:
                logger.error(f"{primary_param}: request failed -> {e}")
                attempt += 1
                continue

        if not success:
            failed_tags.update(generic_params)

    # Log summary
    if master_df.empty:
        logger.warning("No data fetched from API.")
    else:
        logger.info(f"Total rows fetched: {len(master_df)} for {len(tag_groups)} parameters")

    rule_tags_total = len(required_generic)
    rule_tags_mapped = len(filtered_mapping)
    rule_tags_attempted = len(tag_groups)
    tags_with_data = master_df['param'].nunique() if not master_df.empty else 0
    tags_failed = len(failed_tags)

    logger.info(
        f"Rule tags: total={rule_tags_total}, mapped={rule_tags_mapped}, "
        f"attempted={rule_tags_attempted}, with_data={tags_with_data}, failed={tags_failed}"
    )

    return master_df

