"""
Process ODR Module - Main orchestration for ODR processing
Responsibilities:
1. Load industry configurations and rules
2. Filter data for running conditions
3. Apply rules and generate alarms
4. Format observations
5. Export and publish alerts
"""

import pandas as pd
import sys
import json
import logging
import os
from helpers import (
    ensure_datetime,
    TIME_COL,
    eval_group,
    value_at,
    collect_params,
    collect_param_specs,
    ops_map,
    parse_check_components,
    rewrite_condition_group,
    build_process_alert_details,
    build_observation_record,
    save_observations_to_files,
    calculate_derived_parameters,
    load_alert_state,
    save_alert_state,
    load_downtime_state,
    save_downtime_state,
    hampel_filter,
    hampel_filter_df,
    lowess_smooth_df
)
from constants import (
    MAPPING_SHEET,
    GENERIC_COL,
    DESC_COL,
    UNIT_COL,
    DATABASES,
    INDUSTRY_CONFIGS,
)

    
from api import fetch_parameter_data
from publish_process_alerts import publish_process_alerts
import token_generator
from image_url import image_url


def _collect_rule_checks(rule):
    checks = []

    def add_groups(groups):
        for group in groups or []:
            for check in group.get("checks", []):
                checks.append(check)

    def add_block(block):
        if not block:
            return
        # Some blocks use "checks" directly, others nest under "groups"
        for check in block.get("checks", []):
            checks.append(check)
        add_groups(block.get("groups", []))

    add_groups(rule.get("groups", []))
    add_block(rule.get("if"))
    add_block(rule.get("then"))
    add_block(rule.get("else"))

    return checks


def _apply_rules_overrides(mapped_rules, equipment_name, application, overrides_map):
    if not overrides_map:
        return mapped_rules

    equipment_override = overrides_map.get(equipment_name)
    if not equipment_override:
        return mapped_rules

    override_app = equipment_override.get("application")
    if override_app and override_app != application:
        logger.warning(
            "Rules override application mismatch for %s: expected %s, got %s",
            equipment_name, override_app, application
        )
        return mapped_rules

    overrides = equipment_override.get("overrides", [])
    if not overrides:
        return mapped_rules

    rules_by_id = {rule.get("id"): rule for rule in mapped_rules}
    updates = 0

    for override in overrides:
        rule_id = override.get("rule_id")
        rule = rules_by_id.get(rule_id)
        if rule is None:
            logger.warning("Override rule_id %s not found for %s", rule_id, equipment_name)
            continue
        if "name" in override:
            rule["name"] = override["name"]

        for check_override in override.get("checks", []):
            param = check_override.get("param")
            op = check_override.get("op")
            if not param or not op:
                logger.warning("Override check missing param/op for rule_id %s", rule_id)
                continue

            updated = False
            for check in _collect_rule_checks(rule):
                if check.get("param") == param and check.get("op") == op:
                    if "value" in check_override:
                        check["value"] = check_override["value"]
                    if "duration_s" in check_override:
                        check["duration_s"] = check_override["duration_s"]
                    if "units" in check_override:
                        check["units"] = check_override["units"]
                    updated = True

            if not updated:
                logger.warning(
                    "Override check not matched for rule_id %s (param=%s, op=%s)",
                    rule_id, param, op
                )
            else:
                updates += 1

    if updates:
        logger.info("Applied %s rule overrides for %s", updates, equipment_name)

    return mapped_rules
# ============================================================================
# LOGGING SETUP
# ============================================================================
# Suppress print() in UAT/PROD ECS while keeping logger working
ENV_NAME = os.getenv("ENV_NAME", "local").lower()
if ENV_NAME in ["uat", "prod"]:
    # Redirect print() to devnull in UAT/PROD ECS,
    # but keep a reference to original stdout for logger
    _original_stdout = sys.stdout
    sys.stdout = open(os.devnull, 'w')
else:
    _original_stdout = sys.stdout

logger = logging.getLogger("process-odr")
if not logger.handlers:
    # Logger always uses the original stdout (not devnull)
    handler = logging.StreamHandler(_original_stdout)
    handler.setFormatter(
        logging.Formatter("[%(asctime)s] %(levelname)s %(name)s - %(message)s")
    )
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

# Track failure mode trends so they can open after summary plots.
FAILURE_TRENDS_QUEUE = {}


def _queue_failure_trends(run_key, equipment_name, trends_path):
    if not trends_path:
        return
    queue = FAILURE_TRENDS_QUEUE.setdefault(run_key, {})
    queue[equipment_name] = trends_path


def _open_failure_trends(run_key, equipment_order, open_fn):
    equipment_paths = FAILURE_TRENDS_QUEUE.pop(run_key, {})
    for equipment in equipment_order:
        trend_path = equipment_paths.get(equipment)
        if trend_path:
            open_fn(trend_path, f"{equipment} failure mode trends")
# ============================================================================
# LOAD INDUSTRY DATABASES
# ============================================================================
db_registry = {}
for db_name, file_path in DATABASES.items():
    full_path = f"files/{file_path}"
    with open(full_path, "r", encoding="utf-8") as f:
        db_registry[db_name] = json.load(f)
    logger.info(
        f"Loaded {len(db_registry[db_name])} equipments from {db_name} database."
    )


# ============================================================================
# RULE EVALUATION
# ============================================================================

def evaluate_rules(running_df, row_duration, mapped_rules, generic_to_machine):
    """
    Evaluate all rules against the running data and generate alarms.

    Args:
        running_df: DataFrame filtered to running periods only
        row_duration: Duration of each row in seconds
        mapped_rules: List of rule dictionaries
        generic_to_machine: Mapping from generic params to machine tags

    Returns:
        DataFrame with alarm records
    """
    alarms = []
    rule_active_now = {}

    if running_df.empty:
        logger.warning("No valid running periods detected — skipping rule evaluation.")
        return pd.DataFrame()

    total_rules = len(mapped_rules)
    executed_rules = 0
    skipped_rules = 0
    skipped_rules_log = []
    missing_params_overall = set()

    # Evaluate each rule
    for rule in mapped_rules:
        rule_id = rule.get("id", "?")
        # Build root group
        rule_logic = rule.get("logic", "AND")
        root = {"logic": rule_logic}
        if str(rule_logic).upper() == "IF":
            root["if"] = rule.get("if")
            root["then"] = rule.get("then")
            if "else" in rule:
                root["else"] = rule.get("else")
        elif "groups" in rule:
            root["groups"] = rule["groups"]
        elif "checks" in rule:
            root["checks"] = rule["checks"]
        else:
            continue

        # Check if all required params are available
        required_params = set(collect_params(root))
        missing_params = [p for p in required_params if p not in running_df.columns]

        if missing_params:
            skipped_rules += 1
            missing_params_overall.update(missing_params)
            skipped_rules_log.append({
                "id": rule.get("id", "?"),
                "name": rule.get("name", ""),
                "missing": missing_params,
            })
            rule_active_now[rule_id] = False
            continue

        # Evaluate rule group
        mask, chk_masks = eval_group(running_df, root)
        if mask is None:
            skipped_rules += 1
            rule_active_now[rule_id] = False
            continue

        rule_active_now[rule_id] = bool(mask.iloc[-1]) if not mask.empty else False

        executed_rules += 1

        # Generate one alarm per continuous streak after full duration
        duration_needed = max(int(c.get("duration_s", 0)) for _, c in chk_masks) if chk_masks else 0
        trigger_positions = []
        streak_seconds = 0.0
        triggered_in_streak = False
        prev_ts = None

        # Find trigger positions based on streak duration
        for i, flag in enumerate(mask.values):
            ts = mask.index[i]

            # Calculate step size using actual time delta
            if prev_ts is None:
                step = row_duration
            else:
                step = (ts - prev_ts).total_seconds()
                if not step or step != step:  # NaN or zero
                    step = row_duration
            prev_ts = ts

            if not flag:
                streak_seconds = 0.0
                triggered_in_streak = False
                continue

            streak_seconds += step

            if duration_needed <= 0:
                if not triggered_in_streak:
                    trigger_positions.append(i)
                    triggered_in_streak = True
                continue

            if not triggered_in_streak and streak_seconds >= duration_needed:
                trigger_positions.append(i)
                triggered_in_streak = True

        # Create alarm records for each trigger position
        for pos in trigger_positions:
            ts = mask.index[pos]
            # Only include checks true at this timestamp (handles OR correctly)
            active_checks = [chk for cmask, chk in chk_masks if cmask.iloc[pos]]
            if not active_checks:
                continue

            actual_vals = []
            show_vals = []
            equip_tags = []

            for chk in active_checks:
                val = value_at(running_df, ts, chk["param"])
                actual_vals.append(f"{val} {chk.get('units','')}")
                show_vals.append(str(val))
                equip_tags.append(generic_to_machine.get(chk["param"], "N/A"))

            alarms.append({
                "timestamp": ts,
                "rule_id": rule.get("id", ""),
                "rule_name": rule.get("name", ""),
                "check": " AND ".join(f"{c['param']} {c['op']} {c['value']}" for c in active_checks),
                "actual value": " , ".join(actual_vals),
                "param": " , ".join(c["param"] for c in active_checks),
                "equipment_tag": " , ".join(equip_tags),
                "show value": " , ".join(show_vals),
                "diagnostics": rule.get("diagnostics", ""),
                "recommendation": rule.get("recommendation", "")
            })

    # Create alarms DataFrame
    alarms_df = pd.DataFrame(alarms)

    # Log summary
    if missing_params_overall:
        logger.warning(f"Missing params across skipped rules: {', '.join(sorted(missing_params_overall))}")
    if skipped_rules_log:
        logger.warning("Skipped rules detail:")
        for r in skipped_rules_log:
            logger.warning(f"  Rule {r['id']} ({r['name']}): missing {', '.join(r['missing'])}")
    logger.info(f"Rule summary: total={total_rules}, executed={executed_rules}, skipped={skipped_rules}")
    
    return alarms_df, rule_active_now


# ============================================================================
# OBSERVATION PROCESSING
# ============================================================================

def process_observations(unique_obs, equipmentName, param_to_desc, param_to_unit):
    """
    Process observations and create human-readable texts.

    Args:
        unique_obs: DataFrame with unique observations
        equipmentName: Name of equipment
        param_to_desc: Dictionary mapping params to descriptions
        param_to_unit: Dictionary mapping params to units

    Returns:
        List of observation records
    """
    all_observations = []
    observation_number = 1

    for _, row in unique_obs.iterrows():
        final_observation = row.get("final_observation")
        if final_observation is None:
            final_observation = build_final_observation(
                row, param_to_desc, param_to_unit
            )

        # Build observation record
        observation_record = build_observation_record(
            observation_number, equipmentName, row, final_observation
        )
        all_observations.append(observation_record)
        observation_number += 1

    return all_observations


def build_final_observation(row, param_to_desc, param_to_unit):
    """
    Build a human-readable observation text from a row.

    Args:
        row: DataFrame row with alarm data
        param_to_desc: Dictionary mapping params to descriptions
        param_to_unit: Dictionary mapping params to units

    Returns:
        Observation string
    """
    group_conditions, _, group_show_vals, _ = parse_check_components(
        row["check"], row["param"], row["show value"], row["equipment_tag"]
    )

    rewritten_conditions = []
    for group_idx, cond_group in enumerate(group_conditions):
        group_rewritten = rewrite_condition_group(
            cond_group,
            group_show_vals[group_idx],
            param_to_desc,
            param_to_unit
        )
        rewritten_conditions.extend(group_rewritten)

    return " and ".join(rewritten_conditions)


def publish_observations(unique_obs, equipmentName, param_to_desc, param_to_unit, plantId, plantName):
    """
    Publish observations to the API.

    Args:
        unique_obs: DataFrame with unique observations
        equipmentName: Name of equipment
        param_to_desc: Dictionary mapping params to descriptions
        param_to_unit: Dictionary mapping params to units
        plantId: Plant ID
        plantName: Plant name
    """
    logger.info("Starting to publish alerts to API...")

    for _, row in unique_obs.iterrows():
        final_observation = row.get("final_observation")
        if final_observation is None:
            final_observation = build_final_observation(
                row, param_to_desc, param_to_unit
            )

        # Build process alert details
        processAlertDetails = build_process_alert_details(
            row, final_observation, equipmentName, image_url
        )

        # Publish to API
        try:
            status_code = publish_process_alerts(
                processAlertDetails,
                processAlertDetails["alertStartTime"],
                access_token=token_generator.ACCESS_TOKEN,
                plant_id=plantId,
                plant_name=plantName,
            )

            if status_code == 401:
                logger.warning(
                    f"Received 401 for alert '{row['rule_name']}' - "
                    "Refreshing token and retrying"
                )
                token_generator.refresh_token()
                logger.info("Token refreshed successfully, retrying publish")

                status_code = publish_process_alerts(
                    processAlertDetails,
                    processAlertDetails["alertStartTime"],
                    access_token=token_generator.ACCESS_TOKEN,
                    plant_id=plantId,
                    plant_name=plantName,
                )

                if status_code == 200:
                    logger.info(
                        f"Alert '{row['rule_name']}' published successfully "
                        "after token refresh"
                    )
                else:
                    logger.error(
                        f"Alert '{row['rule_name']}' failed even after "
                        f"token refresh. Status: {status_code}"
                    )

            elif status_code == 200:
                logger.info(f"Alert '{row['rule_name']}' published successfully")

        except Exception as publish_error:
            logger.error(
                f"Exception while publishing alert '{row['rule_name']}': "
                f"{publish_error}"
            )
            raise Exception(f"Exception while publishing: {publish_error}")


# ============================================================================
# MAIN PROCESSING FUNCTION
# ============================================================================

def publish_process_odr(start, end, equipmentName, plantId, plantName):
    """
    Main function to process ODR for a given equipment and time range.

    Args:
        start: Start timestamp
        end: End timestamp
        equipmentName: Name of the equipment
        plantId: Plant ID
        plantName: Plant name
    """
    try:
        # ---- DETERMINE INDUSTRY ----
        industry_type = None
        equipment_config = None

        for ind_name, config in INDUSTRY_CONFIGS.items():
            if equipmentName in db_registry[ind_name]:
                industry_type = ind_name
                equipment_config = config
                break

        if not equipment_config:
            raise ValueError(
                f"Equipment '{equipmentName}' not found in any database: "
                f"{list(INDUSTRY_CONFIGS.keys())}"
            )

        # Extract equipment details
        equipment_data = db_registry[industry_type][equipmentName]
        application = equipment_data["application"]

        logger.info(
            f"Equipment: {equipmentName}, Industry: {industry_type.title()}, "
            f"Application: {application}"
        )

        # ---- LOAD MAPPING FILE ----
        mapping_file = equipment_config["metadata_mapping"].get(application)
        if not mapping_file:
            logger.warning(
                f"No mapping file defined for application '{application}' "
                f"in {industry_type}. Skipping ODR processing."
            )
            return

        mapping_file_path = f"files/{equipment_config['base_path']}/{mapping_file}"
        map_df = pd.read_excel(mapping_file_path, sheet_name=MAPPING_SHEET)

        # ---- LOAD ODR RULES ----
        rules_file = equipment_config["rules_mapping"].get(application)
        if not rules_file:
            logger.warning(
                f"No rules file defined for application '{application}' "
                f"in {industry_type}. Skipping ODR processing."
            )
            return

        rules_path = f"files/{equipment_config['base_path']}/{rules_file}"
        with open(rules_path, "r", encoding="utf-8-sig") as f:
            mapped_rules = json.load(f)

        logger.info(
            f"Loaded {len(mapped_rules)} observation rules for {equipmentName} ({application})"
        )
        rules_overrides_file = equipment_config.get("rules_overrides_file")
        overrides_map = {}
        if rules_overrides_file:
            overrides_path = f'files/{equipment_config["base_path"]}/{rules_overrides_file}'
            if os.path.exists(overrides_path):
                with open(overrides_path, "r", encoding="utf-8-sig") as f:
                    overrides_map = json.load(f)
                logger.info(
                    "Loaded %s rule override entries from %s",
                    len(overrides_map),
                    rules_overrides_file,
                )
        mapped_rules = _apply_rules_overrides(
            mapped_rules, equipmentName, application, overrides_map
        )

        # ---- COLLECT REQUIRED PARAMETERS ----
        required_param_specs = set()
        for rule in mapped_rules:
            required_param_specs.update(collect_param_specs(rule, equipmentName))

        required_generic = {
            param_name for param_name, _ in required_param_specs
        }
        param_to_equipment = {
            param_name: equipment_name
            for param_name, equipment_name in required_param_specs
        }

        # ---- LOAD MAPPINGS FOR OTHER EQUIPMENTS (IF ANY) ----
        mapping_by_equipment = {equipmentName: map_df}
        other_equipments = {
            equipment_name for _, equipment_name in required_param_specs
            if equipment_name != equipmentName
        }

        for other_equipment in other_equipments:
            other_equipment_data = db_registry[industry_type].get(other_equipment)
            if not other_equipment_data:
                logger.warning(
                    f"Equipment '{other_equipment}' not found in registry."
                )
                continue

            other_application = other_equipment_data.get("application")
            other_mapping_file = equipment_config["metadata_mapping"].get(other_application)
            if not other_mapping_file:
                logger.warning(
                    f"No mapping file defined for application '{other_application}' "
                    f"(equipment '{other_equipment}')."
                )
                continue

            other_mapping_path = f"files/{equipment_config['base_path']}/{other_mapping_file}"
            other_map_df = pd.read_excel(other_mapping_path, sheet_name=MAPPING_SHEET)
            mapping_by_equipment[other_equipment] = other_map_df

        # ---- LOAD DERIVED PARAMETERS AND ADD TO REQUIRED PARAMS ----
        derived_params_file = equipment_config.get("derived_params_file")
        derived_params_list = []
        all_derived_params = []

        if derived_params_file:
            derived_params_path = f"files/{equipment_config['base_path']}/{derived_params_file}"
            try:
                with open(derived_params_path, "r", encoding="utf-8-sig") as f:
                    derived_config = json.load(f)
                    all_derived_params = derived_config.get("derived_parameters", [])

                    if all_derived_params:
                        # Filter derived parameters for this specific application
                        app_derived_params = [
                            p for p in all_derived_params
                            if p.get("application") == application
                        ]

                        # Collect all parameters from enabled derived params
                        enabled_derived_count = 0
                        import re
                        ignore_tokens = ['SQRT', 'ABS', 'LOG', 'LOG10', 'EXP', 'SIN', 'COS', 'TAN', 'POWER', 'ROUND', 'FLOOR', 'CEIL', 'NP', 'PD']
                        for param_def in app_derived_params:
                            if param_def.get("enabled", False):
                                formula = param_def.get("formula", "")
                                # Extract parameter names from formula
                                # Add all uppercase words that might be parameter names
                                potential_params = re.findall(r'[A-Z_][A-Z0-9_]*', formula)
                                for param in potential_params:
                                    # Skip function names
                                    if param not in ignore_tokens:
                                        required_generic.add(param)

                                name = str(param_def.get("name", "")).strip()
                                if name:
                                    required_generic.add(name)

                                enabled_derived_count += 1

                        derived_params_list = app_derived_params

                        logger.info(
                            f"Loaded {len(app_derived_params)} derived parameter definitions "
                            f"for {application} ({enabled_derived_count} enabled) from {derived_params_file}"
                        )
                    else:
                        logger.info(f"No derived parameters defined in {derived_params_file}")
            except FileNotFoundError:
                logger.info(f"Derived parameters file not found: {derived_params_path} (optional)")
            except Exception as e:
                logger.warning(f"Error loading derived parameters config: {e}")
        else:
            logger.info("No derived parameters configuration file defined for this industry")

        # ---- LOAD RUNNING CONDITION TAG (ENSURE IT'S FETCHED) ----
        run_time_tags_file = equipment_config.get("run_time_tags_file")
        if run_time_tags_file:
            run_time_tags_path = f"files/{equipment_config['base_path']}/{run_time_tags_file}"
            try:
                with open(run_time_tags_path, "r", encoding="utf-8") as f:
                    run_time_config = json.load(f)
                    run_time_tags_list = run_time_config.get("run_time_tags", [])

                    for tag_config in run_time_tags_list:
                        if tag_config.get("application") == application:
                            run_time_tag = tag_config.get("tag")
                            if run_time_tag:
                                required_generic.add(run_time_tag)
                                param_to_equipment.setdefault(run_time_tag, equipmentName)
                                logger.info(
                                    "Loaded run-time tag for %s from %s: %s",
                                    application,
                                    run_time_tags_file,
                                    run_time_tag,
                                )
                            break
            except FileNotFoundError:
                logger.info(f"Run-time tags file not found: {run_time_tags_path} (optional)")
            except Exception as e:
                logger.warning(f"Error loading run-time tags config: {e}")

        # Build generic to machine mapping
        generic_to_machine = {}
        for _, row in map_df.iterrows():
            g = row.get(GENERIC_COL)
            mtag = row.get(equipmentName)

            if pd.isna(g) or pd.isna(mtag):
                continue

            g = str(g).strip()
            mtag = str(mtag).strip()
            if g and mtag:
                generic_to_machine[g] = mtag

        # ---- EXTEND MAPPING FOR CROSS-EQUIPMENT PARAMS ----
        for param_name, equipment_name in required_param_specs:
            if equipment_name == equipmentName:
                continue

            other_map_df = mapping_by_equipment.get(equipment_name)
            if other_map_df is None:
                continue

            param_rows = other_map_df.loc[
                other_map_df[GENERIC_COL].astype(str).str.strip() == param_name,
                equipment_name
            ]
            if not param_rows.empty:
                generic_to_machine[param_name] = str(param_rows.iloc[0]).strip()

        # Identity-map any required params not present in mapping
        for param_name in required_generic:
            if param_name not in generic_to_machine:
                generic_to_machine[param_name] = param_name

        # Force derived params to map to themselves for display/equipment_tag
        for param_def in derived_params_list:
            name = str(param_def.get("name", "")).strip()
            if name:
                generic_to_machine[name] = name

        # Build display mapping that includes derived params -> base machine tags
        display_mapping = dict(generic_to_machine)
        if derived_params_list:
            import re
            ignore_tokens = {
                "SQRT", "ABS", "LOG", "LOG10", "EXP", "SIN", "COS", "TAN",
                "POWER", "ROUND", "FLOOR", "CEIL", "NP", "PD"
            }
            for param_def in derived_params_list:
                if not param_def.get("enabled", False):
                    continue
                name = str(param_def.get("name", "")).strip()
                formula = param_def.get("formula", "")
                if not name or not formula:
                    continue

                base_params = re.findall(r"[A-Z_][A-Z0-9_]*", formula)
                tags = []
                for p in base_params:
                    if p in ignore_tokens:
                        continue
                    tag = generic_to_machine.get(p)
                    if tag and tag not in tags:
                        tags.append(tag)

                if name:
                    display_mapping[name] = name

        # Filter to only rule-needed parameters
        filtered_mapping = {
            g: m for g, m in generic_to_machine.items() if g in required_generic
        }

        # Exclude derived params from API fetch (computed locally)
        derived_param_names = {
            str(p.get("name", "")).strip()
            for p in derived_params_list
            if p.get("enabled", False) and str(p.get("name", "")).strip()
        }
        filtered_mapping_for_fetch = {
            param_name: {
                "equipment": param_to_equipment.get(param_name, equipmentName),
                "tag": machine_tag
            }
            for param_name, machine_tag in filtered_mapping.items()
            if param_name not in derived_param_names
        }

        missing = required_generic - set(filtered_mapping.keys())
        if missing:
            logger.warning(
                f"Warning: comparative tags in rules not found in mapping: {missing}"
            )

        logger.info(
            f"Mapping loaded for {equipmentName}: "
            f"{len(generic_to_machine)} tags mapped, "
            f"Filtered to {len(filtered_mapping)} rule-needed tags for API fetch"
        )

        # Build parameter description and unit mappings
        param_to_desc = dict(
            zip(
                map_df[GENERIC_COL].astype(str).str.strip(),
                map_df[DESC_COL].astype(str).str.strip(),
            )
        )
        param_to_unit = dict(
            zip(
                map_df[GENERIC_COL].astype(str).str.strip(),
                map_df[UNIT_COL].astype(str).str.strip(),
            )
        )
        # Merge derived param descriptions/units so observations use them
        for p in derived_params_list:
            if not p.get("enabled", False):
                continue
            name = str(p.get("name", "")).strip()
            if not name:
                continue
            desc = str(p.get("description", "")).strip()
            unit = str(p.get("unit", "")).strip()
            if desc:
                param_to_desc[name] = desc
            if unit and unit.lower() != "nan":
                param_to_unit[name] = unit

        # ---- SETUP API CONFIGURATION ----
        api_url = equipment_config["url"]
        api_key = os.getenv(
            equipment_config["api_key_env"], equipment_config["default_api_key"]
        )
        api_headers = {"X-API-key": api_key, "Content-Type": "application/json"}

        logger.info(f"API Endpoint: {api_url}")

        # ---- FETCH DATA ----
        start_utc_ms = int(start)
        end_utc_ms = int(end)

        master_df = fetch_parameter_data(
            equipmentName,
            filtered_mapping_for_fetch,
            required_generic,
            start_utc_ms,
            end_utc_ms,
            api_url,
            api_headers,
        )

        if master_df.empty:
            logger.info(f"No data retrieved, cannot evaluate rules for {equipmentName}")
            return

        # ---- PREPARE DATAFRAME ----
        df_raw = master_df.copy()
        df_raw = ensure_datetime(df_raw, "timestamp")
        # ---- CLEAN CYCLIC TREND TAGS (OPTIONAL) ----
        cyclic_trend_tags_file = equipment_config.get("cyclic_trend_tags_file")
        cyclic_trend_config = None

        if cyclic_trend_tags_file:
            cyclic_trend_path = f"files/{equipment_config['base_path']}/{cyclic_trend_tags_file}"
            try:
                with open(cyclic_trend_path, "r", encoding="utf-8") as f:
                    cyclic_trend_data = json.load(f)
                    cyclic_trend_list = cyclic_trend_data.get("cyclic_trend_tags", [])

                    for tag_config in cyclic_trend_list:
                        if tag_config.get("application") == application:
                            cyclic_trend_config = tag_config
                            break
            except FileNotFoundError:
                logger.info(
                    f"Cyclic trend tags file not found: {cyclic_trend_path} (optional)"
                )
            except Exception as e:
                logger.warning(f"Error loading cyclic trend tags config: {e}")

        if cyclic_trend_config:
            tags = set(cyclic_trend_config.get("tags", []))
            window_minutes = float(cyclic_trend_config.get("window_minutes", 7))
            n_sigmas = float(cyclic_trend_config.get("n_sigmas", 3))
            cyclic_before_after = []

            logger.info(
                "Applying cyclic trend cleaning (%s min window) to %s tags.",
                window_minutes,
                len(tags),
            )

            for tag in tags:
                tag_mask = df_raw["param"] == tag
                if not tag_mask.any():
                    continue
                series = df_raw.loc[tag_mask].set_index("timestamp")["value"].sort_index()
                if len(series) < 3:
                    continue
                deltas = series.index.to_series().diff().dt.total_seconds()
                median_delta = deltas[deltas > 0].median()
                if median_delta is None or pd.isna(median_delta) or median_delta <= 0:
                    points_per_min = 1
                else:
                    points_per_min = max(1, int(round(60.0 / median_delta)))
                window_size = max(3, int(round(window_minutes * points_per_min)))
                before = series.copy()
                cleaned = hampel_filter(
                    series,
                    window_size=window_size,
                    n_sigmas=n_sigmas,
                    replace_with_nan=False,
                )
                df_raw.loc[tag_mask, "value"] = cleaned.to_numpy()
                cyclic_before_after.append(
                    pd.DataFrame(
                        {
                            "timestamp": before.index,
                            "param": tag,
                            "before": before.to_numpy(),
                            "after": cleaned.to_numpy(),
                        }
                    )
                )

            if cyclic_before_after:
                output_folder = "alerts_generated"
                os.makedirs(output_folder, exist_ok=True)
                timestamp_now = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
                cyclic_filename = f"cyclic_trend_before_after_{equipmentName}_{timestamp_now}.csv"
                cyclic_filepath = os.path.join(output_folder, cyclic_filename)
                pd.concat(cyclic_before_after, ignore_index=True).to_csv(
                    cyclic_filepath, index=False
                )

        df = df_raw.pivot_table(
            index="timestamp", columns="param", values="value"
        ).reset_index()
        df = ensure_datetime(df, TIME_COL)
        df = df.set_index("timestamp")
        #df.to_csv("debug_raw_dataframe.csv")  # Debugging output
        agg_min = 1
        df = df.resample(f"{agg_min}min").mean()
        # df = hampel_filter_df(df, window_size=7, n_sigmas=3)
        #df = lowess_smooth_df(df, window_minutes=5)
        row_duration = agg_min * 60

        # ---- CALCULATE DERIVED PARAMETERS ----
        if derived_params_list:
            #logger.info(f"Calculating {len(derived_params_list)} derived parameters")
            df = calculate_derived_parameters(df, derived_params_list, logger)

       

        # ---- LOAD RUNNING CONDITION TAGS ----
        run_time_tags_file = equipment_config.get("run_time_tags_file")
        RUN_TAGS = None

        run_tag_config = None
        shutdown_window_s = 0
        transition_window_s = 0

        if run_time_tags_file:
            run_time_tags_path = f"files/{equipment_config['base_path']}/{run_time_tags_file}"
            try:
                with open(run_time_tags_path, "r", encoding="utf-8") as f:
                    run_time_config = json.load(f)
                    run_time_tags_list = run_time_config.get("run_time_tags", [])

                    # Prefer equipment-specific config; fallback to application-level config
                    for tag_config in run_time_tags_list:
                        if tag_config.get("equipment") == equipmentName:
                            run_tag_config = tag_config
                            break

                    if run_tag_config is None:
                        for tag_config in run_time_tags_list:
                            if tag_config.get("application") == application:
                                run_tag_config = tag_config
                                break

                    if run_tag_config is not None:
                        RUN_TAGS = [
                            run_tag_config.get("tag"),
                            run_tag_config.get("threshold"),
                            run_tag_config.get("op", ">"),
                        ]
                        shutdown_window_h = run_tag_config.get("shutdown_window_h")
                        transition_window_h = run_tag_config.get("transition_window_h")
                        if shutdown_window_h is not None:
                            shutdown_window_s = int(float(shutdown_window_h) * 3600)
                        if transition_window_h is not None:
                            transition_window_s = int(float(transition_window_h) * 3600)
                    else:
                        logger.info(f"No run-time tags defined for {application} in {run_time_tags_file}")
            except FileNotFoundError:
                logger.info(f"Run-time tags file not found: {run_time_tags_path} (optional)")
            except Exception as e:
                logger.warning(f"Error loading run-time tags config: {e}")
        else:
            logger.info("No run-time tags file defined for this industry")

        # ---- DEFINE RUNNING CONDITION ----

        unit_suffix = ""
        if RUN_TAGS is None:
            logger.info(
                f"No run-hour tag defined for {application} — "
                "alerts cannot be generated"
            )
            df["RUNNING_FLAG"] = False
        else:
            run_col = None
            if RUN_TAGS[0] in df.columns:
                run_col = RUN_TAGS[0]

            if run_col is None:
                logger.info(
                    "Run-hour tag not found in dataframe — "
                    "alerts cannot be generated"
                )
                df["RUNNING_FLAG"] = False
            else:
                run_op = RUN_TAGS[2]
                op_fn = ops_map.get(run_op, ops_map[">"])
                df[run_col] = df[run_col].fillna(0)
                base_running = op_fn(df[run_col].fillna(0), RUN_TAGS[1])
                run_unit = str(param_to_unit.get(run_col, "")).strip()
                unit_suffix = f" {run_unit}" if run_unit else ""
                if shutdown_window_s > 0 and transition_window_s > 0:
                    running_ok = []
                    prev_ts = None
                    downtime_state = load_downtime_state()
                    equip_state = downtime_state.get(equipmentName, {})
                    state = equip_state.get("state", "RUNNING")
                    last_state = state
                    downtime = float(equip_state.get("downtime_s", 0.0) or 0.0)
                    start_time = equip_state.get("start_time")
                    if start_time:
                        start_time = pd.to_datetime(start_time, errors="coerce")
                        if pd.isna(start_time):
                            start_time = None

                    for ts, is_running in zip(df.index, base_running):
                        if prev_ts is None:
                            step = row_duration
                        else:
                            step = (ts - prev_ts).total_seconds()
                            if not step or step != step:
                                step = row_duration
                        prev_ts = ts

                        if is_running:
                            if state == "SHUTDOWN":
                                state = "STARTING"
                                start_time = ts
                            if state == "STARTING":
                                if start_time is None:
                                    start_time = ts
                                if (ts - start_time).total_seconds() >= transition_window_s:
                                    state = "RUNNING"
                            downtime = 0.0
                        else:
                            downtime += step
                            if downtime >= shutdown_window_s:
                                state = "SHUTDOWN"
                                start_time = None

                        if state != last_state:
                            logger.info(
                                "RUN_STATE change for %s at %s: %s -> %s",
                                equipmentName,
                                ts.isoformat(),
                                last_state,
                                state,
                            )
                            last_state = state

                        running_ok.append(bool(is_running and state == "RUNNING"))

                    df["RUNNING_FLAG"] = running_ok
                    equip_state["state"] = state
                    equip_state["downtime_s"] = float(downtime)
                    equip_state["start_time"] = start_time.isoformat() if start_time else None
                    downtime_state[equipmentName] = equip_state
                    save_downtime_state(downtime_state)
                    logger.info(
                        f"Using run-hour tag: {run_col} | "
                        f"Shutdown window={shutdown_window_s}s, transition window={transition_window_s}s."
                    )
                else:
                    df["RUNNING_FLAG"] = base_running
                    logger.info(
                        f"Using run-hour tag: {run_col} | "
                        f"Applying running filter ({run_op}{RUN_TAGS[1]}{unit_suffix})."
                    )


        running_df = df.copy()
        # ---- SAVE DATAFRAME TO CSV ----
        output_folder = "alerts_generated"
        os.makedirs(output_folder, exist_ok=True)
        timestamp_now = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        df_filename = f"alerts_dataframe_{equipmentName}_{timestamp_now}.csv"
        df_filepath = os.path.join(output_folder, df_filename)
        csv_df = running_df.copy()
        csv_df.index = csv_df.index + pd.Timedelta(hours=5, minutes=30)
        csv_df.to_csv(df_filepath)
        #logger.info(f"DataFrame saved to {df_filepath}")

        # Filter to running periods
        running_df = df[df["RUNNING_FLAG"]].copy()

        if running_df.empty:
            logger.info(
                f"No data points where {equipmentName} was in steady "
                "running condition — skipping rule evaluation."
            )
            return
        else:
            logger.info(
                f"{len(running_df)} data points where {equipmentName} "
                f"was running ({RUN_TAGS[2]}{RUN_TAGS[1]}{unit_suffix})."
            )

        # ---- RULE EVALUATION ----
        logger.info("Evaluating rules only within steady running periods.")

        alarms_df, rule_active_now = evaluate_rules(
            running_df, row_duration, mapped_rules, display_mapping
        )

        state = load_alert_state()
        equipment_state = state.get(equipmentName, {})

        if not alarms_df.empty and equipment_state:
            def _should_emit(rule_id):
                return not bool(equipment_state.get(str(rule_id), False))

            alarms_df = alarms_df[alarms_df["rule_id"].apply(_should_emit)]
            print(len(alarms_df), "alarms after filtering with previous state")

        for rule_id, active in rule_active_now.items():
            equipment_state[str(rule_id)] = bool(active)

        state[equipmentName] = equipment_state
        save_alert_state(state)

        # ---- PREPARE UNIQUE OBSERVATIONS ----
        if alarms_df.empty:
            logger.info("No alarms generated after state filtering.")
            unique_obs = alarms_df
        else:
            unique_obs = alarms_df[
                [
                    "check",
                    "rule_name",
                    "diagnostics",
                    "recommendation",
                    "param",
                    "actual value",
                    "show value",
                    "equipment_tag",
                    "timestamp",
                ]
            ].drop_duplicates()
            print(len(alarms_df), "total alarms generated, after state filtering")
            print(len(unique_obs), "unique observations identified")

            unique_obs["final_observation"] = unique_obs.apply(
                lambda row: build_final_observation(row, param_to_desc, param_to_unit),
                axis=1,
            )

        logger.info(f"Alarms generated: {len(alarms_df)}")

        
        # ---- PROCESS AND SAVE OBSERVATIONS ----
        all_observations = process_observations(
            unique_obs, equipmentName, param_to_desc, param_to_unit
        )

        # Save observations to files
        if all_observations:
            csv_filename, json_filename, failure_counts_filename, failure_mode_counts = save_observations_to_files(
                all_observations, equipmentName, logger
            )

            # Convert epoch timestamps to readable format
            start_readable = pd.to_datetime(start_utc_ms, unit='ms').strftime('%d-%m-%Y %H:%M:%S')
            end_readable = pd.to_datetime(end_utc_ms, unit='ms').strftime('%d-%m-%Y %H:%M:%S')

            print("\n" + "="*100)
            print(f"Total observations exported: {len(all_observations)} | {equipmentName} | {start_readable} to {end_readable}")
            print("="*100 + "\n")

            # Display all failure modes counts
            print(failure_mode_counts.to_string(index=False))
            print("-"*100 + "\n")

        # ---- SETUP PLOTTING (IF AVAILABLE) ----
        plotting_available = False
        should_generate_summary = False
        generate_plots_from_process_data = None
        generate_summary_plots_for_period = None

        # Set this to True to always generate summary plots (for single equipment testing)
        # Set to False to only generate when all equipment complete (multi-equipment mode)
        FORCE_SUMMARY_PLOTS = False

        try:
            from process_alerts_plotting import (
                generate_plots_from_process_data,
                generate_summary_plots_for_period,
                update_summary_tracker,
                DEFAULT_EQUIPMENT_ORDER,
                _open_in_browser,
            )

            plotting_available = True

            if FORCE_SUMMARY_PLOTS:
                # Always generate summary plots (useful for single equipment testing)
                should_generate_summary = True
                logger.info("Summary plots forced enabled (FORCE_SUMMARY_PLOTS=True)")
            else:
                # Only generate when all equipment complete (multi-equipment coordination)
                should_generate_summary = update_summary_tracker(
                    start_time_ms=start,
                    end_time_ms=end,
                    equipment_name=equipmentName,
                )
        except (ImportError, ModuleNotFoundError) as exc:
            logger.warning(
                "Plotting not available (%s). Skipping plot generation.", exc
            )
        except Exception as plot_error:
            logger.error(f"Error preparing plots: {plot_error}")

        if alarms_df.empty:
            logger.info("No alarms to publish")
            if plotting_available and should_generate_summary:
                summary_paths = generate_summary_plots_for_period(
                    start_time_ms=start,
                    end_time_ms=end,
                    output_dir="plots",
                    open_in_browser=True,
                    tz_offset_hours=5.5
                )
                if summary_paths:
                    _open_failure_trends(
                        run_key=(start, end),
                        equipment_order=DEFAULT_EQUIPMENT_ORDER,
                        open_fn=_open_in_browser,
                    )
                    logger.info(f"Generated summary plots: {summary_paths}")
            return


        # ---- GENERATE INTERACTIVE PLOTS (OPTIONAL) ----
        # Generate plots only in LOCAL environment
        if ENV_NAME == "local" and plotting_available:
            try:
                generate_equipment_plots = True

                if generate_equipment_plots:
                    plot_paths = generate_plots_from_process_data(
                        df=df,
                        alarms_df=alarms_df,
                        run_time_tag=RUN_TAGS[0] if RUN_TAGS else list(df.columns)[0],
                        equipment_name=equipmentName,
                        output_dir="plots",
                        additional_params=None,  # Add list of params like ["COAL_MILL_VB_BODY", "COAL_MILL_INLET_TEMP"]
                        open_in_browser=True,
                        rules=mapped_rules  # Pass rules for failure mode analysis plot
                    )
                    logger.info(f"Generated plots: {plot_paths}")

                if should_generate_summary:
                    logger.info("✓ All required equipment processed - generating summary plots")
                    summary_paths = generate_summary_plots_for_period(
                        start_time_ms=start,
                        end_time_ms=end,
                        output_dir="plots",
                        open_in_browser=False,
                        tz_offset_hours=5.5
                    )
                    if summary_paths:
                        logger.info(f"Opening {len(summary_paths)} summary plots in browser")
                        ordered_summary = [
                            "total_alerts_donut",
                            "alerts_by_equipment",
                            "failure_mode_subplots",
                            "parameter_contribution",
                            "alerts_timeline",
                        ]
                        for key in ordered_summary:
                            path = summary_paths.get(key)
                            if path and os.path.exists(path):
                                _open_in_browser(path, key.replace("_", " "))
                            elif path:
                                logger.warning(f"Plot file not found: {path}")
                        _open_failure_trends(
                            run_key=(start, end),
                            equipment_order=DEFAULT_EQUIPMENT_ORDER,
                            open_fn=_open_in_browser,
                        )
                        logger.info("✓ Summary plots opened in browser")
                    else:
                        logger.warning("Summary plot generation returned no plots")
                else:
                    logger.info("Summary plots not triggered - waiting for other equipment to complete")
            except Exception as plot_error:
                logger.error(f"Error generating plots: {plot_error}")
                logger.info("Continuing to publish alerts despite plot error...")
        else:
            logger.info(f"Skipping plot generation in {ENV_NAME.upper()} environment")

        # ---- PUBLISH ALERTS TO API ----
        # Only publish to API in UAT and PROD environments
        if ENV_NAME in ["prod"]:
            publish_observations(
                unique_obs, equipmentName, param_to_desc, param_to_unit, plantId, plantName
            )
        else:
            logger.info(f"Skipping API publish in {ENV_NAME.upper()} environment")
            return
    except Exception as process_odr_error:
        logger.exception(f"Exception: {process_odr_error} during processing odr")
