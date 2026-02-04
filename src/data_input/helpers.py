"""
Helpers Module - Supporting utility functions
Responsibilities:
1. Data transformation utilities (datetime conversion)
2. Rule evaluation helper functions
3. Observation text formatting
4. Value extraction and formatting
"""

import operator
import os
import json
import pandas as pd
import numpy as np

# Constants
COOLDOWN_SECONDS = 300
TIME_COL = "timestamp"
ALERT_STATE_FILE = "alerts_state.json"
DOWNTIME_STATE_FILE = "downtime_state.json"


def load_alert_state(path=ALERT_STATE_FILE):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_alert_state(state, path=ALERT_STATE_FILE):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def load_downtime_state(path=DOWNTIME_STATE_FILE):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_downtime_state(state, path=DOWNTIME_STATE_FILE):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

# Operator mapping for rule evaluation
ops_map = {
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne,
    "abs_gt": lambda s, thr: s.abs() > thr,
    "between": lambda s, rng: (s >= rng[0]) & (s <= rng[1]),
    "deviation": lambda s, base, pct: (
        (s < (1-pct)*base) | (s > (1+pct)*base)
    ),
    "outside": lambda s, rng: (s < rng["low"]) | (s > rng["high"])
}


# ============================================================================
# DATA TRANSFORMATION UTILITIES
# ============================================================================

def ensure_datetime(df, ts_col=TIME_COL):
    """
    Convert timestamp column to datetime and sort by timestamp.

    Args:
        df: DataFrame with timestamp column
        ts_col: Name of timestamp column

    Returns:
        DataFrame with datetime timestamp, sorted and reset index
    """
    df[ts_col] = pd.to_datetime(df[ts_col], unit="ms", errors="coerce")
    return df.sort_values(ts_col).reset_index(drop=True)


def consecutive_duration_boolseries(mask, row_duration_sec):
    """
    Calculate cumulative streak duration in seconds for a boolean array.

    Args:
        mask: Boolean array indicating true/false conditions
        row_duration_sec: Duration of each row in seconds

    Returns:
        Array of cumulative streak durations in seconds
    """
    durations = np.zeros(len(mask), dtype=float)
    streak = 0.0
    for i, val in enumerate(mask):
        if val:
            streak += row_duration_sec
        else:
            streak = 0.0
        durations[i] = streak
    return durations

# Function to reduce the noise and remove outliers 
def hampel_filter(series, window_size=7, n_sigmas=3, k=1.4826, replace_with_nan=False):
    """
    Hampel filter for outlier removal on a pandas Series.
    Replaces outliers with rolling median (or NaN if replace_with_nan=True).
    """
    s = pd.to_numeric(series, errors="coerce")
    rolling_median = s.rolling(window_size, center=True, min_periods=1).median()
    mad = (s - rolling_median).abs().rolling(window_size, center=True, min_periods=1).median()

    # Scale MAD to be consistent with std dev for normal data
    threshold = n_sigmas * k * mad

    # Avoid marking when MAD is zero/NaN
    outliers = (s - rolling_median).abs() > threshold
    outliers = outliers & threshold.notna() & (threshold > 0)

    if replace_with_nan:
        return s.mask(outliers)
    return s.mask(outliers, rolling_median)


def hampel_filter_df(df, window_size=7, n_sigmas=3, cols=None, exclude_cols=None):
    """
    Apply Hampel filter across selected numeric columns of a DataFrame.
    """
    if exclude_cols is None:
        exclude_cols = []
    if cols is None:
        cols = [c for c in df.columns if c not in exclude_cols]

    out = df.copy()
    for c in cols:
        if pd.api.types.is_numeric_dtype(out[c]):
            out[c] = hampel_filter(out[c], window_size, n_sigmas)
    return out


def lowess_smooth_df(df, window_minutes=5, cols=None, exclude_cols=None):
    """
    Apply LOESS smoothing across selected numeric columns of a DataFrame.

    Args:
        df: Input DataFrame indexed by timestamp
        window_minutes: Approximate smoothing window in minutes (for 1-min data)
        cols: Optional list of columns to smooth
        exclude_cols: Optional list of columns to skip

    Returns:
        DataFrame with LOESS-smoothed numeric columns
    """
    try:
        from statsmodels.nonparametric.smoothers_lowess import lowess
    except Exception as exc:
        raise ImportError(
            "statsmodels is required for LOESS smoothing. "
            "Install it in the environment to use lowess_smooth_df."
        ) from exc

    if exclude_cols is None:
        exclude_cols = []
    if cols is None:
        cols = [c for c in df.columns if c not in exclude_cols]

    out = df.copy()
    if len(out) == 0:
        return out

    x = np.arange(len(out))
    # Use window_minutes as an approximate fraction of the series length.
    frac = min(1.0, max(0.01, window_minutes / max(len(out), 1)))

    for c in cols:
        if pd.api.types.is_numeric_dtype(out[c]):
            y = out[c].to_numpy(dtype=float)
            mask = np.isfinite(y)
            if mask.sum() < 3:
                continue
            smoothed = np.full_like(y, np.nan, dtype=float)
            smoothed[mask] = lowess(
                y[mask],
                x[mask],
                frac=frac,
                it=3,
                return_sorted=False,
            )
            out[c] = smoothed
    return out

# ============================================================================
# DERIVED PARAMETER CALCULATION
# ============================================================================

def calculate_derived_parameters(df, derived_params_config, logger):
    """
    Calculate derived parameters based on user-defined formulas.

    This function safely evaluates mathematical formulas using existing
    DataFrame columns to create new derived parameters.

    Args:
        df: DataFrame with timestamp index and parameter columns
        derived_params_config: List of derived parameter definitions, each containing:
            - name: Name of the derived parameter
            - description: Description of what it calculates
            - formula: Mathematical formula using actual parameter names
            - unit: Unit of measurement
            - enabled: Boolean flag to enable/disable calculation
        logger: Logger instance

    Returns:
        DataFrame with additional derived parameter columns

    Example derived_params_config:
        [
            {
                "name": "EFFICIENCY_RATIO",
                "description": "Efficiency calculation",
                "formula": "((INLET_TEMP + OUTLET_TEMP) / OUTLET_TEMP) * 100",
                "unit": "%",
                "enabled": true
            }
        ]
    """
    if not derived_params_config:
        return df

    df_copy = df.copy()
    derived_count = 0

    for param_def in derived_params_config:
        # Skip if not enabled
        if not param_def.get("enabled", False):
            continue

        param_name = param_def.get("name")
        formula = param_def.get("formula")
        description = param_def.get("description", "")
        unit = param_def.get("unit", "")

        if not param_name or not formula:
            logger.warning("Skipping derived parameter: missing name or formula")
            continue

        try:
            # Create a safe namespace for evaluation
            # Add numpy/pandas functions and DataFrame columns
            eval_namespace = {
                'np': np,
                'pd': pd,
                'sqrt': np.sqrt,
                'abs': np.abs,
                'log': np.log,
                'log10': np.log10,
                'exp': np.exp,
                'sin': np.sin,
                'cos': np.cos,
                'tan': np.tan,
                'power': np.power,
                'round': np.round,
                'floor': np.floor,
                'ceil': np.ceil,
            }

            # Add all DataFrame columns to the namespace
            for col in df_copy.columns:
                eval_namespace[col] = df_copy[col]

            # Evaluate the formula
            df_copy[param_name] = eval(formula, {"__builtins__": {}}, eval_namespace)

            derived_count += 1

        except KeyError as e:
            logger.error(
                f"Error calculating derived parameter '{param_name}': "
                f"Missing column {e} in formula"
            )
            continue
        except Exception as e:
            logger.error(
                f"Error calculating derived parameter as api could not fetch '{param_name}': {e}"
            )
            continue

    if derived_count > 0:
        logger.info(f"Successfully calculated {derived_count} derived parameters")

    return df_copy

# ============================================================================
# RULE EVALUATION HELPERS
# ============================================================================

def collect_params(group):
    """
    Recursively collect all parameter names from a rule group.

    Args:
        group: Rule group dictionary with checks and/or nested groups

    Returns:
        List of parameter names
    """
    params = []
    logic = group.get("logic", "").upper()
    if logic == "IF":
        for key in ("if", "then", "else"):
            sub_group = group.get(key)
            if sub_group:
                params.extend(collect_params(sub_group))
        return params
    for chk in group.get("checks", []):
        if chk.get("param"):
            params.append(chk["param"])
    for sub in group.get("groups", []):
        params.extend(collect_params(sub))
    return params


def collect_param_specs(group, default_equipment):
    """
    Recursively collect (param, equipment) pairs from a rule group.

    Args:
        group: Rule group dictionary with checks and/or nested groups
        default_equipment: Equipment name to use when a check has no equipment field

    Returns:
        List of (param, equipment) tuples
    """
    specs = []
    logic = group.get("logic", "").upper()
    if logic == "IF":
        for key in ("if", "then", "else"):
            sub_group = group.get(key)
            if sub_group:
                specs.extend(collect_param_specs(sub_group, default_equipment))
        return specs
    for check in group.get("checks", []):
        param_name = check.get("param")
        if not param_name:
            continue
        equipment_name = check.get("equipment", default_equipment)
        specs.append((param_name, equipment_name))
    for sub_group in group.get("groups", []):
        specs.extend(collect_param_specs(sub_group, default_equipment))
    return specs


def evaluate_check(df, chk):
    """
    Evaluate a single check using the aggregated dataframe.

    Args:
        df: DataFrame with parameter data
        chk: Check dictionary with param, op, value

    Returns:
        Boolean Series indicating where the check passed
    """
    param = chk["param"]
    op = chk["op"]
    val = chk["value"]

    if param not in df.columns:
        return pd.Series(False, index=df.index, dtype=bool)

    s = pd.to_numeric(df[param], errors="coerce")
    s = s.replace([np.inf, -np.inf], 0)
    s = s.fillna(0)
    valid = s.notna()

    if op == "deviation":
        if isinstance(val, dict) and "pct" in val:
            if isinstance(val.get("base"), str) and val["base"] in df.columns:
                base = pd.to_numeric(df[val["base"]], errors="coerce")
            else:
                base = float(val.get("base", 0))
            out = (s < (1 - float(val["pct"])) * base) | (s > (1 + float(val["pct"])) * base)
        else:
            out = pd.Series(False, index=df.index)

    elif op == "outside":
        low = float(val["low"])
        high = float(val["high"])
        out = (s < low) | (s > high)

    elif op == "between":
        if isinstance(val, dict):
            lo, hi = float(val["low"]), float(val["high"])
        else:
            lo, hi = float(val[0]), float(val[1])
        out = (s >= lo) & (s <= hi)

    elif isinstance(val, str) and val in df.columns:
        thr = pd.to_numeric(df[val], errors="coerce")
        out = ops_map[op](s, thr)

    else:
        out = ops_map[op](s, float(val))

    out = out & valid
    return out.fillna(False).astype(bool)


def eval_group(df, group):
    """
    Recursively evaluate a rule group and return combined mask.

    Args:
        df: DataFrame with parameter data
        group: Rule group dictionary with logic, checks, and/or nested groups

    Returns:
        Tuple of (combined_mask, check_masks_list)
    """
    logic = group.get("logic", "AND").upper()
    masks, chk_masks = [], []
    if logic == "IF":
        if_group = group.get("if")
        then_group = group.get("then")
        else_group = group.get("else")
        if if_group is None or then_group is None:
            return None, []

        if_mask, if_chk = eval_group(df, if_group)
        if if_mask is None:
            return None, []

        then_mask, then_chk = eval_group(df, then_group)
        if then_mask is None:
            return None, []

        if else_group is not None:
            else_mask, else_chk = eval_group(df, else_group)
            if else_mask is None:
                return None, []
        else:
            else_mask = pd.Series(False, index=df.index, dtype=bool)
            else_chk = []

        then_active = if_mask & then_mask
        else_active = if_mask & (~then_mask) & else_mask
        combined = then_active | else_active
        chk_masks = []
        chk_masks.extend((if_mask & m, chk) for m, chk in if_chk)
        chk_masks.extend((then_active & m, chk) for m, chk in then_chk)
        chk_masks.extend((else_active & m, chk) for m, chk in else_chk)
        return combined, chk_masks

    # Evaluate individual checks
    for chk in group.get("checks", []):
        if chk["param"] not in df.columns:
            return None, []
        m = evaluate_check(df, chk)
        masks.append(m)
        chk_masks.append((m, chk))

    # Recursively evaluate nested groups
    for sub in group.get("groups", []):
        m, clist = eval_group(df, sub)
        if m is not None:
            masks.append(m)
            chk_masks.extend(clist)

    if not masks:
        return None, []

    # Combine masks based on logic (AND/OR)
    combined = masks[0]
    for m in masks[1:]:
        combined = combined & m if logic == "AND" else combined | m

    return combined, chk_masks


def value_at(df, ts, param):
    """
    Get value at timestamp for a parameter.

    Args:
        df: DataFrame with parameter data
        ts: Timestamp index
        param: Parameter name

    Returns:
        Value at the timestamp, or None if not found
    """
    if param not in df.columns:
        return None
    v = df.loc[ts, param]
    if isinstance(v, pd.Series):  # handle duplicate timestamps
        v = v.iloc[0]
    return v


# ============================================================================
# OBSERVATION TEXT FORMATTING HELPERS
# ============================================================================

def format_actual_value(value, precision=3):
    """
    Format actual value to specified precision.

    Args:
        value: Value to format (string or numeric)
        precision: Number of decimal places

    Returns:
        Formatted value string
    """
    try:
        return f"{float(value):.{precision}f}"
    except (ValueError, TypeError):
        return str(value)


def build_condition_text(param, op, value, actual_val, unit_text=""):
    """
    Build human-readable condition text based on operator.

    Args:
        param: Parameter description
        op: Operator (>, <, >=, <=, between, outside, etc.)
        value: Threshold value
        actual_val: Actual value observed
        unit_text: Unit of measurement with space prefix

    Returns:
        Human-readable condition string
    """
    if op == ">":
        return f"{param} is greater [{actual_val}] than {value}{unit_text}"
    elif op == "<":
        return f"{param} is less [{actual_val}] than {value}{unit_text}"
    elif op == ">=":
        return f"{param} is greater or equal [{actual_val}] to {value}{unit_text}"
    elif op == "<=":
        return f"{param} is less or equal [{actual_val}] to {value}{unit_text}"
    elif op == "between":
        return f"{param} lies between [{actual_val}] {value}{unit_text}"
    elif op == "abs_gt":
        return f"{param} deviates beyond ±{value}{unit_text} (actual {actual_val})"
    elif op == "outside":
        try:
            val_dict = eval(value)
            low, high = val_dict.get("low"), val_dict.get("high")
            return f"{param} is outside [{actual_val}] the normal range ({low}-{high}){unit_text}"
        except (ValueError, SyntaxError, TypeError, KeyError):
            return f"{param} is outside the specified range {unit_text}"
    else:
        return f"{param} {op} [{actual_val}] {value}{unit_text}"


def parse_check_components(check_text, param_text, show_val_text, equip_tag_text):
    """
    Parse check text and split into components handling AND/OR logic.

    Args:
        check_text: Check condition text with AND/OR operators
        param_text: Parameter names separated by ' , '
        show_val_text: Show values separated by ' , '
        equip_tag_text: Equipment tags separated by ' , '

    Returns:
        List of tuples: (group_conditions, group_params, group_show_vals, group_equip_tags)
    """
    group_conditions = [c.strip() for c in check_text.split(" AND ")]
    group_params = [p.strip() for p in param_text.split(" , ")]
    group_show_vals = [v.strip() for v in show_val_text.split(" , ")]
    group_equip_tags = [t.strip() for t in equip_tag_text.split(" , ")]

    return group_conditions, group_params, group_show_vals, group_equip_tags


def rewrite_condition_group(cond_group, group_show_val, param_to_desc, param_to_unit):
    """
    Rewrite a condition group into human-readable format.

    Args:
        cond_group: Condition text (may contain 'and' or 'or')
        group_show_val: Show value(s)
        param_to_desc: Dictionary mapping params to descriptions
        param_to_unit: Dictionary mapping params to units

    Returns:
        List of rewritten condition strings
    """
    rewritten_conditions = []

    # Determine if this group uses 'and' or 'or'
    if " or " in cond_group:
        conditions = [c.strip() for c in cond_group.split(" or ")]
        show_vals = [v.strip() for v in group_show_val.split(" or ")]
    else:
        conditions = [c.strip() for c in cond_group.split(" and ")]
        show_vals = [v.strip() for v in group_show_val.split(" and ")]

    for idx, cond in enumerate(conditions):
        parts = cond.split(" ", 2)
        if len(parts) < 3:
            rewritten_conditions.append(cond)
            continue

        param, op, value = parts[0], parts[1], parts[2]
        param_desc = param_to_desc.get(param, param.replace("_", " ").lower())
        unit = param_to_unit.get(param, "").strip()
        if unit.lower() == "nan":
            unit = ""
        unit_text = f" {unit}" if unit else ""

        # Safe mapping for actual values
        actual_val = show_vals[idx] if idx < len(show_vals) else ""
        actual_val = format_actual_value(actual_val)

        # Build condition text
        rewritten = build_condition_text(param_desc, op, value, actual_val, unit_text)
        rewritten_conditions.append(rewritten)

    return rewritten_conditions


def build_process_alert_details(row, final_observation, equipmentName, image_url):
    """
    Build the process alert details dictionary.

    Args:
        row: DataFrame row with alarm data
        final_observation: Human-readable observation text
        equipmentName: Name of equipment
        image_url: URL of related image

    Returns:
        Dictionary with process alert details
    """
    return {
        "source": "Outcome Assistant",
        "parameterName": row["equipment_tag"],
        "machineOrMachineGroupName": equipmentName,
        "valueText": final_observation,
        "alertName": row["rule_name"],
        "alertStartTime": pd.to_datetime(row["timestamp"]).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "alertEndTime": (pd.to_datetime(row["timestamp"]) + pd.Timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "standardValue": row["check"],
        "actualValue": row["actual value"],
        "observation": final_observation,
        "diagnostic": row["diagnostics"],
        "recommendation": row["recommendation"],
        "image": image_url,
        "url": " ",
    }


def build_observation_record(observation_number, equipmentName, row, final_observation):
    """
    Build an observation record for export.

    Args:
        observation_number: Sequential observation number
        equipmentName: Name of equipment
        row: DataFrame row with alarm data
        final_observation: Human-readable observation text

    Returns:
        Dictionary with observation record
    """
    return {
        "observation_number": observation_number,
        "equipment_name": equipmentName,
        "observation": final_observation,
        "failure_mode": row['rule_name'],
        "diagnostics": row['diagnostics'],
        "recommendation": row['recommendation'],
        "alert_start_time": row['timestamp'],
        "actual_value": row['actual value'],
        "equipment_tag": row['equipment_tag'],
        "check": row['check']
    }


def save_observations_to_files(observations, equipmentName, logger):
    """
    Save observations to CSV and JSON files, and generate failure mode counts.

    Args:
        observations: List of observation dictionaries
        equipmentName: Name of equipment
        logger: Logger instance

    Returns:
        Tuple of (csv_filename, json_filename, failure_mode_counts_filename, failure_mode_counts_df)
    """
    if not observations:
        return None, None, None, None

    import json
    import os

    # Create alerts_generated folder if it doesn't exist
    output_folder = "alerts_generated"
    os.makedirs(output_folder, exist_ok=True)

    timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')

    # Convert alert_start_time to IST for saved outputs only
    ist_observations = []
    for obs in observations:
        obs_copy = dict(obs)
        if "alert_start_time" in obs_copy:
            ts = pd.to_datetime(obs_copy["alert_start_time"], errors="coerce")
            if pd.notna(ts):
                obs_copy["alert_start_time"] = ts + pd.Timedelta(hours=5, minutes=30)
        ist_observations.append(obs_copy)

    # Save observations to CSV
    observations_df = pd.DataFrame(ist_observations)
    csv_filename = f"all_observations_{equipmentName}_{timestamp}.csv"
    csv_filepath = os.path.join(output_folder, csv_filename)
    observations_df.to_csv(csv_filepath, index=False)
    print(f"Saved {len(observations)} observations to {csv_filepath}")

    # Save observations to JSON
    if ist_observations and "alert_start_time" in ist_observations[0]:
        ist_observations.sort(
            key=lambda obs: pd.to_datetime(obs.get("alert_start_time"), errors="coerce")
        )
    json_filename = f"all_observations_{equipmentName}_{timestamp}.json"
    json_filepath = os.path.join(output_folder, json_filename)
    with open(json_filepath, 'w', encoding='utf-8') as f:
        json.dump(ist_observations, f, indent=2, default=str, ensure_ascii=False)
    print(f"Saved {len(observations)} observations to {json_filepath}")

    # Count failure modes
    failure_mode_counts = observations_df['failure_mode'].value_counts().reset_index()
    failure_mode_counts.columns = ['failure_mode', 'count']

    # Calculate percentage
    total_observations = len(observations_df)
    failure_mode_counts['percentage'] = (
        failure_mode_counts['count'] / total_observations * 100
    ).round(2)

    # Sort by count (descending)
    failure_mode_counts = failure_mode_counts.sort_values('count', ascending=False)

    # Save failure mode counts to CSV
    failure_counts_filename = f"failure_mode_counts_{equipmentName}_{timestamp}.csv"
    failure_counts_filepath = os.path.join(output_folder, failure_counts_filename)
    failure_mode_counts.to_csv(failure_counts_filepath, index=False)
    print(f"Saved {len(failure_mode_counts)} failure mode counts to {failure_counts_filepath}")

    return csv_filepath, json_filepath, failure_counts_filepath, failure_mode_counts





