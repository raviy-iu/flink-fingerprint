# ---------- SHEET AND COLUMN NAMES ----------
# Sheet name containing metadata mapping
MAPPING_SHEET = "Metadata_mapping"

# Column names in mapping file
GENERIC_COL = "Comparative_tag"  # rules use these
DESC_COL = "Metadata"  # Parameter description
UNIT_COL = "UOM"  # Unit of measurement

# ---------- INDUSTRY DATABASE CONFIGURATIONS ----------
# Define database configurations
DATABASES = {
    "cement": "cement/cement_ind_db.json",
    "aluminium foil": "aluminium_foil/aluminium_foil_ind_db.json"
}

# ---------- CEMENT INDUSTRY MAPPINGS ----------
cement_metadata_mapping = {
    "coal mill": "Coal_mill_mapping.xlsx",
    "kiln": "Kiln_mapping.xlsx",
    "raw mill": "Raw_mill_mapping.xlsx",
    "crusher": "Crusher_mapping.xlsx",
    "cement mill": "Cement_mill_mapping.xlsx",
    "cooler": "Cooler_mapping.xlsx"
}

cement_rules_mapping = {
    "coal mill": "coal_mill_rules.json",
    "kiln": "kiln_rules.json",
    "raw mill": "raw_mill_rules.json",
    "cement mill": "cement_mill_rules.json",
    "crusher": "crusher_rules.json",
    "cooler": "cooler_rules.json"
}

cement_rules_overrides_file = "rules_overrides.json"




# ---------- ALUMINIUM FOIL INDUSTRY MAPPINGS ----------
al_foil_metadata_mapping = {
    "Twin Chamber Furnace": "tcf_mapping.xlsx",
    "holding furnace": "holder_mapping.xlsx",
    "casting machine": "caster_mapping.xlsx",
    "rolling mill": "mill_mapping.xlsx",
    "cold rolling mill": "crm_mapping.xlsx",
    "vacuum distillation unit": "vdu_mapping.xlsx",
    "foil annealing furnace": "faf_mapping.xlsx",
    "separator": "separator_mapping.xlsx",
    "FRP annealing furnace": "frp_af_mapping.xlsx",
    "FRP slitter": "frp_slitter_mapping.xlsx"
}

al_foil_rules_mapping = {
    "Twin Chamber Furnace": "tcf_rules.json",
    "holding furnace": "holder_rules.json",
    "casting machine": "caster_rules.json",
    "rolling mill": "mill_rules.json",
    "cold rolling mill": "crm_rules.json",
    "vacuum distillation unit": "vdu_rules.json",
    "foil annealing furnace": "faf_rules.json",
    "separator": "separator_rules.json",
    "FRP annealing furnace": "frp_af_rules.json",
    "FRP slitter": "frp_slitter_rules.json"
}

# ---------- INDUSTRY CONFIGURATIONS ----------
INDUSTRY_CONFIGS = {
    "cement": {
        "base_path": "cement",
        "metadata_mapping": cement_metadata_mapping,
        "rules_mapping": cement_rules_mapping,
        "rules_overrides_file": cement_rules_overrides_file,
        "derived_params_file": "cement_derived_params.json",
        "run_time_tags_file": "cement_run_time_tags.json",
        "cyclic_trend_tags_file": "cyclic_trend_tags.json",
        "url": "https://starcement.covacsis.com/api/client/raw-parameter/trend",
        "api_key_env": "COVACSIS_API_KEY",
        "default_api_key": "wqa8IHvhg8zLVQQJpD6RFcYRnBkD0xp3"
    }


    # "aluminium foil": {
    #     "base_path": "aluminium_foil",
    #     "metadata_mapping": al_foil_metadata_mapping,
    #     "rules_mapping": al_foil_rules_mapping,
    #     "derived_params_file": "al_foil_derived_params.json",
    #     "run_time_tags_file": "al_foil_run_time_tags.json",
    #     "url": "https://hindalco.covacsis.com/api/client/raw-parameter/trend",
    #     "api_key_env": "HINDALCO_API_KEY",
    #     "default_api_key": "abc123xyz789"
    # }
}
# Note: The aluminium foil industry configuration is currently commented out.




