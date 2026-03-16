import time
import pandas as pd
from pathlib import Path
from common.logging import get_logger, setup_logging, INFO
from schemas.datatypes import EXPECTED_DTYPES_EPICS
from conversion.shared import (
    CACHE_DIR, run_query, clean_dtypes, update_history, union_data,
    export_hyper, load_config, log_dataframe_summary, publish_hyper,
)
from conversion.console import (
    print_header, step_spinner, print_pipeline_complete,
)

SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "output"

setup_logging(
    workflow_name=Path(__file__).stem,
    log_dir=SCRIPT_DIR / "logs",
    console_level=INFO,
)
logger = get_logger(__name__)

ACRP_TYPES = {"Feature", "Sub-capability"}


def fetch_summary_full(config: dict) -> pd.DataFrame:
    cfg = config["epics"]
    df = run_query(cfg["sql_summary"], database=config["database"]["name"])

    if "LAST_UPDATED" in df.columns:
        df["LAST_UPDATED"] = pd.to_datetime(df["LAST_UPDATED"], errors="coerce")

    key_col = cfg["key_column"]
    if key_col in df.columns:
        df[key_col] = df[key_col].astype(str)

    return df


def build_acrp(df: pd.DataFrame) -> pd.DataFrame:
    # Filter: null snapshot date AND type is Feature or Sub-capability
    mask = df["SNAPSHOT_DATE"].isna() & df["FEATURE_TYPE"].isin(ACRP_TYPES)
    filtered = df.loc[mask].copy()

    logger.info(f"ACRP filter: {len(filtered)} rows from {len(df)} (null snapshot, Feature/Sub-capability)")

    # Split FEATURE_FIX_VERSION on comma into separate rows
    filtered["FEATURE_FIX_VERSION"] = filtered["FEATURE_FIX_VERSION"].astype(str)
    split = filtered.assign(
        FEATURE_FIX_VERSION=filtered["FEATURE_FIX_VERSION"].str.split(",")
    ).explode("FEATURE_FIX_VERSION", ignore_index=True)
    split["FEATURE_FIX_VERSION"] = split["FEATURE_FIX_VERSION"].str.strip()

    # Summarize: min and max target release per feature number
    summary = (
        split.groupby("FEATURE_NUMBER", as_index=False)
        .agg(
            MIN_TARGET_RELEASE=("FEATURE_FIX_VERSION", "min"),
            MAX_TARGET_RELEASE=("FEATURE_FIX_VERSION", "max"),
        )
    )

    # Inner join back to the split data
    result = split.merge(summary, on="FEATURE_NUMBER", how="inner")

    logger.info(f"ACRP result: {len(result)} rows, {result['FEATURE_NUMBER'].nunique()} features")
    return result


def run_update_cache(config: dict):
    cfg = config["epics"]
    cache_path = CACHE_DIR / cfg["cache_filename"]
    print_header("Epics Cache Update")
    logger.info("Updating epics history cache")
    with step_spinner(1, 1, "Updating history cache"):
        update_history(
            cfg["sql_history_full"], cfg["sql_history_recent"],
            cfg["key_column"], cache_path,
        )
    logger.info("Epics cache update complete")


def _calc_steps(publish: bool) -> int:
    # base: summary, history, union, export epics, build acrp, export acrp = 6
    # publish adds 2 more (publish epics + publish acrp)
    return 8 if publish else 6


def run(config: dict, publish: bool = False):
    cfg = config["epics"]
    cache_path = CACHE_DIR / cfg["cache_filename"]
    hyper_path = OUTPUT_DIR / cfg["hyper_filename"]
    acrp_hyper_path = OUTPUT_DIR / cfg["acrp_hyper_filename"]
    total = _calc_steps(publish)
    start = time.time()

    print_header("Epics Pipeline")
    logger.info("Starting epics pipeline")

    with step_spinner(1, total, "Fetching summary"):
        df_summary = fetch_summary_full(config)
        df_summary = clean_dtypes(df_summary, EXPECTED_DTYPES_EPICS)
    log_dataframe_summary(df_summary, "Epics Summary")

    with step_spinner(2, total, "Updating history cache"):
        df_history = update_history(
            cfg["sql_history_full"], cfg["sql_history_recent"],
            cfg["key_column"], cache_path,
        )
        df_history = clean_dtypes(df_history, EXPECTED_DTYPES_EPICS)
    log_dataframe_summary(df_history, "Epics History")

    with step_spinner(3, total, "Unioning data"):
        df = union_data(df_summary, df_history)

    log_dataframe_summary(df, "Epics Final")

    with step_spinner(4, total, "Exporting EPICS.hyper"):
        export_hyper(df, hyper_path, "Epics", config)

    with step_spinner(5, total, "Building ACRP release range"):
        df_acrp = build_acrp(df)
    log_dataframe_summary(df_acrp, "Epics ACRP")

    with step_spinner(6, total, "Exporting EPICS_ACRP.hyper"):
        export_hyper(df_acrp, acrp_hyper_path, "Epics_ACRP", config)

    if publish:
        with step_spinner(7, total, "Publishing EPICS to Tableau"):
            publish_hyper(hyper_path, "Epics", config)
        with step_spinner(8, total, "Publishing EPICS_ACRP to Tableau"):
            publish_hyper(acrp_hyper_path, "Epics_ACRP", config)

    elapsed = time.time() - start
    logger.info("Epics pipeline complete")
    print_pipeline_complete("Epics", elapsed)
