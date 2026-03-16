import time
import pandas as pd
from pathlib import Path
from common.logging import get_logger, setup_logging, INFO
from schemas.datatypes import EXPECTED_DTYPES_EPICS
from conversion.shared import (
    CACHE_DIR, run_query, clean_dtypes, update_history, union_data,
    export_hyper, load_config, log_dataframe_summary,
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

TOTAL_STEPS = 4


def fetch_summary_full(config: dict) -> pd.DataFrame:
    cfg = config["epics"]
    df = run_query(cfg["sql_summary"], database=config["database"]["name"])

    if "LAST_UPDATED" in df.columns:
        df["LAST_UPDATED"] = pd.to_datetime(df["LAST_UPDATED"], errors="coerce")

    key_col = cfg["key_column"]
    if key_col in df.columns:
        df[key_col] = df[key_col].astype(str)

    return df


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


def run(config: dict):
    cfg = config["epics"]
    cache_path = CACHE_DIR / cfg["cache_filename"]
    hyper_path = OUTPUT_DIR / cfg["hyper_filename"]
    start = time.time()

    print_header("Epics Pipeline")
    logger.info("Starting epics pipeline")

    with step_spinner(1, TOTAL_STEPS, "Fetching summary"):
        df_summary = fetch_summary_full(config)
        df_summary = clean_dtypes(df_summary, EXPECTED_DTYPES_EPICS)
    log_dataframe_summary(df_summary, "Epics Summary")

    with step_spinner(2, TOTAL_STEPS, "Updating history cache"):
        df_history = update_history(
            cfg["sql_history_full"], cfg["sql_history_recent"],
            cfg["key_column"], cache_path,
        )
        df_history = clean_dtypes(df_history, EXPECTED_DTYPES_EPICS)
    log_dataframe_summary(df_history, "Epics History")

    with step_spinner(3, TOTAL_STEPS, "Unioning data"):
        df = union_data(df_summary, df_history)

    log_dataframe_summary(df, "Epics Final")

    with step_spinner(4, TOTAL_STEPS, "Exporting hyper"):
        export_hyper(df, hyper_path, "Epics", config)

    elapsed = time.time() - start
    logger.info("Epics pipeline complete")
    print_pipeline_complete("Epics", elapsed)
