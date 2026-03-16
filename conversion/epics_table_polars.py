import time
import polars as pl
from pathlib import Path
from common.logging import get_logger, setup_logging, INFO
from schemas.datatypes import EXPECTED_DTYPES_EPICS
from conversion.shared_polars import (
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

ACRP_TYPES = ["Feature", "Sub-capability"]


def fetch_summary_full(config: dict) -> pl.DataFrame:
    cfg = config["epics"]
    return run_query(cfg["sql_summary"], database=config["database"]["name"])


def build_acrp(df: pl.DataFrame) -> pl.DataFrame:
    # Filter: null snapshot date AND type is Feature or Sub-capability
    filtered = df.filter(
        pl.col("SNAPSHOT_DATE").is_null() & pl.col("FEATURE_TYPE").is_in(ACRP_TYPES)
    )

    logger.info(f"ACRP filter: {filtered.height} rows from {df.height} (null snapshot, Feature/Sub-capability)")

    # Split FEATURE_FIX_VERSION on comma into separate rows
    split = filtered.with_columns(
        pl.col("FEATURE_FIX_VERSION").cast(pl.Utf8).str.split(",")
    ).explode("FEATURE_FIX_VERSION").with_columns(
        pl.col("FEATURE_FIX_VERSION").str.strip_chars()
    )

    # Summarize: min and max target release per feature number
    summary = split.group_by("FEATURE_KEY").agg([
        pl.col("FEATURE_FIX_VERSION").min().alias("MIN_TARGET_RELEASE"),
        pl.col("FEATURE_FIX_VERSION").max().alias("MAX_TARGET_RELEASE"),
    ])

    # Inner join back to the split data
    result = split.join(summary, on="FEATURE_KEY", how="inner")

    logger.info(f"ACRP result: {result.height} rows, {result['FEATURE_KEY'].n_unique()} features")
    return result


def run_update_cache(config: dict):
    cfg = config["epics"]
    cache_path = CACHE_DIR / cfg["cache_filename"]
    print_header("Epics Cache Update (Polars)")
    logger.info("Updating epics history cache")
    with step_spinner(1, 1, "Updating history cache"):
        update_history(
            cfg["sql_history_full"], cfg["sql_history_recent"],
            cfg["key_column"], cache_path,
        )
    logger.info("Epics cache update complete")


def _calc_steps(publish: bool) -> int:
    return 8 if publish else 6


def run(config: dict, publish: bool = False):
    cfg = config["epics"]
    cache_path = CACHE_DIR / cfg["cache_filename"]
    hyper_path = OUTPUT_DIR / cfg["hyper_filename"]
    acrp_hyper_path = OUTPUT_DIR / cfg["acrp_hyper_filename"]
    total = _calc_steps(publish)
    start = time.time()

    print_header("Epics Pipeline (Polars)")
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
