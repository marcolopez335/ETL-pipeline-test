import time
import polars as pl
from pathlib import Path
from common.logging import get_logger, setup_logging, INFO
from schemas.datatypes import EXPECTED_DTYPES_EPICS
from conversion.shared import (
    CACHE_DIR, run_query, clean_dtypes, update_history, union_data,
    export_hyper, load_config, log_dataframe_summary, publish_hyper,
    fill_missing_snapshots,
)
from conversion.console import (
    print_header, step_spinner, print_info, print_pipeline_complete,
)

SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "output"

setup_logging(
    workflow_name=Path(__file__).stem,
    log_dir=SCRIPT_DIR / "logs",
    console_level=INFO,
)
logger = get_logger(__name__)

SPRINT_PARTITION = ["SNAPSHOT_DATE", "PROGRAM_INCREMENT"]


def _sprint_sort_key() -> pl.Expr:
    """Parse sprint version (e.g. '26.1.2' or '26.1.IP') into a sortable integer.

    Key = year * 10000 + pi * 100 + sprint, where IP = 99.
    Example: '26.1.2' -> 260102, '26.1.IP' -> 260199
    """
    version = pl.col("SPRINT_VERSION").str.split(".")
    major = version.list.get(0).cast(pl.Int64, strict=False).fill_null(0) % 100
    minor = version.list.get(1).cast(pl.Int64, strict=False).fill_null(0)
    patch_str = version.list.get(2)
    patch = (
        pl.when(patch_str == "IP")
        .then(pl.lit(99))
        .otherwise(patch_str.cast(pl.Int64, strict=False).fill_null(0))
    )
    return major * 10000 + minor * 100 + patch


def _sort_key_to_version(col_name: str, alias: str) -> pl.Expr:
    """Convert a numeric sprint sort key back to a version string."""
    key = pl.col(col_name)
    major = (key // 10000).cast(pl.Utf8)
    minor = ((key % 10000) // 100).cast(pl.Utf8)
    patch_num = key % 100
    patch = (
        pl.when(patch_num == 99)
        .then(pl.lit("IP"))
        .otherwise(patch_num.cast(pl.Utf8))
    )
    return (major + pl.lit(".") + minor + pl.lit(".") + patch).alias(alias)


def _compute_sprint_range(df: pl.DataFrame) -> pl.DataFrame:
    """Extract sprint version from SPRINT_NAME and compute min/max per partition."""
    # Extract version pattern from end of SPRINT_NAME (e.g. "26.1.2" or "26.1.IP")
    df = df.with_columns(
        pl.col("SPRINT_NAME")
        .cast(pl.Utf8)
        .str.extract(r"(\d{2,4}\.\d+\.(?:\d+|IP))\s*$")
        .alias("SPRINT_VERSION")
    )

    # Build sortable key for proper version comparison
    df = df.with_columns(_sprint_sort_key().alias("_sprint_sort_key"))

    # Min/max per SNAPSHOT_DATE + PROGRAM_INCREMENT
    df = df.with_columns([
        pl.col("_sprint_sort_key").min().over(SPRINT_PARTITION).alias("_min_key"),
        pl.col("_sprint_sort_key").max().over(SPRINT_PARTITION).alias("_max_key"),
    ])

    # Reconstruct version strings from keys
    df = df.with_columns([
        _sort_key_to_version("_min_key", "MIN_SPRINT"),
        _sort_key_to_version("_max_key", "MAX_SPRINT"),
    ])

    # Drop temp columns
    df = df.drop(["_sprint_sort_key", "_min_key", "_max_key"])

    logger.info(
        f"Sprint range: {df['MIN_SPRINT'][0]} - {df['MAX_SPRINT'][0]} "
        f"({df.select(pl.col('SPRINT_VERSION').n_unique()).item()} unique sprints)"
    )

    return df


def data_functions(df: pl.DataFrame) -> pl.DataFrame:
    """Apply all post-union transformations."""
    from datetime import datetime

    # LAST_UPDATED timestamp
    now = datetime.now()
    local_tz = now.astimezone().tzname()
    logger.info(f"LAST_UPDATED set to {now} (timezone: {local_tz})")

    df = df.with_columns(pl.lit(now).alias("LAST_UPDATED"))

    # Sprint range (MIN_SPRINT / MAX_SPRINT per snapshot + PI)
    df = _compute_sprint_range(df)

    return df


def fetch_summary_full(config: dict) -> pl.DataFrame:
    cfg = config["epics"]
    return run_query(cfg["sql_summary"], database=config["database"]["name"])


def build_acrp(df: pl.DataFrame) -> pl.DataFrame:
    # Filter: null snapshot date AND row is a feature or subcapability level
    filtered = df.filter(
        pl.col("SNAPSHOT_DATE").is_null()
        & (pl.col("FEATURE_KEY").is_not_null() | pl.col("SUBCAPABILITY_KEY").is_not_null())
    )

    logger.info(f"ACRP filter: {filtered.height} rows from {df.height} (null snapshot, feature/subcap)")

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
    return 9 if publish else 7


def run(config: dict, publish: bool = False) -> tuple[pl.DataFrame, pl.DataFrame]:
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

    with step_spinner(3, total, "Filling missing snapshots"):
        df_history = fill_missing_snapshots(df_summary, df_history, cfg["key_column"])

    with step_spinner(4, total, "Unioning & transforming"):
        df = union_data(df_summary, df_history)
        df = data_functions(df)

    log_dataframe_summary(df, "Epics Final")

    with step_spinner(5, total, "Exporting EPICS.hyper"):
        export_hyper(df, hyper_path, "Epics", config)

    with step_spinner(6, total, "Building ACRP release range"):
        df_acrp = build_acrp(df)
    log_dataframe_summary(df_acrp, "Epics ACRP")

    with step_spinner(7, total, "Exporting EPICS_ACRP.hyper"):
        export_hyper(df_acrp, acrp_hyper_path, "Epics_ACRP", config)

    if publish:
        with step_spinner(8, total, "Publishing EPICS to Tableau"):
            publish_hyper(hyper_path, "Epics", config)
        with step_spinner(9, total, "Publishing EPICS_ACRP to Tableau"):
            publish_hyper(acrp_hyper_path, "Epics_ACRP", config)

    elapsed = time.time() - start
    logger.info("Epics pipeline complete")
    print_pipeline_complete("Epics", elapsed)
    return df, df_acrp
