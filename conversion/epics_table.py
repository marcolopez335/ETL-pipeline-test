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

# Sprint version parsing constants
# IP (Innovation & Planning) is the final sprint in a PI, so it sorts last
IP_SPRINT_LABEL = "IP"
IP_SPRINT_SORT_VALUE = 99
SPRINT_VERSION_REGEX = r"(\d{2,4}\.\d+\.(?:\d+|IP))\s*$"


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
        pl.when(patch_str == IP_SPRINT_LABEL)
        .then(pl.lit(IP_SPRINT_SORT_VALUE))
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
        pl.when(patch_num == IP_SPRINT_SORT_VALUE)
        .then(pl.lit(IP_SPRINT_LABEL))
        .otherwise(patch_num.cast(pl.Utf8))
    )
    return (major + pl.lit(".") + minor + pl.lit(".") + patch).alias(alias)


def _compute_sprint_range(df: pl.DataFrame, partition_cols: list[str] = None) -> pl.DataFrame:
    """Extract sprint version from SPRINT_NAME and compute min/max per partition."""
    if partition_cols is None:
        partition_cols = SPRINT_PARTITION

    # Extract version pattern from end of SPRINT_NAME (e.g. "26.1.2" or "26.1.IP")
    df = df.with_columns(
        pl.col("SPRINT_NAME")
        .cast(pl.Utf8)
        .str.extract(SPRINT_VERSION_REGEX)
        .alias("SPRINT_VERSION")
    )

    # Warn if many SPRINT_VERSION values are null (regex didn't match)
    null_count = df["SPRINT_VERSION"].null_count()
    if df.height > 0:
        null_pct = null_count / df.height
        if null_pct > 0.10:
            logger.warning(
                f"SPRINT_VERSION: {null_count}/{df.height} ({null_pct:.0%}) values are null — "
                f"regex may not match SPRINT_NAME format"
            )

    # Build sortable key for proper version comparison
    df = df.with_columns(_sprint_sort_key().alias("_sprint_sort_key"))

    # Min/max per partition
    df = df.with_columns([
        pl.col("_sprint_sort_key").min().over(partition_cols).alias("_min_key"),
        pl.col("_sprint_sort_key").max().over(partition_cols).alias("_max_key"),
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


def _build_sprint_lookup(df: pl.DataFrame, partition_cols: list[str]) -> pl.DataFrame:
    """Compute MIN_SPRINT / MAX_SPRINT from sprint name data and collapse to lookup."""
    df = _compute_sprint_range(df, partition_cols=partition_cols)
    lookup = df.select(partition_cols + ["MIN_SPRINT", "MAX_SPRINT"]).unique()
    logger.info(f"Sprint range lookup ({partition_cols}): {lookup.height} rows")
    return lookup


def fetch_sprint_range(config: dict) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Fetch sprint range lookups for both history and summary."""
    cfg = config["epics"]
    db = config["database"]["name"]

    # History: keyed by SNAPSHOT_DATE + PROGRAM_INCREMENT
    df_hist = run_query(cfg["sql_agile_sprint_range"], database=db)
    if "SNAPSHOT_DATE" in df_hist.columns:
        df_hist = df_hist.with_columns(pl.col("SNAPSHOT_DATE").cast(pl.Date, strict=False))
    history_lookup = _build_sprint_lookup(df_hist, SPRINT_PARTITION)

    # Summary: keyed by PROGRAM_INCREMENT only (no snapshot date)
    df_sum = run_query(cfg["sql_agile_sprint_range_summary"], database=db)
    summary_lookup = _build_sprint_lookup(df_sum, ["PROGRAM_INCREMENT"])

    return history_lookup, summary_lookup


def data_functions(df: pl.DataFrame, sprint_history_lookup: pl.DataFrame,
                   sprint_summary_lookup: pl.DataFrame) -> pl.DataFrame:
    """Apply all post-union transformations."""
    from datetime import datetime

    # LAST_UPDATED timestamp
    now = datetime.now()
    local_tz = now.astimezone().tzname()
    logger.info(f"LAST_UPDATED set to {now} (timezone: {local_tz})")

    df = df.with_columns(pl.lit(now).alias("LAST_UPDATED"))

    # Join sprint range: history rows match on SNAPSHOT_DATE + PROGRAM_INCREMENT,
    # summary rows (null SNAPSHOT_DATE) match on PROGRAM_INCREMENT only.
    # First try the history lookup, then fill gaps from summary lookup.
    df = df.join(sprint_history_lookup, on=SPRINT_PARTITION, how="left")
    df = df.join(sprint_summary_lookup, on=["PROGRAM_INCREMENT"], how="left", suffix="_sum")

    # Coalesce: prefer history range, fall back to summary range
    df = df.with_columns([
        pl.coalesce(["MIN_SPRINT", "MIN_SPRINT_sum"]).alias("MIN_SPRINT"),
        pl.coalesce(["MAX_SPRINT", "MAX_SPRINT_sum"]).alias("MAX_SPRINT"),
    ]).drop(["MIN_SPRINT_sum", "MAX_SPRINT_sum"])

    # Rename columns: SNAKE_CASE -> Title Case
    rename_map = {
        col: col.lower().replace("_", " ").title()
        for col in df.columns
    }
    df = df.rename(rename_map)

    return df


def fetch_summary_full(config: dict) -> pl.DataFrame:
    cfg = config["epics"]
    return run_query(cfg["sql_summary"], database=config["database"]["name"])


def fetch_agile(config: dict, history: bool = True) -> pl.DataFrame:
    """Fetch agile sprint data (history or summary) as a separate query."""
    cfg = config["epics"]
    sql_key = "sql_agile_history" if history else "sql_agile_summary"
    df = run_query(cfg[sql_key], database=config["database"]["name"])
    # Cast SNAPSHOT_DATE to Date to match epic data (avoids type mismatch on join)
    if "SNAPSHOT_DATE" in df.columns:
        df = df.with_columns(pl.col("SNAPSHOT_DATE").cast(pl.Date, strict=False))
    return df


def join_agile(df: pl.DataFrame, df_agile: pl.DataFrame, has_snapshot: bool = True) -> pl.DataFrame:
    """Join agile sprint data onto epics at the feature level.

    Agile data is aggregated per FEATURE_ID + PROGRAM_INCREMENT (+ SNAPSHOT_DATE
    for history), so this is a many-to-one join from epics — no fan-out.
    """
    if has_snapshot:
        return df.join(
            df_agile,
            left_on=["FEATURE_KEY", "SNAPSHOT_DATE"],
            right_on=["FEATURE_ID", "SNAPSHOT_DATE"],
            how="left",
            suffix="_agile",
        )
    else:
        return df.join(
            df_agile,
            left_on=["FEATURE_KEY"],
            right_on=["FEATURE_ID"],
            how="left",
            suffix="_agile",
        )


def build_acrp(df: pl.DataFrame) -> pl.DataFrame:
    # Filter: null snapshot date AND row is a feature or subcapability level
    filtered = df.filter(
        pl.col("Snapshot Date").is_null()
        & (pl.col("Feature Key").is_not_null() | pl.col("Subcapability Key").is_not_null())
    )

    logger.info(f"ACRP filter: {filtered.height} rows from {df.height} (null snapshot, feature/subcap)")

    # Split FEATURE_FIX_VERSION on comma into separate rows
    split = filtered.with_columns(
        pl.col("Feature Fix Version").cast(pl.Utf8).str.split(",")
    ).explode("Feature Fix Version").with_columns(
        pl.col("Feature Fix Version").str.strip_chars()
    )

    # Summarize: min and max target release per feature number
    summary = split.group_by("Feature Key").agg([
        pl.col("Feature Fix Version").min().alias("Min Target Release"),
        pl.col("Feature Fix Version").max().alias("Max Target Release"),
    ])

    # Inner join back to the split data
    result = split.join(summary, on="Feature Key", how="inner")

    logger.info(f"ACRP result: {result.height} rows, {result['Feature Key'].n_unique()} features")
    return result


def run_update_cache(config: dict, force: bool = False):
    cfg = config["epics"]
    cache_path = CACHE_DIR / cfg["cache_filename"]
    print_header("Epics Cache Update (Polars)")
    logger.info("Updating epics history cache")
    with step_spinner(1, 1, "Updating history cache"):
        update_history(
            cfg["sql_history_full"], cfg["sql_history_recent"],
            cfg["key_column"], cache_path,
            config=config, force=force,
        )
    logger.info("Epics cache update complete")


def _calc_steps(publish: bool) -> int:
    return 10 if publish else 9


def run(config: dict, publish: bool = False, publish_targets: list[str] = None,
        force: bool = False) -> tuple[pl.DataFrame, pl.DataFrame]:
    cfg = config["epics"]
    cache_path = CACHE_DIR / cfg["cache_filename"]
    hyper_path = OUTPUT_DIR / cfg["hyper_filename"]
    acrp_hyper_path = OUTPUT_DIR / cfg["acrp_hyper_filename"]
    total = _calc_steps(publish)
    start = time.time()

    print_header("Epics Pipeline (Polars)")
    logger.info("Starting epics pipeline")

    with step_spinner(1, total, "Fetching epic summary"):
        df_summary = fetch_summary_full(config)
        df_summary = clean_dtypes(df_summary, EXPECTED_DTYPES_EPICS)
    log_dataframe_summary(df_summary, "Epics Summary")

    with step_spinner(2, total, "Updating epic history cache"):
        df_history = update_history(
            cfg["sql_history_full"], cfg["sql_history_recent"],
            cfg["key_column"], cache_path,
            config=config, force=force,
        )
        df_history = clean_dtypes(df_history, EXPECTED_DTYPES_EPICS)
    log_dataframe_summary(df_history, "Epics History")

    with step_spinner(3, total, "Filling missing snapshots"):
        df_history = fill_missing_snapshots(df_summary, df_history, cfg["key_column"], config=config)

    with step_spinner(4, total, "Fetching agile data"):
        df_agile_history = fetch_agile(config, history=True)
        df_agile_summary = fetch_agile(config, history=False)
        sprint_history_lookup, sprint_summary_lookup = fetch_sprint_range(config)
    log_dataframe_summary(df_agile_history, "Agile History")
    log_dataframe_summary(df_agile_summary, "Agile Summary")

    with step_spinner(5, total, "Joining agile data"):
        # Ensure SNAPSHOT_DATE is Date on both sides before joining
        df_history = df_history.with_columns(pl.col("SNAPSHOT_DATE").cast(pl.Date, strict=False))
        # Join agile history onto epic history (by FEATURE_KEY + SNAPSHOT_DATE)
        df_history = join_agile(df_history, df_agile_history, has_snapshot=True)
        # Join agile summary onto epic summary (by FEATURE_KEY only, no snapshot)
        df_summary = join_agile(df_summary, df_agile_summary, has_snapshot=False)

    with step_spinner(6, total, "Unioning & transforming"):
        df = union_data(df_summary, df_history)
        df = data_functions(df, sprint_history_lookup, sprint_summary_lookup)

    log_dataframe_summary(df, "Epics Final")

    with step_spinner(7, total, "Exporting EPICS.hyper"):
        export_hyper(df, hyper_path, "Epics", config)

    with step_spinner(8, total, "Building ACRP release range"):
        df_acrp = build_acrp(df)
    log_dataframe_summary(df_acrp, "Epics ACRP")

    with step_spinner(9, total, "Exporting EPICS_ACRP.hyper"):
        export_hyper(df_acrp, acrp_hyper_path, "Epics_ACRP", config)

    if publish:
        with step_spinner(10, total, "Publishing to Tableau"):
            publish_hyper(hyper_path, "Epics", config, targets=publish_targets,
                         datasource_name=cfg["table_id"])
            publish_hyper(acrp_hyper_path, "Epics_ACRP", config, targets=publish_targets,
                         datasource_name=cfg["acrp_table_id"])

    elapsed = time.time() - start
    logger.info("Epics pipeline complete")
    print_pipeline_complete("Epics", elapsed)
    return df, df_acrp
