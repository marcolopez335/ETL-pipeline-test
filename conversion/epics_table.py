import time
from datetime import datetime
import polars as pl
from common.logging import get_logger
from schemas.datatypes import EXPECTED_DTYPES_EPICS
from conversion.shared import (
    OUTPUT_DIR, SPRINT_VERSION_PATTERN, get_cache_path, run_query, clean_dtypes,
    update_history, union_data, export_hyper, log_dataframe_summary,
    publish_hyper, fill_missing_snapshots, history_fetch_plan, parallel_fetch,
    rename_to_snake_case, rename_to_title_case,
)
from conversion.console import (
    print_header, step_spinner, print_pipeline_complete,
)
from conversion.burnup_table import build_burnup

logger = get_logger(__name__)

SPRINT_PARTITION = ["SNAPSHOT_DATE", "PROGRAM_INCREMENT"]

# Sprint version parsing constants
# IP (Innovation & Planning) is the final sprint in a PI, so it sorts last
IP_SPRINT_LABEL = "IP"
IP_SPRINT_SORT_VALUE = 99
# Anchored version at the END of SPRINT_NAME (e.g. "26.1.2" or "26.1.IP"),
# used for the min/max sprint range; the looser SPRINT_VERSION_PATTERN from
# shared is used for CURRENT_SPRINT.
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


def _compute_sprint_range(df: pl.DataFrame, partition_cols: list[str] | None = None) -> pl.DataFrame:
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
                f"SPRINT_VERSION: {null_count}/{df.height} ({null_pct:.0%}) values are null - "
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

    if df.height > 0:
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


def _build_current_sprint_lookup(df: pl.DataFrame, partition_cols: list[str],
                                 reference_date) -> pl.DataFrame:
    """Build a lookup of CURRENT_SPRINT per partition.

    Pre-filters to the sprint whose BEGIN_DATE–END_DATE contains reference_date,
    so the result has at most one row per partition (no fan-out on join).
    """
    lookup = df.with_columns(
        pl.col("SPRINT_NAME")
        .cast(pl.Utf8)
        .str.extract(SPRINT_VERSION_PATTERN)
        .alias("CURRENT_SPRINT")
    ).select(
        partition_cols + ["CURRENT_SPRINT", "BEGIN_DATE", "END_DATE"]
    ).unique()

    # Cast dates for consistent comparison
    for col in ["BEGIN_DATE", "END_DATE"]:
        if col in lookup.columns:
            lookup = lookup.with_columns(pl.col(col).cast(pl.Date, strict=False))

    # Filter to only the sprint that contains the reference date
    ref = pl.lit(reference_date)
    lookup = lookup.filter(
        (ref >= pl.col("BEGIN_DATE")) & (ref <= pl.col("END_DATE"))
    ).select(partition_cols + ["CURRENT_SPRINT"]).unique()

    logger.info(f"Current sprint lookup ({partition_cols}): {lookup.height} rows")
    return lookup


def build_sprint_lookups(
    df_hist: pl.DataFrame, df_sum: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Build sprint range lookups from already-fetched sprint range data.

    Returns (history_lookup, summary_lookup, current_sprint_hist, current_sprint_sum).
    """
    today = datetime.now().date()

    # History: keyed by SNAPSHOT_DATE + PROGRAM_INCREMENT
    if "SNAPSHOT_DATE" in df_hist.columns:
        df_hist = df_hist.with_columns(pl.col("SNAPSHOT_DATE").cast(pl.Date, strict=False))
    history_lookup = _build_sprint_lookup(df_hist, SPRINT_PARTITION)
    current_sprint_hist = _build_current_sprint_lookup(df_hist, SPRINT_PARTITION, today)

    # Summary: keyed by PROGRAM_INCREMENT only (no snapshot date)
    summary_lookup = _build_sprint_lookup(df_sum, ["PROGRAM_INCREMENT"])
    current_sprint_sum = _build_current_sprint_lookup(df_sum, ["PROGRAM_INCREMENT"], today)

    return history_lookup, summary_lookup, current_sprint_hist, current_sprint_sum


def fetch_sprint_range(config: dict) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Fetch sprint range data and build the lookups (sequential convenience wrapper)."""
    cfg = config["epics"]
    db = config["database"]["name"]
    df_hist = run_query(cfg["sql_agile_sprint_range"], database=db, config=config)
    df_sum = run_query(cfg["sql_agile_sprint_range_summary"], database=db, config=config)
    return build_sprint_lookups(df_hist, df_sum)


def apply_transforms(df: pl.DataFrame, sprint_history_lookup: pl.DataFrame,
                     sprint_summary_lookup: pl.DataFrame,
                     current_sprint_hist: pl.DataFrame,
                     current_sprint_sum: pl.DataFrame) -> pl.DataFrame:
    """Apply all post-union transformations and rename for Tableau."""
    # LAST_UPDATED timestamp
    now = datetime.now()
    local_tz = now.astimezone().tzname()
    logger.info(f"LAST_UPDATED set to {now} (timezone: {local_tz})")

    # SNAPSHOT_DATE nulls are preserved here so build_acrp can identify summary
    # rows; they get filled in run() after ACRP is built.
    df = df.with_columns([
        pl.lit(now).alias("LAST_UPDATED"),
        pl.when(pl.col("SNAPSHOT_DATE").is_null())
          .then(pl.lit(now))
          .otherwise(pl.col("SNAPSHOT_DATE"))
          .alias("SNAPSHOT_DATE_ALT"),
    ])

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

    # CURRENT_SPRINT: pre-filtered to the sprint containing today's date,
    # so the join is one-to-one (no fan-out). History first, summary fallback.
    df = df.join(current_sprint_hist, on=SPRINT_PARTITION, how="left")
    df = df.join(current_sprint_sum, on=["PROGRAM_INCREMENT"], how="left", suffix="_sum")

    df = df.with_columns(
        pl.coalesce(["CURRENT_SPRINT", "CURRENT_SPRINT_sum"]).alias("CURRENT_SPRINT"),
    ).drop(["CURRENT_SPRINT_sum"], strict=False)

    return rename_to_title_case(df)


def fetch_summary_full(config: dict) -> pl.DataFrame:
    cfg = config["epics"]
    return run_query(cfg["sql_summary"], database=config["database"]["name"], config=config)


def fetch_agile(config: dict, history: bool = True) -> pl.DataFrame:
    """Fetch agile sprint data (history or summary) as a separate query."""
    cfg = config["epics"]
    sql_key = "sql_agile_history" if history else "sql_agile_summary"
    df = run_query(cfg[sql_key], database=config["database"]["name"], config=config)
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
    # Diagnostic: null counts for the columns the filter cares about (one pass)
    null_snap, null_feat, null_subcap = df.select([
        pl.col("Snapshot Date").is_null().sum(),
        pl.col("Feature Key").is_null().sum(),
        pl.col("Subcapability Key").is_null().sum(),
    ]).row(0)
    logger.info(
        f"ACRP diag: total={df.height} null Snapshot Date={null_snap} "
        f"null Feature Key={null_feat} null Subcapability Key={null_subcap}"
    )

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

    # Inner join back to the split data; back to SNAKE_CASE for the ACRP output
    result = rename_to_snake_case(split.join(summary, on="Feature Key", how="inner"))

    logger.info(f"ACRP result: {result.height} rows, {result['FEATURE_KEY'].n_unique()} features")
    return result


def run_update_cache(config: dict, force: bool = False):
    cfg = config["epics"]
    cache_path = get_cache_path(cfg["cache_filename"])
    print_header("Epics Cache Update (Polars)")
    logger.info("Updating epics history cache")
    with step_spinner(1, 1, "Updating history cache"):
        update_history(
            cfg["sql_history_full"], cfg["sql_history_recent"],
            cfg["key_column"], cache_path,
            config=config, force=force,
        )
    logger.info("Epics cache update complete")


def run(config: dict, publish: bool = False, publish_targets: list[str] | None = None,
        force: bool = False) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    cfg = config["epics"]
    cache_path = get_cache_path(cfg["cache_filename"])
    hyper_path = OUTPUT_DIR / cfg["hyper_filename"]
    acrp_hyper_path = OUTPUT_DIR / cfg["acrp_hyper_filename"]
    burnup_hyper_path = OUTPUT_DIR / cfg["burnup_hyper_filename"]
    total = 11 if publish else 10  # publishing adds one step
    start = time.time()

    print_header("Epics Pipeline (Polars)")
    logger.info("Starting epics pipeline")

    with step_spinner(1, total, "Fetching epic & agile data (parallel)"):
        hist_sql, hist_kind = history_fetch_plan(
            cache_path, cfg["sql_history_full"], cfg["sql_history_recent"])
        db = config["database"]["name"]
        fetched = parallel_fetch({
            "summary": lambda: fetch_summary_full(config),
            "history": lambda: run_query(hist_sql, database=db, config=config),
            "agile_history": lambda: fetch_agile(config, history=True),
            "agile_summary": lambda: fetch_agile(config, history=False),
            "sprint_range": lambda: run_query(cfg["sql_agile_sprint_range"], database=db, config=config),
            "sprint_range_summary": lambda: run_query(cfg["sql_agile_sprint_range_summary"], database=db, config=config),
            "burnup": lambda: run_query(cfg["sql_burnup"], database=db, config=config),
        }, config=config)
        df_summary = clean_dtypes(fetched["summary"], EXPECTED_DTYPES_EPICS)
    log_dataframe_summary(df_summary, "Epics Summary")

    with step_spinner(2, total, "Updating epic history cache"):
        df_history = update_history(
            cfg["sql_history_full"], cfg["sql_history_recent"],
            cfg["key_column"], cache_path,
            config=config, force=force,
            prefetched=fetched["history"], prefetched_kind=hist_kind,
        )
        df_history = clean_dtypes(df_history, EXPECTED_DTYPES_EPICS)
    log_dataframe_summary(df_history, "Epics History")

    with step_spinner(3, total, "Filling missing snapshots"):
        df_history = fill_missing_snapshots(df_summary, df_history, cfg["key_column"], config=config)

    df_agile_history = fetched["agile_history"]
    df_agile_summary = fetched["agile_summary"]
    log_dataframe_summary(df_agile_history, "Agile History")
    log_dataframe_summary(df_agile_summary, "Agile Summary")

    with step_spinner(4, total, "Joining agile data"):
        sprint_history_lookup, sprint_summary_lookup, current_sprint_hist, current_sprint_sum = \
            build_sprint_lookups(fetched["sprint_range"], fetched["sprint_range_summary"])
        # Ensure SNAPSHOT_DATE is Date on both sides before joining
        df_history = df_history.with_columns(pl.col("SNAPSHOT_DATE").cast(pl.Date, strict=False))
        # Join agile history onto epic history (by FEATURE_KEY + SNAPSHOT_DATE)
        df_history = join_agile(df_history, df_agile_history, has_snapshot=True)
        # Join agile summary onto epic summary (by FEATURE_KEY only, no snapshot)
        df_summary = join_agile(df_summary, df_agile_summary, has_snapshot=False)

    with step_spinner(5, total, "Unioning & transforming"):
        df = union_data(df_summary, df_history)
        df = apply_transforms(df, sprint_history_lookup, sprint_summary_lookup,
                              current_sprint_hist, current_sprint_sum)

    # Build ACRP before filling Snapshot Date nulls — its filter relies on the
    # null marker to identify summary rows.
    with step_spinner(6, total, "Building ACRP release range"):
        df_acrp = build_acrp(df)
    log_dataframe_summary(df_acrp, "Epics ACRP")

    df = df.with_columns(
        pl.col("Snapshot Date").fill_null(pl.col("Last Updated").cast(pl.Date, strict=False))
    )
    log_dataframe_summary(df, "Epics Final")

    with step_spinner(7, total, "Building feature burn-up"):
        df_burnup = build_burnup(fetched["burnup"])
    log_dataframe_summary(df_burnup, "Feature Burn-Up")

    with step_spinner(8, total, "Exporting EPICS.hyper"):
        export_hyper(df, hyper_path, "Epics", config)

    with step_spinner(9, total, "Exporting EPICS_ACRP.hyper"):
        export_hyper(df_acrp, acrp_hyper_path, "Epics_ACRP", config)

    with step_spinner(10, total, "Exporting FEATURE_BURNUP.hyper"):
        export_hyper(df_burnup, burnup_hyper_path, "Feature_Burnup", config)

    if publish:
        with step_spinner(11, total, "Publishing to Tableau"):
            publish_hyper(hyper_path, config, targets=publish_targets,
                          datasource_name=cfg["table_id"])
            publish_hyper(acrp_hyper_path, config, targets=publish_targets,
                          datasource_name=cfg["acrp_table_id"])
            publish_hyper(burnup_hyper_path, config, targets=publish_targets,
                          datasource_name=cfg["burnup_table_id"])

    elapsed = time.time() - start
    logger.info("Epics pipeline complete")
    print_pipeline_complete("Epics", elapsed)
    return df, df_acrp, df_burnup
