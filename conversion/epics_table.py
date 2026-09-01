"""Epics pipeline -- every epic with the hierarchy above it, per snapshot and PI.

Data model
----------
::

    EPICS = (epic history  U  epic summary)
            LEFT JOIN agile rollups    ON FEATURE_KEY = FEATURE_ID (+ SNAPSHOT_DATE for history)
            LEFT JOIN sprint lookups   ON PROGRAM_INCREMENT        (+ SNAPSHOT_DATE for history)

* **Epic history / summary** -- ``EpicHistory.sql`` (cache seed),
  ``EpicHistory_recent.sql`` (last 30 days) and ``EpicSummary.sql`` (live).
  Each returns one row per Epic with everything above it in the Jira
  hierarchy flattened onto the row::

      Epic -> Feature -> Sub-Capability -> Customer Capability -> Customer Epic (PROGRAM)

* **Agile rollups** -- ``AgileHistory.sql`` / ``AgileSummary.sql``: story
  points per FEATURE_ID + PROGRAM_INCREMENT (+ SNAPSHOT_DATE). This join is
  where PROGRAM_INCREMENT enters the epic row, so the output grain is
  *(epic, program increment, snapshot)*: a feature with stories in two PIs
  yields two rows per epic; a feature with no stories keeps one row with
  null rollups.
* **Sprint lookups** -- ``AgileSprintRange.sql`` / ``AgileSprintRange_summary.sql``:
  sprint names and dates per PI, parsed into MIN_SPRINT / MAX_SPRINT and
  the CURRENT_SPRINT containing today. History rows use the per-snapshot
  lookup; summary rows fall back to the current one.

The summary owns "today" in the export: history rows dated today are
dropped first (``shared.drop_todays_history``), exactly as in stories.

Outputs
-------
* ``EPICS.hyper`` -- the frame above, Title Case columns, summary rows
  stamped with today's SNAPSHOT_DATE.
* ``EPICS_ACRP.hyper`` -- Active Capability Release Plan: the summary rows
  at feature / sub-capability level, one row per FEATURE_FIX_VERSION, with
  the min/max target release per feature. SCREAMING_SNAKE_CASE columns.
* ``FEATURE_BURNUP.hyper`` -- see ``conversion.burnup_table``.

Steps: fetch (parallel) -> update history cache -> fill missing weekly
snapshots -> build (agile joins, union, transforms, ACRP) -> burn-up ->
export -> publish.
"""

import time
from dataclasses import dataclass
from datetime import date, datetime

import polars as pl

from schemas.datatypes import EXPECTED_DTYPES_AGILE, EXPECTED_DTYPES_EPICS
from conversion.burnup_table import build_burnup
from conversion.console import print_header, print_pipeline_complete, step_spinner
# get_logger comes via shared so this module imports (and its tests run)
# without the proprietary csm_commonlib package
from conversion.shared import (
    OUTPUT_DIR, SPRINT_VERSION_PATTERN, clean_dtypes, drop_todays_history,
    export_hyper, fill_missing_snapshots, get_cache_path, get_logger,
    history_fetch_plan, log_dataframe_summary, parallel_fetch, publish_hyper,
    rename_to_title_case, run_query, union_data, update_history,
)

logger = get_logger(__name__)

PIPELINE_NAME = "Epics"

# Agile rollups are keyed by FEATURE_ID; the epic row carries its feature as FEATURE_KEY
AGILE_LEFT_KEY = "FEATURE_KEY"
AGILE_RIGHT_KEY = "FEATURE_ID"
AGILE_SUFFIX = "_agile"

# Sprint lookups: history rows are keyed per snapshot, summary rows per PI only
SPRINT_PARTITION_HISTORY = ["SNAPSHOT_DATE", "PROGRAM_INCREMENT"]
SPRINT_PARTITION_SUMMARY = ["PROGRAM_INCREMENT"]

# Sprint version parsing. IP (Innovation & Planning) is the final sprint in
# a PI, so it sorts after every numbered sprint.
IP_SPRINT_LABEL = "IP"
IP_SPRINT_SORT_VALUE = 99
# Anchored version at the END of SPRINT_NAME (e.g. "26.1.2" or "26.1.IP"),
# used for the min/max sprint range; the looser SPRINT_VERSION_PATTERN from
# shared is used for CURRENT_SPRINT (same parse as the stories' SPRINT_NAME_ALT).
SPRINT_VERSION_REGEX = r"(\d{2,4}\.\d+\.(?:\d+|IP))\s*$"


# ---------------------------------------------------------------------------
# Sprint parsing & lookups
# ---------------------------------------------------------------------------

def _sprint_sort_key() -> pl.Expr:
    """Parse SPRINT_VERSION (e.g. '26.1.2' or '26.1.IP') into a sortable integer.

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


def _compute_sprint_range(df: pl.DataFrame, partition_cols: list[str]) -> pl.DataFrame:
    """Extract SPRINT_VERSION from SPRINT_NAME and add MIN_SPRINT / MAX_SPRINT per partition."""
    df = df.with_columns(
        pl.col("SPRINT_NAME")
        .cast(pl.Utf8)
        .str.extract(SPRINT_VERSION_REGEX)
        .alias("SPRINT_VERSION")
    )

    # Warn if many SPRINT_VERSION values are null (regex didn't match)
    null_count = df["SPRINT_VERSION"].null_count()
    if df.height > 0 and null_count / df.height > 0.10:
        logger.warning(
            f"SPRINT_VERSION: {null_count}/{df.height} ({null_count / df.height:.0%}) "
            f"values are null - regex may not match SPRINT_NAME format"
        )

    df = df.with_columns(_sprint_sort_key().alias("_sprint_sort_key"))
    df = df.with_columns([
        pl.col("_sprint_sort_key").min().over(partition_cols).alias("_min_key"),
        pl.col("_sprint_sort_key").max().over(partition_cols).alias("_max_key"),
    ])
    df = df.with_columns([
        _sort_key_to_version("_min_key", "MIN_SPRINT"),
        _sort_key_to_version("_max_key", "MAX_SPRINT"),
    ]).drop(["_sprint_sort_key", "_min_key", "_max_key"])

    if df.height > 0:
        logger.info(
            f"Sprint range: {df['MIN_SPRINT'][0]} - {df['MAX_SPRINT'][0]} "
            f"({df.select(pl.col('SPRINT_VERSION').n_unique()).item()} unique sprints)"
        )
    return df


def _build_sprint_lookup(df: pl.DataFrame, partition_cols: list[str]) -> pl.DataFrame:
    """MIN_SPRINT / MAX_SPRINT per partition, collapsed to one row per partition."""
    df = _compute_sprint_range(df, partition_cols)
    lookup = df.select(partition_cols + ["MIN_SPRINT", "MAX_SPRINT"]).unique()
    logger.info(f"Sprint range lookup ({partition_cols}): {lookup.height} rows")
    return lookup


def _build_current_sprint_lookup(df: pl.DataFrame, partition_cols: list[str],
                                 reference_date: date) -> pl.DataFrame:
    """CURRENT_SPRINT per partition: the sprint whose BEGIN_DATE..END_DATE contains reference_date.

    Pre-filtering to that one sprint keeps the result at most one row per
    partition, so the later join cannot fan out.
    """
    lookup = df.with_columns(
        pl.col("SPRINT_NAME")
        .cast(pl.Utf8)
        .str.extract(SPRINT_VERSION_PATTERN)
        .alias("CURRENT_SPRINT")
    ).select(
        partition_cols + ["CURRENT_SPRINT", "BEGIN_DATE", "END_DATE"]
    ).unique()

    lookup = lookup.with_columns([
        pl.col("BEGIN_DATE").cast(pl.Date, strict=False),
        pl.col("END_DATE").cast(pl.Date, strict=False),
    ])
    ref = pl.lit(reference_date)
    lookup = lookup.filter(
        (ref >= pl.col("BEGIN_DATE")) & (ref <= pl.col("END_DATE"))
    ).select(partition_cols + ["CURRENT_SPRINT"]).unique()

    logger.info(f"Current sprint lookup ({partition_cols}): {lookup.height} rows")
    return lookup


@dataclass(frozen=True)
class SprintLookups:
    """Per-partition sprint attributes derived from the sprint range queries."""
    range_history: pl.DataFrame    # SNAPSHOT_DATE + PROGRAM_INCREMENT -> MIN_SPRINT, MAX_SPRINT
    range_summary: pl.DataFrame    # PROGRAM_INCREMENT -> MIN_SPRINT, MAX_SPRINT
    current_history: pl.DataFrame  # SNAPSHOT_DATE + PROGRAM_INCREMENT -> CURRENT_SPRINT
    current_summary: pl.DataFrame  # PROGRAM_INCREMENT -> CURRENT_SPRINT


def build_sprint_lookups(df_hist: pl.DataFrame, df_sum: pl.DataFrame,
                         today: date | None = None) -> SprintLookups:
    """Build the four sprint lookups from the sprint range query results."""
    if today is None:
        today = datetime.now().date()
    # History is keyed per snapshot day
    df_hist = df_hist.with_columns(pl.col("SNAPSHOT_DATE").cast(pl.Date, strict=False))
    return SprintLookups(
        range_history=_build_sprint_lookup(df_hist, SPRINT_PARTITION_HISTORY),
        range_summary=_build_sprint_lookup(df_sum, SPRINT_PARTITION_SUMMARY),
        current_history=_build_current_sprint_lookup(df_hist, SPRINT_PARTITION_HISTORY, today),
        current_summary=_build_current_sprint_lookup(df_sum, SPRINT_PARTITION_SUMMARY, today),
    )


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_summary(config: dict) -> pl.DataFrame:
    """Live epic hierarchy rows (``sql_summary``), dtypes normalized."""
    cfg = config["epics"]
    df = run_query(cfg["sql_summary"], database=config["database"]["name"], config=config)
    return clean_dtypes(df, EXPECTED_DTYPES_EPICS)


def fetch_agile(config: dict, sql_key: str) -> pl.DataFrame:
    """One of the agile queries (a ``sql_agile_*`` config key), dtypes normalized.

    SNAPSHOT_DATE comes back as a Date so it joins cleanly onto epic history.
    """
    cfg = config["epics"]
    df = run_query(cfg[sql_key], database=config["database"]["name"], config=config)
    return clean_dtypes(df, EXPECTED_DTYPES_AGILE)


# ---------------------------------------------------------------------------
# Build (pure DataFrame -> DataFrame; no database access)
# ---------------------------------------------------------------------------

def join_agile(df: pl.DataFrame, df_agile: pl.DataFrame, by_snapshot: bool) -> pl.DataFrame:
    """Attach the feature-level agile rollup to each epic row.

    The rollup is one row per FEATURE_ID + PROGRAM_INCREMENT (+ SNAPSHOT_DATE
    for history), so an epic whose feature has stories in several PIs fans
    out to one row per PI -- that is how PROGRAM_INCREMENT reaches the epic
    row. Epics whose feature has no rollup keep a single row with nulls.
    """
    left_on, right_on = [AGILE_LEFT_KEY], [AGILE_RIGHT_KEY]
    if by_snapshot:
        left_on.append("SNAPSHOT_DATE")
        right_on.append("SNAPSHOT_DATE")
    return df.join(df_agile, left_on=left_on, right_on=right_on, how="left", suffix=AGILE_SUFFIX)


def apply_transforms(df: pl.DataFrame, lookups: SprintLookups,
                     now: datetime | None = None) -> pl.DataFrame:
    """Post-union computed columns (SCREAMING_SNAKE_CASE in and out).

    * LAST_UPDATED -- pipeline run time (data-freshness stamp)
    * SNAPSHOT_DATE_ALT -- SNAPSHOT_DATE, with summary rows given the exact
      run timestamp
    * MIN_SPRINT / MAX_SPRINT -- sprint range for the row's snapshot + PI
      (per-snapshot lookup first, current lookup as the fallback)
    * CURRENT_SPRINT -- the sprint containing today for the row's snapshot + PI

    SNAPSHOT_DATE nulls (summary rows) are preserved here so ``build_acrp``
    can identify them; ``finalize_epics`` fills them afterwards.
    """
    if now is None:
        now = datetime.now()
    local_tz = now.astimezone().tzname()
    logger.info(f"LAST_UPDATED set to {now} (timezone: {local_tz})")

    df = df.with_columns([
        pl.lit(now).alias("LAST_UPDATED"),
        pl.when(pl.col("SNAPSHOT_DATE").is_null())
          .then(pl.lit(now))
          .otherwise(pl.col("SNAPSHOT_DATE"))
          .alias("SNAPSHOT_DATE_ALT"),
    ])

    # Sprint range: history rows match on SNAPSHOT_DATE + PROGRAM_INCREMENT;
    # summary rows (NULL SNAPSHOT_DATE) only match the PI-level fallback.
    df = df.join(lookups.range_history, on=SPRINT_PARTITION_HISTORY, how="left")
    df = df.join(lookups.range_summary, on=SPRINT_PARTITION_SUMMARY, how="left", suffix="_sum")
    df = df.with_columns([
        pl.coalesce(["MIN_SPRINT", "MIN_SPRINT_sum"]).alias("MIN_SPRINT"),
        pl.coalesce(["MAX_SPRINT", "MAX_SPRINT_sum"]).alias("MAX_SPRINT"),
    ]).drop(["MIN_SPRINT_sum", "MAX_SPRINT_sum"])

    # CURRENT_SPRINT: the lookups hold at most one sprint per partition, so
    # these joins are one-to-one. History first, summary as the fallback.
    df = df.join(lookups.current_history, on=SPRINT_PARTITION_HISTORY, how="left")
    df = df.join(lookups.current_summary, on=SPRINT_PARTITION_SUMMARY, how="left", suffix="_sum")
    return df.with_columns(
        pl.coalesce(["CURRENT_SPRINT", "CURRENT_SPRINT_sum"]).alias("CURRENT_SPRINT"),
    ).drop(["CURRENT_SPRINT_sum"], strict=False)


def build_acrp(df: pl.DataFrame) -> pl.DataFrame:
    """Active Capability Release Plan from the transformed (SCREAMING_SNAKE_CASE) frame.

    Takes the summary rows (NULL SNAPSHOT_DATE -- call before the fill) at
    feature or sub-capability level, explodes the comma-separated
    FEATURE_FIX_VERSION into one row per release, and adds the min / max
    target release per feature. Output stays SCREAMING_SNAKE_CASE.
    """
    null_snap, null_feat, null_subcap = df.select([
        pl.col("SNAPSHOT_DATE").is_null().sum(),
        pl.col("FEATURE_KEY").is_null().sum(),
        pl.col("SUBCAPABILITY_KEY").is_null().sum(),
    ]).row(0)
    logger.info(
        f"ACRP diag: total={df.height} null SNAPSHOT_DATE={null_snap} "
        f"null FEATURE_KEY={null_feat} null SUBCAPABILITY_KEY={null_subcap}"
    )

    filtered = df.filter(
        pl.col("SNAPSHOT_DATE").is_null()
        & (pl.col("FEATURE_KEY").is_not_null() | pl.col("SUBCAPABILITY_KEY").is_not_null())
    )
    logger.info(f"ACRP filter: {filtered.height} rows from {df.height} (null snapshot, feature/subcap)")

    split = filtered.with_columns(
        pl.col("FEATURE_FIX_VERSION").cast(pl.Utf8).str.split(",")
    ).explode("FEATURE_FIX_VERSION").with_columns(
        pl.col("FEATURE_FIX_VERSION").str.strip_chars()
    )

    release_range = split.group_by("FEATURE_KEY").agg([
        pl.col("FEATURE_FIX_VERSION").min().alias("MIN_TARGET_RELEASE"),
        pl.col("FEATURE_FIX_VERSION").max().alias("MAX_TARGET_RELEASE"),
    ])
    result = split.join(release_range, on="FEATURE_KEY", how="inner")

    logger.info(f"ACRP result: {result.height} rows, {result['FEATURE_KEY'].n_unique()} features")
    return result


def finalize_epics(df: pl.DataFrame, today: date) -> pl.DataFrame:
    """Stamp summary rows with today's SNAPSHOT_DATE and rename for Tableau."""
    df = df.with_columns(pl.col("SNAPSHOT_DATE").fill_null(pl.lit(today)))
    return rename_to_title_case(df)


def build_epics(
    df_summary: pl.DataFrame, df_history: pl.DataFrame,
    agile_history: pl.DataFrame, agile_summary: pl.DataFrame,
    sprint_range: pl.DataFrame, sprint_range_summary: pl.DataFrame,
    now: datetime | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Epic summary + history + agile data -> ``(EPICS frame, ACRP frame)``.

    Pure function of its inputs (``now`` is injectable), so the whole
    join / union / transform / ACRP chain can be exercised on in-memory frames.
    """
    if now is None:
        now = datetime.now()
    today = now.date()
    lookups = build_sprint_lookups(sprint_range, sprint_range_summary, today=today)

    df_history = drop_todays_history(df_history, today=today)
    df_history = join_agile(df_history, agile_history, by_snapshot=True)
    df_summary = join_agile(df_summary, agile_summary, by_snapshot=False)

    df = union_data(df_summary, df_history)
    df = apply_transforms(df, lookups, now=now)

    # ACRP before the SNAPSHOT_DATE fill -- its filter relies on the null marker
    df_acrp = build_acrp(df)
    return finalize_epics(df, today), df_acrp


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def run_update_cache(config: dict, force: bool = False) -> None:
    """Refresh the epics history cache only (no export)."""
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
    """Full epics pipeline: fetch, cache, build, export and (optionally) publish.

    Returns ``(epics, acrp, burnup)`` for the interactive SQL shell.
    """
    cfg = config["epics"]
    cache_path = get_cache_path(cfg["cache_filename"])
    hyper_path = OUTPUT_DIR / cfg["hyper_filename"]
    acrp_hyper_path = OUTPUT_DIR / cfg["acrp_hyper_filename"]
    burnup_hyper_path = OUTPUT_DIR / cfg["burnup_hyper_filename"]
    total = 9 if publish else 8  # publishing adds one step
    start = time.time()

    print_header("Epics Pipeline (Polars)")
    logger.info("Starting epics pipeline")

    with step_spinner(1, total, "Fetching epic, agile & sprint data (parallel)"):
        hist_sql, hist_kind = history_fetch_plan(
            cache_path, cfg["sql_history_full"], cfg["sql_history_recent"])
        db = config["database"]["name"]
        fetched = parallel_fetch({
            "summary": lambda: fetch_summary(config),
            "history": lambda: run_query(hist_sql, database=db, config=config),
            "agile_history": lambda: fetch_agile(config, "sql_agile_history"),
            "agile_summary": lambda: fetch_agile(config, "sql_agile_summary"),
            "sprint_range": lambda: fetch_agile(config, "sql_agile_sprint_range"),
            "sprint_range_summary": lambda: fetch_agile(config, "sql_agile_sprint_range_summary"),
            "burnup": lambda: run_query(cfg["sql_burnup"], database=db, config=config),
        }, config=config)
    df_summary = fetched["summary"]
    log_dataframe_summary(df_summary, "Epics Summary")
    log_dataframe_summary(fetched["agile_history"], "Agile History")
    log_dataframe_summary(fetched["agile_summary"], "Agile Summary")

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

    with step_spinner(4, total, "Joining agile data, transforming & building ACRP"):
        df, df_acrp = build_epics(
            df_summary, df_history,
            fetched["agile_history"], fetched["agile_summary"],
            fetched["sprint_range"], fetched["sprint_range_summary"],
        )
        del df_summary, df_history
    log_dataframe_summary(df, "Epics Final")
    log_dataframe_summary(df_acrp, "Epics ACRP")

    with step_spinner(5, total, "Building feature burn-up"):
        df_burnup = build_burnup(fetched["burnup"])
        del fetched
    log_dataframe_summary(df_burnup, "Feature Burn-Up")

    with step_spinner(6, total, f"Exporting {hyper_path.name}"):
        export_hyper(df, hyper_path, "Epics", config)

    with step_spinner(7, total, f"Exporting {acrp_hyper_path.name}"):
        export_hyper(df_acrp, acrp_hyper_path, "Epics_ACRP", config)

    with step_spinner(8, total, f"Exporting {burnup_hyper_path.name}"):
        export_hyper(df_burnup, burnup_hyper_path, "Feature_Burnup", config)

    if publish:
        with step_spinner(9, total, "Publishing to Tableau"):
            for path, table_id in (
                (hyper_path, cfg["table_id"]),
                (acrp_hyper_path, cfg["acrp_table_id"]),
                (burnup_hyper_path, cfg["burnup_table_id"]),
            ):
                publish_hyper(path, config, targets=publish_targets, datasource_name=table_id)

    logger.info("Epics pipeline complete")
    print_pipeline_complete(PIPELINE_NAME, time.time() - start)
    return df, df_acrp, df_burnup
