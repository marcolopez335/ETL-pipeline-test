"""Stories pipeline -- one row per story per snapshot, with its feature attached.

Data model
----------
::

    STORIES = (story history  U  story summary)
              LEFT JOIN (feature history  U  feature summary)
                   ON FEATURE_ID + SNAPSHOT_DATE

* **Story history** -- ``Ahist.sql`` seeds the parquet cache (the last year
  of snapshots); ``Ahist_recent.sql`` refreshes the last 30 days on every
  run. One row per STORY_NUMBER per SNAPSHOT_DATE.
* **Story summary** -- ``Asum.sql``, the live state of every story. Its
  SNAPSHOT_DATE is NULL until the transforms stamp it with today's date, so
  the summary owns "today" in the export: history rows dated today are
  dropped first (``shared.drop_todays_history``) to avoid double counting.
* **Feature lookup** -- ``EsumEhist.sql`` unions the same two views at the
  feature (parent) level. A snapshot story joins its feature as it was on
  that snapshot; a summary story joins the live feature (NULL matches NULL).

Steps: fetch (parallel) -> update history cache -> fill missing weekly
snapshots -> build (drop today's history, union, feature join, transforms)
-> export ``STORIES.hyper`` -> publish.

Column casing: SCREAMING_SNAKE_CASE from the database through the build,
Title Case in the .hyper output (``rename_to_title_case``).
"""

import time
from datetime import datetime

import polars as pl

from schemas.datatypes import EXPECTED_DTYPES_STORIES
# get_logger comes via shared so this module imports (and its tests run)
# without the proprietary csm_commonlib package
from conversion.shared import (
    JOIN_NULLS_KWARG, OUTPUT_DIR, SPRINT_VERSION_PATTERN, clean_dtypes,
    drop_todays_history, export_hyper, fill_missing_snapshots, get_cache_path,
    get_logger, history_fetch_plan, log_dataframe_summary, parallel_fetch,
    publish_hyper, rename_to_title_case, run_query, union_data, update_history,
)
from conversion.console import (
    print_header, print_info, print_pipeline_complete, step_spinner,
)

logger = get_logger(__name__)

PIPELINE_NAME = "Stories"
HYPER_TABLE = "Stories"

# A story joins the feature row for the same snapshot
FEATURE_JOIN_KEYS = ["FEATURE_ID", "SNAPSHOT_DATE"]
# Suffix for feature columns that collide with story columns (TARGET_START /
# TARGET_END). Historical name -- the workbooks reference "Target Start Epics".
FEATURE_SUFFIX = "_epics"
# PI is the first 4 chars of the extracted sprint version: "26.1" from "26.1.IP"
PI_PREFIX_LENGTH = 4


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_summary(config: dict) -> pl.DataFrame:
    """Live story rows (``sql_summary``), dtypes normalized."""
    cfg = config["stories"]
    df = run_query(cfg["sql_summary"], database=config["database"]["name"], config=config)
    return clean_dtypes(df, EXPECTED_DTYPES_STORIES)


def fetch_features(config: dict) -> pl.DataFrame:
    """Feature-level history + summary lookup (``sql_features``), dtypes normalized."""
    cfg = config["stories"]
    df = run_query(cfg["sql_features"], database=config["database"]["name"], config=config)
    return clean_dtypes(df, EXPECTED_DTYPES_STORIES)


# ---------------------------------------------------------------------------
# Build (pure DataFrame -> DataFrame; no database access)
# ---------------------------------------------------------------------------

def join_features(stories: pl.DataFrame, features: pl.DataFrame) -> pl.DataFrame:
    """Attach each story's feature row for the same snapshot.

    Null-equal matching lets summary stories (NULL SNAPSHOT_DATE) pick up
    the feature summary rows, which carry a NULL SNAPSHOT_DATE as well.
    The feature key is aligned to the stories' SNAPSHOT_DATE dtype and
    truncated to midnight, so a database timestamp with a time component
    still matches the day-grain history dates.
    """
    key_dtype = stories.schema["SNAPSHOT_DATE"]
    snapshot = pl.col("SNAPSHOT_DATE").cast(key_dtype, strict=False)
    if isinstance(key_dtype, pl.Datetime):
        snapshot = snapshot.dt.truncate("1d")
    features = features.with_columns(snapshot)

    return stories.join(
        features, on=FEATURE_JOIN_KEYS, how="left", suffix=FEATURE_SUFFIX,
        **JOIN_NULLS_KWARG,
    )


def apply_transforms(df: pl.DataFrame, now: datetime | None = None) -> pl.DataFrame:
    """Add the computed columns Tableau expects (SCREAMING_SNAKE_CASE in and out).

    * LAST_UPDATED -- pipeline run time (data-freshness stamp)
    * SNAPSHOT_DATE -- NULL (summary rows) filled with today's date
    * SNAPSHOT_DATE_ALT -- like SNAPSHOT_DATE, but summary rows get the exact
      run timestamp instead of midnight
    * PROJECT_NAME_VERSION -- ``"<PROJECT_NAME> <FIX_VERSION>"``
    * SPRINT_NAME_ALT -- sprint version parsed from SPRINT_NAME ("26.1.2")
    * PI_FROM_SPRINT -- the PI prefix of SPRINT_NAME_ALT ("26.1")
    """
    if now is None:
        now = datetime.now()
    local_tz = now.astimezone().tzname()
    logger.info(f"LAST_UPDATED set to {now} (timezone: {local_tz})")
    print_info(f"LAST_UPDATED: [bold]{now}[/]  [dim](timezone: {local_tz})[/]")

    df = df.with_columns([
        pl.lit(now).alias("LAST_UPDATED"),
        (pl.col("PROJECT_NAME") + " " + pl.col("FIX_VERSION")).alias("PROJECT_NAME_VERSION"),
        pl.col("SPRINT_NAME").cast(pl.Utf8).str.extract(SPRINT_VERSION_PATTERN).alias("SPRINT_NAME_ALT"),
        pl.when(pl.col("SNAPSHOT_DATE").is_null())
          .then(pl.lit(now))
          .otherwise(pl.col("SNAPSHOT_DATE"))
          .alias("SNAPSHOT_DATE_ALT"),
        pl.col("SNAPSHOT_DATE").fill_null(pl.lit(now.date())),
    ])
    # PI_FROM_SPRINT is a prefix of SPRINT_NAME_ALT -- derive it instead of
    # running the regex extraction over SPRINT_NAME a second time
    return df.with_columns(
        pl.col("SPRINT_NAME_ALT").str.slice(0, PI_PREFIX_LENGTH).alias("PI_FROM_SPRINT")
    )


def build_stories(
    df_summary: pl.DataFrame, df_history: pl.DataFrame, df_features: pl.DataFrame,
    now: datetime | None = None,
) -> pl.DataFrame:
    """summary + history + feature lookup -> the STORIES.hyper frame.

    Pure function of its inputs (``now`` is injectable), so the whole
    union / join / transform chain can be exercised on in-memory frames.
    """
    if now is None:
        now = datetime.now()
    df_history = drop_todays_history(df_history, today=now.date())
    stories = union_data(df_summary, df_history)
    df = join_features(stories, df_features)
    df = apply_transforms(df, now=now)
    return rename_to_title_case(df)


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def run_update_cache(config: dict, force: bool = False, rebuild_cache: bool = False) -> None:
    """Refresh the stories history cache only (no export)."""
    cfg = config["stories"]
    cache_path = get_cache_path(cfg["cache_filename"])
    print_header("Stories Cache Update (Polars)")
    logger.info("Updating stories history cache")
    with step_spinner(1, 1, "Updating history cache"):
        update_history(
            cfg["sql_history_full"], cfg["sql_history_recent"],
            cfg["key_column"], cache_path,
            config=config, force=force, rebuild=rebuild_cache,
        )
    logger.info("Stories cache update complete")


def run(config: dict, publish: bool = False, publish_targets: list[str] | None = None,
        force: bool = False, rebuild_cache: bool = False) -> pl.DataFrame:
    """Full stories pipeline: fetch, cache, build, export and (optionally) publish."""
    cfg = config["stories"]
    cache_path = get_cache_path(cfg["cache_filename"])
    hyper_path = OUTPUT_DIR / cfg["hyper_filename"]
    total = 6 if publish else 5  # publishing adds one step
    start = time.time()

    print_header("Stories Pipeline (Polars)")
    logger.info("Starting stories pipeline")

    with step_spinner(1, total, "Fetching summary, history & features (parallel)"):
        hist_sql, hist_kind = history_fetch_plan(
            cache_path, cfg["sql_history_full"], cfg["sql_history_recent"], rebuild=rebuild_cache)
        db = config["database"]["name"]
        fetched = parallel_fetch({
            "summary": lambda: fetch_summary(config),
            "history": lambda: run_query(hist_sql, database=db, config=config),
            "features": lambda: fetch_features(config),
        }, config=config)
    df_summary = fetched["summary"]
    log_dataframe_summary(df_summary, "Stories Summary")

    with step_spinner(2, total, "Updating history cache"):
        df_history = update_history(
            cfg["sql_history_full"], cfg["sql_history_recent"],
            cfg["key_column"], cache_path,
            config=config, force=force,
            prefetched=fetched["history"], prefetched_kind=hist_kind, rebuild=rebuild_cache,
        )
        df_history = clean_dtypes(df_history, EXPECTED_DTYPES_STORIES)
    log_dataframe_summary(df_history, "Stories History")

    with step_spinner(3, total, "Filling missing snapshots"):
        df_history = fill_missing_snapshots(df_summary, df_history, cfg["key_column"], config=config)

    with step_spinner(4, total, "Joining features & transforming"):
        df = build_stories(df_summary, df_history, fetched["features"])
        del df_summary, df_history, fetched
    log_dataframe_summary(df, "Stories Final")

    with step_spinner(5, total, f"Exporting {hyper_path.name}"):
        export_hyper(df, hyper_path, HYPER_TABLE, config)

    if publish:
        with step_spinner(6, total, "Publishing to Tableau"):
            publish_hyper(hyper_path, config, targets=publish_targets,
                          datasource_name=cfg["table_id"])

    logger.info("Stories pipeline complete")
    print_pipeline_complete(PIPELINE_NAME, time.time() - start)
    return df
