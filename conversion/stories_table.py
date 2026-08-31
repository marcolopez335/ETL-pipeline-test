import time
from datetime import datetime
import polars as pl
from schemas.datatypes import EXPECTED_DTYPES_STORIES
# get_logger comes via shared so this module stays importable (and testable)
# without the proprietary `common` package, like burnup_table
from conversion.shared import (
    OUTPUT_DIR, SPRINT_VERSION_PATTERN, get_cache_path, get_logger, run_query,
    clean_dtypes, update_history, union_data, export_hyper,
    log_dataframe_summary, publish_hyper, fill_missing_snapshots,
    history_fetch_plan, parallel_fetch, rename_to_title_case,
)
from conversion.console import (
    print_header, step_spinner, print_info, print_pipeline_complete,
)

logger = get_logger(__name__)

# PI is the first 4 chars of the extracted sprint version: "26.1" from "26.1.IP"
PI_PREFIX_LENGTH = 4


def fetch_summary_full(config: dict) -> pl.DataFrame:
    cfg = config["stories"]
    return run_query(cfg["sql_summary"], database=config["database"]["name"], config=config)


def fetch_epics_full(config: dict) -> pl.DataFrame:
    cfg = config["stories"]
    return run_query(cfg["sql_epics"], database=config["database"]["name"], config=config)


def join_stories_data(stories: pl.DataFrame, epics: pl.DataFrame) -> pl.DataFrame:
    return stories.join(
        epics,
        on=["FEATURE_ID", "SNAPSHOT_DATE"],
        how="left",
        suffix="_epics",
        join_nulls=True,
    )


def drop_todays_history(df_history: pl.DataFrame) -> pl.DataFrame:
    """Drop history rows snapshotted today — the summary owns today's data.

    On snapshot days the history table already contains rows dated today.
    The summary rows (null SNAPSHOT_DATE, filled with today's date in
    apply_transforms) would then duplicate every story on the latest date,
    doubling counts in Tableau. The summary is fetched at run time, so it is
    the fresher version of today; keep it and drop the morning snapshot from
    the export. The cache is unaffected — update_history has already stored
    today's snapshot, and tomorrow's export serves today from history as
    usual.
    """
    today = datetime.now().date()
    before = df_history.height
    df_history = df_history.filter(
        pl.col("SNAPSHOT_DATE").cast(pl.Date, strict=False).ne_missing(today)
    )
    dropped = before - df_history.height
    if dropped:
        logger.info(
            f"Dropped {dropped} history rows dated {today} — summary supplies today's rows"
        )
    return df_history


def apply_transforms(df: pl.DataFrame) -> pl.DataFrame:
    """Add the computed columns and rename for Tableau (last step before export)."""
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
    # PI_FROM_SPRINT is a prefix of SPRINT_NAME_ALT — derive it instead of
    # running the regex extraction over SPRINT_NAME a second time
    df = df.with_columns(
        pl.col("SPRINT_NAME_ALT").str.slice(0, PI_PREFIX_LENGTH).alias("PI_FROM_SPRINT")
    )

    return rename_to_title_case(df)


def run_update_cache(config: dict, force: bool = False):
    cfg = config["stories"]
    cache_path = get_cache_path(cfg["cache_filename"])
    print_header("Stories Cache Update (Polars)")
    logger.info("Updating stories history cache")
    with step_spinner(1, 1, "Updating history cache"):
        update_history(
            cfg["sql_history_full"], cfg["sql_history_recent"],
            cfg["key_column"], cache_path,
            config=config, force=force,
        )
    logger.info("Stories cache update complete")


def run(config: dict, publish: bool = False, publish_targets: list[str] | None = None,
        force: bool = False) -> pl.DataFrame:
    cfg = config["stories"]
    cache_path = get_cache_path(cfg["cache_filename"])
    hyper_path = OUTPUT_DIR / cfg["hyper_filename"]
    total = 6 if publish else 5  # publishing adds one step
    start = time.time()

    print_header("Stories Pipeline (Polars)")
    logger.info("Starting stories pipeline")

    with step_spinner(1, total, "Fetching summary, history & epics (parallel)"):
        hist_sql, hist_kind = history_fetch_plan(
            cache_path, cfg["sql_history_full"], cfg["sql_history_recent"])
        db = config["database"]["name"]
        fetched = parallel_fetch({
            "summary": lambda: fetch_summary_full(config),
            "history": lambda: run_query(hist_sql, database=db, config=config),
            "epics": lambda: fetch_epics_full(config),
        }, config=config)
        df_summary = clean_dtypes(fetched["summary"], EXPECTED_DTYPES_STORIES)
    log_dataframe_summary(df_summary, "Stories Summary")

    with step_spinner(2, total, "Updating history cache"):
        df_history = update_history(
            cfg["sql_history_full"], cfg["sql_history_recent"],
            cfg["key_column"], cache_path,
            config=config, force=force,
            prefetched=fetched["history"], prefetched_kind=hist_kind,
        )
        df_history = clean_dtypes(df_history, EXPECTED_DTYPES_STORIES)
    log_dataframe_summary(df_history, "Stories History")

    with step_spinner(3, total, "Filling missing snapshots"):
        df_history = fill_missing_snapshots(df_summary, df_history, cfg["key_column"], config=config)

    with step_spinner(4, total, "Joining & transforming"):
        df_history = drop_todays_history(df_history)
        stories = union_data(df_summary, df_history)
        epics = clean_dtypes(fetched["epics"], EXPECTED_DTYPES_STORIES)
        df = join_stories_data(stories, epics)
        df = apply_transforms(df)

    log_dataframe_summary(df, "Stories Final")

    with step_spinner(5, total, "Exporting hyper"):
        export_hyper(df, hyper_path, "Stories", config)

    if publish:
        with step_spinner(6, total, "Publishing to Tableau"):
            publish_hyper(hyper_path, config, targets=publish_targets,
                          datasource_name=cfg["table_id"])

    elapsed = time.time() - start
    logger.info("Stories pipeline complete")
    print_pipeline_complete("Stories", elapsed)
    return df
