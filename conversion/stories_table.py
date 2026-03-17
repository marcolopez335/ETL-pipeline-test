import time
from datetime import datetime
import polars as pl
from pathlib import Path
from common.logging import get_logger, setup_logging, INFO
from schemas.datatypes import EXPECTED_DTYPES_STORIES
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

TOTAL_STEPS_BASE = 6
TOTAL_STEPS_PUBLISH = 7


def fetch_summary_full(config: dict) -> pl.DataFrame:
    cfg = config["stories"]
    return run_query(cfg["sql_summary"], database=config["database"]["name"])


def fetch_epics_full(config: dict) -> pl.DataFrame:
    cfg = config["stories"]
    return run_query(cfg["sql_epics"], database=config["database"]["name"])


def join_stories_data(stories: pl.DataFrame, epics: pl.DataFrame) -> pl.DataFrame:
    return stories.join(
        epics,
        on=["FEATURE_ID", "SNAPSHOT_DATE"],
        how="left",
        suffix="_epics",
    )


def data_functions(df: pl.DataFrame) -> pl.DataFrame:
    now = datetime.now()
    local_tz = now.astimezone().tzname()
    logger.info(f"LAST_UPDATED set to {now} (timezone: {local_tz})")
    print_info(f"LAST_UPDATED: [bold]{now}[/]  [dim](timezone: {local_tz})[/]")

    df = df.with_columns([
        pl.lit(now).alias("LAST_UPDATED"),
        (pl.col("PROJECT_NAME") + " " + pl.col("FIX_VERSION")).alias("PROJECT_NAME_VERSION"),
        pl.col("SPRINT_NAME").cast(pl.Utf8).str.extract(r"(\d{2}\.\d.\w+)").alias("SPRINT_NAME_ALT"),
        pl.when(pl.col("SNAPSHOT_DATE").is_null())
          .then(pl.lit(now))
          .otherwise(pl.col("SNAPSHOT_DATE"))
          .alias("SNAPSHOT_DATE_ALT"),
        pl.col("SPRINT_NAME").cast(pl.Utf8).str.extract(r"(\d{2}\.\d.\w+)").str.slice(0, 4).alias("PI_FROM_SPRINT"),
    ])

    # Rename columns: SNAKE_CASE -> Title Case
    rename_map = {
        col: col.lower().replace("_", " ").title()
        for col in df.columns
    }
    df = df.rename(rename_map)

    return df


def run_update_cache(config: dict):
    cfg = config["stories"]
    cache_path = CACHE_DIR / cfg["cache_filename"]
    print_header("Stories Cache Update (Polars)")
    logger.info("Updating stories history cache")
    with step_spinner(1, 1, "Updating history cache"):
        update_history(
            cfg["sql_history_full"], cfg["sql_history_recent"],
            cfg["key_column"], cache_path,
        )
    logger.info("Stories cache update complete")


def run(config: dict, publish: bool = False) -> pl.DataFrame:
    cfg = config["stories"]
    cache_path = CACHE_DIR / cfg["cache_filename"]
    hyper_path = OUTPUT_DIR / cfg["hyper_filename"]
    total = TOTAL_STEPS_PUBLISH if publish else TOTAL_STEPS_BASE
    start = time.time()

    print_header("Stories Pipeline (Polars)")
    logger.info("Starting stories pipeline")

    with step_spinner(1, total, "Fetching summary"):
        df_summary = fetch_summary_full(config)
        df_summary = clean_dtypes(df_summary, EXPECTED_DTYPES_STORIES)
    log_dataframe_summary(df_summary, "Stories Summary")

    with step_spinner(2, total, "Updating history cache"):
        df_history = update_history(
            cfg["sql_history_full"], cfg["sql_history_recent"],
            cfg["key_column"], cache_path,
        )
        df_history = clean_dtypes(df_history, EXPECTED_DTYPES_STORIES)
    log_dataframe_summary(df_history, "Stories History")

    with step_spinner(3, total, "Filling missing snapshots"):
        df_history = fill_missing_snapshots(df_summary, df_history, cfg["key_column"])

    with step_spinner(4, total, "Fetching epics"):
        stories = union_data(df_summary, df_history)
        epics = fetch_epics_full(config)
        epics = clean_dtypes(epics, EXPECTED_DTYPES_STORIES)

    with step_spinner(5, total, "Joining & transforming"):
        df = join_stories_data(stories, epics)
        df = data_functions(df)

    log_dataframe_summary(df, "Stories Final")

    with step_spinner(6, total, "Exporting hyper"):
        export_hyper(df, hyper_path, "Stories", config)

    if publish:
        with step_spinner(7, total, "Publishing to Tableau"):
            publish_hyper(hyper_path, "Stories", config)

    elapsed = time.time() - start
    logger.info("Stories pipeline complete")
    print_pipeline_complete("Stories", elapsed)
    return df
