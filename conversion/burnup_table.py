"""Feature burn-up output — long-format date events per feature.

Mirrors the Alteryx burn-up workflow:

1. Rename the database LAST_UPDATED to SNAPSHOT_DATE (midnight-normalized);
   keep the raw value as IMET_SUMMARY_LAST_UPDATED. Derive SOURCE_TYPE and
   TARGET_END_REF.
2. Unpivot the three date columns (TARGET_END / RESOLVED / PLANNED_END)
   into DATE_TYPE + DATE_VALUE rows.
3. Flag DONE / PROJECTED / PLANNED features per melted row.
4. Add a fresh LAST_UPDATED = pipeline run time converted to US Central
   (DST-aware) — a data-freshness stamp, unrelated to the DB column.
5. Drop rows with no DATE_VALUE.
"""

from datetime import datetime, timezone

import polars as pl

from conversion.shared import get_logger

logger = get_logger(__name__)

SOURCE_COLUMNS = [
    "LAST_UPDATED", "TEAM_NAME", "ISSUE_KEY", "SUMMARY", "ISSUE_TYPE",
    "STATUS", "TARGET_END", "RESOLVED", "PLANNED_END", "PARENT_KEY",
]
DATE_COLS = ["TARGET_END", "RESOLVED", "PLANNED_END"]

# Status comparisons are case-insensitive
DONE_STATUSES = ["done", "accepted"]
TERMINAL_STATUSES = ["done", "accepted", "cancelled"]

# Local zone for the LAST_UPDATED freshness stamp. DST-aware: -5 (CDT) in
# summer, -6 (CST) in winter. Change to "America/New_York" for US Eastern.
LOCAL_TIMEZONE = "America/Chicago"


def _ensure_datetime(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    """Cast/parse the given columns to Datetime, whatever the source dtype.

    ODBC may deliver these as strings, Date, or Datetime depending on the
    driver — normalize before the unpivot so all melted values share a dtype.
    """
    exprs = []
    for col in cols:
        dtype = df.schema[col]
        if isinstance(dtype, pl.Datetime):
            continue
        if dtype in (pl.Utf8, pl.Null):
            exprs.append(pl.col(col).cast(pl.Utf8).str.to_datetime(strict=False).alias(col))
        else:
            exprs.append(pl.col(col).cast(pl.Datetime, strict=False).alias(col))
    if exprs:
        df = df.with_columns(exprs)
    return df


def build_burnup(df: pl.DataFrame, run_timestamp: datetime | None = None) -> pl.DataFrame:
    """Build the feature burn-up dataset from the FeatureBurnup.sql result.

    Args:
        run_timestamp: Aware UTC datetime used for the LAST_UPDATED
            freshness stamp. Defaults to now; injectable for tests.
    """
    missing = [c for c in SOURCE_COLUMNS if c not in df.columns]
    if missing:
        raise KeyError(f"Burn-up source missing columns: {missing}")

    df = df.select(SOURCE_COLUMNS)
    df = _ensure_datetime(df, ["LAST_UPDATED"] + DATE_COLS)

    # The database LAST_UPDATED becomes SNAPSHOT_DATE (midnight) and is kept
    # raw as IMET_SUMMARY_LAST_UPDATED; the original column is then dropped
    # so the name is free for the run-time freshness stamp below.
    df = df.with_columns([
        pl.lit("summary").alias("SOURCE_TYPE"),
        pl.col("LAST_UPDATED").alias("IMET_SUMMARY_LAST_UPDATED"),
        pl.col("LAST_UPDATED").dt.truncate("1d").alias("SNAPSHOT_DATE"),
        pl.col("TARGET_END").alias("TARGET_END_REF"),
    ]).drop("LAST_UPDATED")

    # Unpivot: one row per (feature, date column). DATE_TYPE carries the
    # source column name; DATE_VALUE the date, normalized to midnight.
    id_cols = [c for c in df.columns if c not in DATE_COLS]
    df = df.unpivot(
        on=DATE_COLS, index=id_cols,
        variable_name="DATE_TYPE", value_name="DATE_VALUE",
    )
    df = df.with_columns(pl.col("DATE_VALUE").dt.truncate("1d"))

    status_lc = pl.col("STATUS").cast(pl.Utf8).str.to_lowercase().str.strip_chars()
    has_date = pl.col("DATE_VALUE").is_not_null()

    df = df.with_columns([
        # Done: resolved date present and the feature is finished
        pl.when(
            (pl.col("DATE_TYPE") == "RESOLVED") & has_date
            & status_lc.is_in(DONE_STATUSES)
        ).then(pl.col("ISSUE_KEY")).alias("DONE_FEATURES"),
        # Projected: open work (not done/accepted/cancelled) by target end
        pl.when(
            (pl.col("DATE_TYPE") == "TARGET_END") & has_date
            & ~status_lc.is_in(TERMINAL_STATUSES)
        ).then(pl.col("ISSUE_KEY")).alias("PROJECTED_FEATURES"),
        # Planned: any feature with a planned end, regardless of status
        pl.when(
            (pl.col("DATE_TYPE") == "PLANNED_END") & has_date
        ).then(pl.col("ISSUE_KEY")).alias("PLANNED_FEATURES"),
    ])

    # LAST_UPDATED freshness stamp: when this pipeline run produced the data,
    # converted UTC -> US Central (DST-aware) and stored as a naive local
    # datetime. Uses Polars' bundled tz database (no system tzdata needed).
    if run_timestamp is None:
        run_timestamp = datetime.now(timezone.utc)
    now_utc_naive = run_timestamp.astimezone(timezone.utc).replace(tzinfo=None)
    df = df.with_columns(
        pl.lit(now_utc_naive, dtype=pl.Datetime)
        .dt.replace_time_zone("UTC")
        .dt.convert_time_zone(LOCAL_TIMEZONE)
        .dt.replace_time_zone(None)
        .alias("LAST_UPDATED")
    )

    before = df.height
    df = df.filter(pl.col("DATE_VALUE").is_not_null())

    logger.info(
        f"Burn-up: {df.height} rows ({before - df.height} null-date rows dropped), "
        f"done={df['DONE_FEATURES'].is_not_null().sum()} "
        f"projected={df['PROJECTED_FEATURES'].is_not_null().sum()} "
        f"planned={df['PLANNED_FEATURES'].is_not_null().sum()}"
    )
    return df
