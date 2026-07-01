"""Feature burn-up output — long-format date events per feature.

Mirrors the Alteryx burn-up workflow:

1. Select the summary fields and derive SOURCE_TYPE / SNAPSHOT_DATE /
   IMET_SUMMARY_LAST_UPDATED / TARGET_END_REF.
2. Unpivot the three date columns (TARGET_END / RESOLVED / PLANNED_END)
   into DATE_TYPE + DATE_VALUE rows.
3. Flag DONE / PROJECTED / PLANNED features per melted row.
4. Convert LAST_UPDATED from UTC to US Eastern (DST-aware).
5. Drop rows with no DATE_VALUE.
"""

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

LOCAL_TIMEZONE = "America/New_York"


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


def build_burnup(df: pl.DataFrame) -> pl.DataFrame:
    """Build the feature burn-up dataset from the FeatureBurnup.sql result."""
    missing = [c for c in SOURCE_COLUMNS if c not in df.columns]
    if missing:
        raise KeyError(f"Burn-up source missing columns: {missing}")

    df = df.select(SOURCE_COLUMNS)
    df = _ensure_datetime(df, ["LAST_UPDATED"] + DATE_COLS)

    # Derived columns — computed from the ORIGINAL (unshifted) LAST_UPDATED
    df = df.with_columns([
        pl.lit("summary").alias("SOURCE_TYPE"),
        pl.col("LAST_UPDATED").alias("IMET_SUMMARY_LAST_UPDATED"),
        pl.col("LAST_UPDATED").dt.truncate("1d").alias("SNAPSHOT_DATE"),
        pl.col("TARGET_END").alias("TARGET_END_REF"),
    ])

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

    # LAST_UPDATED: source timestamps are UTC — convert to US Eastern
    # (DST-aware: -4 in summer, -5 in winter), then drop the tz marker
    # so the hyper column stays a naive local datetime.
    df = df.with_columns(
        pl.col("LAST_UPDATED")
        .dt.replace_time_zone("UTC")
        .dt.convert_time_zone(LOCAL_TIMEZONE)
        .dt.replace_time_zone(None)
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
