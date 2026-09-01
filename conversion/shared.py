import inspect
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
import yaml

# csm_commonlib.* is imported lazily, inside the functions that need it, so
# this module can be imported -- and its pure-Polars helpers tested -- without
# the proprietary `csm_commonlib` package. get_logger falls back to stdlib
# logging for the same reason; the pipeline modules import it from here.
try:
    from csm_commonlib.logging import get_logger
except ImportError:  # pragma: no cover — fallback for unit-test environments
    import logging
    def get_logger(name: str) -> logging.Logger:
        logger = logging.getLogger(name)
        if not logger.handlers:
            logging.basicConfig(level=logging.INFO)
        return logger

from conversion.console import (
    print_polars_summary, print_info, print_success, print_error,
)

ROOT_DIR = Path(__file__).resolve().parent.parent

# Polars renamed DataFrame.join(join_nulls=...) to nulls_equal= in 1.24.
# Resolved once so null-matching joins work on either side of the rename.
JOIN_NULLS_KWARG = (
    {"nulls_equal": True}
    if "nulls_equal" in inspect.signature(pl.DataFrame.join).parameters
    else {"join_nulls": True}
)

# Default locations — override via config.yaml `paths:` section.
SQL_DIR = ROOT_DIR / "sql"
CACHE_DIR = ROOT_DIR / "cache"
BACKUP_DIR = ROOT_DIR / "backups"
OUTPUT_DIR = ROOT_DIR / "output"

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Config & paths
# ---------------------------------------------------------------------------

def load_config() -> dict:
    """Load config.yaml and apply path overrides to module-level dirs.

    Path keys in ``paths:`` are resolved relative to the project root.
    Reassigning the module globals keeps the public ``CACHE_DIR`` /
    ``OUTPUT_DIR`` / ``BACKUP_DIR`` / ``SQL_DIR`` constants consistent
    with what callers see after ``load_config()`` runs.
    """
    config_path = ROOT_DIR / "config.yaml"
    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    paths = cfg.get("paths") or {}
    global SQL_DIR, CACHE_DIR, BACKUP_DIR, OUTPUT_DIR
    if "sql_dir" in paths:
        SQL_DIR = ROOT_DIR / paths["sql_dir"]
    if "cache_dir" in paths:
        CACHE_DIR = ROOT_DIR / paths["cache_dir"]
    if "backup_dir" in paths:
        BACKUP_DIR = ROOT_DIR / paths["backup_dir"]
    if "output_dir" in paths:
        OUTPUT_DIR = ROOT_DIR / paths["output_dir"]

    return cfg


def get_cache_path(filename: str) -> Path:
    """Return the absolute path for a cache file under the configured CACHE_DIR."""
    return CACHE_DIR / filename


def get_output_path(filename: str) -> Path:
    """Return the absolute path for an output file under the configured OUTPUT_DIR."""
    return OUTPUT_DIR / filename


# ---------------------------------------------------------------------------
# Database access & SQL loading
# ---------------------------------------------------------------------------

def _use_stored_credentials(config: dict | None) -> bool:
    if not config:
        return True
    return bool(config.get("database", {}).get("use_stored_credentials", True))


def _configure_result_encoding(conn, config: dict | None) -> None:
    """Tell the ODBC connection how to decode result text.

    Tibco CHAR/VARCHAR columns come back as cp1252/Latin-1, but pyodbc
    defaults to strict UTF-8. Pure-ASCII data decodes identically under
    both, so it works until a non-ASCII byte (e.g. 0xa0 non-breaking space
    pasted into a Jira field) appears — then fetchall() raises
    UnicodeDecodeError. Configuring setdecoding fixes it for all values.

    Safe no-op when the underlying connection isn't exposed, isn't a pyodbc
    connection, or pyodbc isn't importable (e.g. unit-test environments).
    Override the encoding via ``database.result_encoding`` in config.yaml.
    """
    encoding = (config or {}).get("database", {}).get("result_encoding", "cp1252")

    raw = getattr(conn, "connection", None)
    if raw is None:
        logger.info("Connection not exposed for decoding config; using driver default")
        return
    setdecoding = getattr(raw, "setdecoding", None)
    if setdecoding is None:
        return  # not a pyodbc connection — nothing to configure

    try:
        import pyodbc
        setdecoding(pyodbc.SQL_CHAR, encoding=encoding)
        setdecoding(pyodbc.SQL_WCHAR, encoding=encoding)
        logger.info(f"Result text decoding set to '{encoding}'")
    except Exception as exc:
        logger.warning(f"Could not configure result decoding ({encoding}): {exc}")


def parallel_fetch(jobs: dict, config: dict | None = None) -> dict:
    """Run independent fetch callables concurrently and return results by key.

    Each callable opens its own ODBC connection, so jobs are fully
    independent. ODBC drivers release the GIL during fetch, so threads
    give real overlap on the network/DB wait.

    Set ``database.parallel_fetch: false`` in config.yaml to force
    sequential fetching (e.g. if the DB limits concurrent sessions);
    ``database.max_parallel_queries`` caps the worker count (default 4).
    """
    db_cfg = (config or {}).get("database", {})
    if not db_cfg.get("parallel_fetch", True) or len(jobs) <= 1:
        return {key: fn() for key, fn in jobs.items()}

    max_workers = min(int(db_cfg.get("max_parallel_queries", 4)), len(jobs))
    logger.info(f"Fetching {len(jobs)} queries in parallel ({max_workers} workers)")
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {key: pool.submit(fn) for key, fn in jobs.items()}
        return {key: fut.result() for key, fut in futures.items()}


def _decode_sql_text(raw: bytes, label: str = "SQL") -> str:
    """Decode SQL bytes, tolerating Windows-edited files.

    SQL files edited on Windows (paste from a browser/Excel/Word) often
    pick up a raw 0xa0 non-breaking space or other cp1252 bytes, which
    break a strict UTF-8 read. Strip a UTF-8 BOM, fall back to cp1252 on
    a decode error, and normalize non-breaking spaces to plain spaces so
    the query parses regardless of how it was saved.
    """
    if raw.startswith(b"\xef\xbb\xbf"):  # UTF-8 BOM
        raw = raw[3:]
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        logger.warning(
            f"{label}: not valid UTF-8 ({exc}); decoding as cp1252 and "
            f"normalizing non-breaking spaces"
        )
        text = raw.decode("cp1252")
    # Normalize non-breaking spaces (U+00A0) to regular spaces — in SQL they
    # are almost always an accidental whitespace char that should be a space.
    return text.replace("\u00a0", " ")


def load_sql(filename: str) -> str:
    sql_path = SQL_DIR / filename
    if not sql_path.is_file():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    return _decode_sql_text(sql_path.read_bytes(), label=filename)


def run_query(
    sql_filename: str, database: str = "default", verbose: bool = True,
    config: dict | None = None,
) -> pl.DataFrame:
    from csm_commonlib.database.tibco import TibcoConnection
    logger.info(f"Running query: {sql_filename}")
    query = load_sql(sql_filename)
    conn = TibcoConnection()
    conn.connect(database=database, use_stored_credentials=_use_stored_credentials(config))
    # Decode Latin-1/cp1252 result text (e.g. non-breaking spaces in Jira
    # fields) so fetchall() doesn't crash on the driver's strict UTF-8 default
    _configure_result_encoding(conn, config)

    try:
        pdf = conn.execute_query(query, verbose=verbose)
        df = pl.from_pandas(pdf)
        # Free the pandas copy immediately — it can be as large as the Polars one
        del pdf
        # Normalize dtypes at the source:
        # - Null-typed columns (all-null from pandas) -> Utf8
        # - Datetime("ns") (pandas datetime64[ns]) -> Datetime("us"), matching
        #   what Polars writes to parquet, so cache vs fresh never unit-clash
        casts = []
        for col, dtype in df.schema.items():
            if dtype == pl.Null:
                casts.append(pl.col(col).cast(pl.Utf8))
            elif isinstance(dtype, pl.Datetime) and dtype.time_unit == "ns":
                casts.append(pl.col(col).cast(pl.Datetime("us", dtype.time_zone)))
        if casts:
            df = df.with_columns(casts)
        logger.info(f"Query returned {df.height} rows")
    except Exception as exc:
        logger.error(f"Failed to execute {sql_filename}: {exc}")
        raise
    finally:
        conn.close()

    return df


def test_connection(database: str = "default", config: dict | None = None) -> bool:
    from csm_commonlib.database.tibco import TibcoConnection
    logger.info("Testing database connection")
    conn = TibcoConnection()
    try:
        conn.connect(database=database, use_stored_credentials=_use_stored_credentials(config))
        logger.info("Connection successful")
        print_success(f"Connected to database: [bold]{database}[/]")
        return True
    except Exception as exc:
        logger.error(f"Connection failed: {exc}")
        print_error(f"Connection failed: {exc}")
        return False
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Dtypes & schema alignment
# ---------------------------------------------------------------------------

def clean_dtypes(df: pl.DataFrame, schema: dict) -> pl.DataFrame:
    """Cast columns to the dtypes declared in a ``schemas.datatypes`` mapping.

    ``schema`` maps column name -> ``"datetime" | "date" | "float" | "string"``.
    Columns missing from ``df`` are skipped, so one mapping can describe every
    query a pipeline runs. Casts are non-strict: values that cannot be
    converted become null rather than raising.

    The ODBC driver normally returns real datetimes; a string-typed date
    column is parsed with format inference (the String -> Datetime cast is
    deprecated in Polars and only accepts ISO "T" timestamps anyway).
    """
    casts = []
    for col, dtype in schema.items():
        if col not in df.columns:
            continue
        expr = pl.col(col)

        if dtype in ("datetime", "date"):
            if df.schema[col] == pl.Utf8:
                expr = expr.str.to_datetime(strict=False)
            target = pl.Datetime("us") if dtype == "datetime" else pl.Date
            casts.append(expr.cast(target, strict=False))
        elif dtype == "float":
            casts.append(expr.cast(pl.Float64, strict=False))
        elif dtype == "string":
            casts.append(expr.cast(pl.Utf8, strict=False).str.strip_chars())
        else:
            raise ValueError(
                f"Unknown dtype '{dtype}' for column {col} -- "
                f"expected datetime, date, float or string"
            )

    if casts:
        df = df.with_columns(casts)

    return df


def _safe_dtype(dtype: pl.DataType) -> pl.DataType:
    """Return Utf8 if dtype is Null, otherwise return as-is."""
    return pl.Utf8 if dtype == pl.Null else dtype


def _supertype(a: pl.DataType, b: pl.DataType) -> pl.DataType:
    """Pick a target dtype that both ``a`` and ``b`` can be cast to losslessly.

    Rules (in order):
      1. Identical dtypes → return as-is.
      2. Either is Null → use the other.
      3. Both numeric → Float64 (covers Int / Float mix).
      4. Both temporal → Datetime("us") (covers Date / Datetime and us/ns mixes).
      5. Otherwise → Utf8 (last-resort string fallback).
    """
    if a == b:
        return a
    if a == pl.Null:
        return b
    if b == pl.Null:
        return a
    if a.is_numeric() and b.is_numeric():
        return pl.Float64
    # Date/Datetime mixes — including Datetime unit mismatches (us vs ns) —
    # unify on microseconds. Must be a CONCRETE dtype: the bare pl.Datetime
    # class compares equal to instances of ANY unit, which makes the
    # "already the target type?" check in _align_schemas skip needed casts.
    if isinstance(a, (pl.Date, pl.Datetime)) and isinstance(b, (pl.Date, pl.Datetime)):
        return pl.Datetime("us")
    if a.is_temporal() and b.is_temporal():
        return pl.Datetime("us")
    return pl.Utf8


def _align_schemas(df1: pl.DataFrame, df2: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Align two DataFrames to the same columns and types before concat."""
    all_cols = dict.fromkeys(df1.columns + df2.columns)

    df1_casts: list[pl.Expr] = []
    df2_casts: list[pl.Expr] = []

    for col in all_cols:
        in_df1 = col in df1.columns
        in_df2 = col in df2.columns

        if in_df1 and in_df2:
            target = _supertype(df1[col].dtype, df2[col].dtype)
            if df1[col].dtype != target:
                df1_casts.append(pl.col(col).cast(target, strict=False))
            if df2[col].dtype != target:
                df2_casts.append(pl.col(col).cast(target, strict=False))
        elif in_df2:
            df1_casts.append(pl.lit(None).cast(_safe_dtype(df2[col].dtype)).alias(col))
        else:
            df2_casts.append(pl.lit(None).cast(_safe_dtype(df1[col].dtype)).alias(col))

    if df1_casts:
        df1 = df1.with_columns(df1_casts)
    if df2_casts:
        df2 = df2.with_columns(df2_casts)

    # Ensure same column order
    df2 = df2.select(df1.columns)
    return df1, df2


# ---------------------------------------------------------------------------
# Console summaries
# ---------------------------------------------------------------------------

# Full per-column stats (null %, n_unique, min/max) are expensive at 4M+ rows
# and run once per pipeline step. Off by default; enabled via --verbose.
_VERBOSE_SUMMARIES = False


def set_verbose_summaries(verbose: bool) -> None:
    global _VERBOSE_SUMMARIES
    _VERBOSE_SUMMARIES = bool(verbose)


def log_dataframe_summary(df: pl.DataFrame, label: str) -> None:
    total_mem = df.estimated_size()
    logger.info(f"--- {label} Summary ---")
    logger.info(f"  Rows: {df.height}  Columns: {df.width}  Memory: {total_mem:,} bytes")

    if not _VERBOSE_SUMMARIES:
        print_info(
            f"[bold]{label}[/]: {df.height:,} rows x {df.width} cols  "
            f"[dim]({total_mem / 1024 ** 2:.1f} MB)[/]"
        )
        return

    # Compute stats once and share with both logger and console display
    null_counts = df.null_count()
    try:
        n_unique = df.select(pl.all().n_unique())
    except Exception:
        n_unique = None

    total_nulls = 0
    for col in df.columns:
        null_count = null_counts[col][0]
        total_nulls += null_count
        null_pct = (null_count / df.height * 100) if df.height > 0 else 0.0
        if n_unique is not None:
            unique_count = n_unique[col][0]
        else:
            try:
                unique_count = df[col].n_unique()
            except Exception:
                unique_count = -1
        unique_str = str(unique_count) if unique_count >= 0 else "n/a"
        logger.info(f"    {col:<30} {str(df[col].dtype):<20} nulls: {null_count} ({null_pct:.1f}%)  uniques: {unique_str}")
    total_cells = df.height * df.width
    total_null_pct = (total_nulls / total_cells * 100) if total_cells > 0 else 0.0
    logger.info(f"  Total null %: {total_null_pct:.1f}%")

    # Pass pre-computed stats so print_polars_summary doesn't recompute n_unique
    print_polars_summary(df, label, null_counts=null_counts, n_unique=n_unique)


# ---------------------------------------------------------------------------
# Backups
# ---------------------------------------------------------------------------

def _backup_with_rotation(file_path: Path, keep: int) -> str:
    """Copy ``file_path`` into BACKUP_DIR with a timestamp suffix, pruning the
    oldest copies beyond ``keep``. Returns the backup filename."""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"{file_path.stem}_{timestamp}{file_path.suffix}"
    shutil.copy2(file_path, BACKUP_DIR / backup_name)
    logger.info(f"Backed up {file_path.name} -> {backup_name}")

    existing = sorted(
        BACKUP_DIR.glob(f"{file_path.stem}_*{file_path.suffix}"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for old in existing[keep:]:
        old.unlink()
        logger.info(f"Removed old backup: {old.name}")
    return backup_name


def backup_file(file_path: Path, config: dict) -> None:
    """Back up an output file before overwrite, honoring the ``backup:`` config."""
    backup_cfg = config.get("backup", {})
    if not backup_cfg.get("enabled", True) or not file_path.exists():
        return
    backup_name = _backup_with_rotation(file_path, keep=backup_cfg.get("max_backups", 5))
    print_info(f"Backup: [dim]{backup_name}[/]")


# ---------------------------------------------------------------------------
# History cache
# ---------------------------------------------------------------------------

def read_history_cache(cache_path: Path) -> pl.LazyFrame | None:
    if not cache_path.exists():
        logger.info(f"No cache found at {cache_path}")
        return None

    lf = pl.scan_parquet(cache_path)
    logger.info(f"Lazy-scanning cache: {cache_path}")
    return lf


def write_history_cache(df: pl.DataFrame, cache_path: Path, config: dict | None = None) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    # Back up the existing cache before overwrite, honoring the ``cache:`` config
    cache_cfg = (config or {}).get("cache", {})
    if cache_cfg.get("backup_enabled", False) and cache_path.exists():
        _backup_with_rotation(cache_path, keep=cache_cfg.get("max_cache_backups", 3))
    df.write_parquet(cache_path)
    logger.info(f"Wrote {df.height} rows to cache: {cache_path}")


def validate_history(df: pl.DataFrame, key_col: str) -> pl.DataFrame:
    """Check required history columns and normalize SNAPSHOT_DATE to Date."""
    required = {key_col, "SNAPSHOT_DATE"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"Table missing columns: {missing}")

    return df.with_columns(pl.col("SNAPSHOT_DATE").cast(pl.Date, strict=False))


def fetch_history(
    sql_filename: str, key_col: str,
    database: str = "default", config: dict | None = None,
) -> pl.DataFrame:
    df = run_query(sql_filename, database=database, config=config)
    return validate_history(df, key_col)


def history_fetch_plan(cache_path: Path, sql_full: str, sql_recent: str) -> tuple[str, str]:
    """Choose which history SQL to run based on cache presence.

    Returns (sql_filename, kind) where kind is 'full' or 'recent'. Lets
    pipelines prefetch the history query in parallel with other queries
    and hand the result to update_history() afterwards.
    """
    if cache_path.exists():
        return sql_recent, "recent"
    return sql_full, "full"


def update_history_cache_with_recent(
    cached_lf: pl.LazyFrame, recent: pl.DataFrame, key_col: str,
    config: dict | None = None,
) -> pl.DataFrame:
    key_cols = [key_col, "SNAPSHOT_DATE"]

    # Cast SNAPSHOT_DATE to Date (not Datetime) — snapshots are daily, and
    # Date avoids precision mismatches (us vs ns) between cache and fresh query
    # that silently break the anti-join dedup
    cached_lf = cached_lf.with_columns([
        pl.col("SNAPSHOT_DATE").cast(pl.Date, strict=False),
        pl.col(key_col).cast(pl.Utf8, strict=False),
    ])
    recent = recent.with_columns([
        pl.col("SNAPSHOT_DATE").cast(pl.Date, strict=False),
        pl.col(key_col).cast(pl.Utf8, strict=False),
    ])

    # Get cached row count cheaply before we consume the lazy frame
    # (only reads parquet metadata + key columns, not all data)
    cached_count = cached_lf.select(pl.len()).collect().item()

    # Anti-join lazily — Polars only scans the parquet rows it needs
    recent_keys = recent.lazy().select(key_cols).unique()
    cached_keep = cached_lf.join(recent_keys, on=key_cols, how="anti").collect()

    removed = cached_count - cached_keep.height
    logger.info(
        f"Cache dedup: {cached_count} cached, {removed} replaced by {recent.height} recent "
        f"-> {cached_keep.height} kept"
    )

    # Align schemas before concat — cached from parquet may differ from fresh query
    cached_keep, recent = _align_schemas(cached_keep, recent)

    # Concat and free the inputs immediately to avoid holding 2x in memory
    combined = pl.concat([cached_keep, recent])
    del cached_keep, recent

    combined = combined.drop_nulls(subset=key_cols).unique(subset=key_cols, keep="last")

    min_retention = 0.98
    if config is not None:
        min_retention = config.get("cache", {}).get("min_retention_pct", 0.98)
    if combined.height < min_retention * cached_count:
        raise RuntimeError(
            f"Cache shrank unexpectedly: {combined.height} rows vs {cached_count} prior "
            f"(threshold: {min_retention:.0%}). Use --force to override."
        )

    logger.info(f"Cache updated: {cached_count} -> {combined.height} rows")
    return combined


def build_and_cache_history(sql_full: str, key_col: str, cache_path: Path, config: dict | None = None) -> pl.DataFrame:
    logger.info("Building full history cache")
    db = (config or {}).get("database", {}).get("name", "default")
    df = fetch_history(sql_full, key_col, database=db, config=config)
    write_history_cache(df, cache_path, config=config)
    return df


def update_history(
    sql_full: str, sql_recent: str, key_col: str, cache_path: Path,
    config: dict | None = None, force: bool = False,
    prefetched: pl.DataFrame | None = None, prefetched_kind: str | None = None,
) -> pl.DataFrame:
    """Merge fresh history into the cache.

    ``prefetched``/``prefetched_kind`` let a pipeline fetch the history
    query concurrently with its other queries (see history_fetch_plan)
    and pass the result in, avoiding a second sequential fetch here.
    """
    cached_lf = read_history_cache(cache_path)

    if cached_lf is None:
        if prefetched is not None and prefetched_kind == "full":
            df = validate_history(prefetched, key_col)
            write_history_cache(df, cache_path, config=config)
            return df
        # No cache and no full prefetch — fall back to a full fetch
        return build_and_cache_history(sql_full, key_col, cache_path, config=config)

    if prefetched is not None:
        recent = validate_history(prefetched, key_col)
    else:
        db = (config or {}).get("database", {}).get("name", "default")
        recent = fetch_history(sql_recent, key_col, database=db, config=config)

    if force:
        # Skip shrinkage check when --force is used
        cfg_override = dict(config) if config else {}
        cfg_override["cache"] = {**cfg_override.get("cache", {}), "min_retention_pct": 0.0}
        config = cfg_override

    updated = update_history_cache_with_recent(cached_lf, recent, key_col, config=config)
    write_history_cache(updated, cache_path, config=config)
    return updated


# ---------------------------------------------------------------------------
# Snapshot filling & unioning
# ---------------------------------------------------------------------------

def get_last_n_snapshots(n: int, day_of_week: int = 0, from_date: datetime | None = None) -> list[datetime]:
    """Get the last n snapshot days up to and including the most recent one.

    Args:
        n: Number of past snapshot days to return.
        day_of_week: 0=Monday, 1=Tuesday, ..., 6=Sunday.
        from_date: Reference date (defaults to now).
    """
    if from_date is None:
        from_date = datetime.now()
    days_since = (from_date.weekday() - day_of_week) % 7
    most_recent = from_date - timedelta(days=days_since)
    most_recent = most_recent.replace(hour=0, minute=0, second=0, microsecond=0)
    return [most_recent - timedelta(weeks=i) for i in range(n)]


def fill_missing_snapshots(
    df_summary: pl.DataFrame,
    df_history: pl.DataFrame,
    key_col: str,
    config: dict | None = None,
) -> pl.DataFrame:
    """Fill missing weekly snapshots in history using summary data.

    Checks the last ``snapshots.lookback_weeks`` snapshot days (the weekday
    set by ``snapshots.day_of_week``). For any snapshot day missing from
    df_history, creates a synthetic snapshot from df_summary with
    SNAPSHOT_DATE set to that day and IS_SYNTHETIC = True.

    When the database later provides the real snapshot, the cache update's
    anti-join will replace the synthetic row automatically.
    """
    snap_cfg = (config or {}).get("snapshots", {})
    day_of_week = snap_cfg.get("day_of_week", 0)
    n_weeks = snap_cfg.get("lookback_weeks", 4)

    expected_days = get_last_n_snapshots(n_weeks, day_of_week=day_of_week)

    # Get existing snapshot dates from history
    existing_dates = set()
    if "SNAPSHOT_DATE" in df_history.columns and df_history.height > 0:
        dates = df_history.select(
            pl.col("SNAPSHOT_DATE").cast(pl.Date)
        ).unique().to_series().to_list()
        existing_dates = {d for d in dates if d is not None}

    # Find missing snapshot days — exclude today because the summary already
    # represents current-day data; synthesizing today duplicates it
    today = datetime.now().date()
    missing_days = [d for d in expected_days
                    if d.date() not in existing_dates and d.date() != today]

    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    day_name = day_names[day_of_week]

    if not missing_days:
        logger.info(f"No missing snapshots in the last {n_weeks} {day_name}s")
        return df_history

    logger.info(f"Found {len(missing_days)} missing {day_name} snapshot(s)")
    for d in sorted(missing_days):
        logger.info(f"  Missing: {d.strftime('%Y-%m-%d')} ({day_name})")

    # Build synthetic snapshots from summary data
    summary_cols = [c for c in df_summary.columns if c not in ("SNAPSHOT_DATE", "IS_SYNTHETIC")]
    base = df_summary.select(summary_cols)

    synthetic_frames = []
    for snapshot_day in sorted(missing_days):
        snapshot = base.with_columns([
            pl.lit(snapshot_day.date()).alias("SNAPSHOT_DATE"),
            pl.lit(True).alias("IS_SYNTHETIC"),
        ])
        synthetic_frames.append(snapshot)
        logger.info(f"  Synthesized {snapshot.height} rows for {snapshot_day.strftime('%Y-%m-%d')}")

    synthetic = pl.concat(synthetic_frames)

    # Add IS_SYNTHETIC=False to history if column doesn't exist
    if "IS_SYNTHETIC" not in df_history.columns:
        df_history = df_history.with_columns(pl.lit(False).alias("IS_SYNTHETIC"))

    # Align schemas and concat
    df_history, synthetic = _align_schemas(df_history, synthetic)
    combined = pl.concat([df_history, synthetic])
    logger.info(f"History: {df_history.height} -> {combined.height} rows (+{synthetic.height} synthetic)")

    return combined


def union_data(df_summary: pl.DataFrame, df_history: pl.DataFrame) -> pl.DataFrame:
    """Stack the live summary (NULL SNAPSHOT_DATE) on top of history, deduplicated.

    Column order follows the summary; columns only one side has are added
    as nulls to the other (see ``_align_schemas``).
    """
    df_summary, df_history = _align_schemas(df_summary, df_history)
    unioned = pl.concat([df_summary, df_history])
    return unioned.unique()


def drop_todays_history(df_history: pl.DataFrame, today: date | None = None) -> pl.DataFrame:
    """Drop history rows snapshotted today -- the summary owns today's data.

    On snapshot days the history table already contains rows dated today.
    The summary rows (NULL SNAPSHOT_DATE, stamped with today's date by the
    pipeline's transforms) would then duplicate every key on the latest
    date, doubling counts in Tableau. The summary is fetched at run time,
    so it is the fresher version of today: keep it and drop the morning
    snapshot from the export.

    The cache is unaffected: update_history has already stored today's
    snapshot, and tomorrow's export serves today from history as usual.
    NULL snapshot dates are never dropped (ne_missing).
    """
    if today is None:
        today = datetime.now().date()
    before = df_history.height
    kept = df_history.filter(
        pl.col("SNAPSHOT_DATE").cast(pl.Date, strict=False).ne_missing(today)
    )
    dropped = before - kept.height
    if dropped:
        logger.info(f"Dropped {dropped} history rows dated {today} -- summary supplies today's rows")
    return kept


# ---------------------------------------------------------------------------
# Column naming
# ---------------------------------------------------------------------------

# Extracts a short sprint version like "26.1.2" or "26.1.IP" from a sprint
# name like "AMMM 26.1.IP". Note the second dot is unescaped (matches any
# character) — kept as-is to preserve the historical match behavior.
SPRINT_VERSION_PATTERN = r"(\d{2}\.\d.\w+)"


def rename_to_title_case(df: pl.DataFrame) -> pl.DataFrame:
    """Rename SCREAMING_SNAKE_CASE columns to Title Case for Tableau.

    e.g. ``FEATURE_KEY`` -> ``Feature Key``. Applied as the last transform
    before export so the .hyper files show friendly column names.
    """
    return df.rename({col: col.lower().replace("_", " ").title() for col in df.columns})


def rename_to_snake_case(df: pl.DataFrame) -> pl.DataFrame:
    """Inverse of :func:`rename_to_title_case` — ``Feature Key`` -> ``FEATURE_KEY``."""
    return df.rename({col: col.upper().replace(" ", "_") for col in df.columns})


# ---------------------------------------------------------------------------
# Export & publish (Tableau)
# ---------------------------------------------------------------------------

def export_hyper(df: pl.DataFrame, hyper_path: Path, table_name: str, config: dict) -> None:
    import pantab as pt
    import pyarrow as pa

    hyper_path.parent.mkdir(parents=True, exist_ok=True)
    backup_file(hyper_path, config)
    # Cast Null-typed columns to String so pantab doesn't choke on Arrow na type
    null_cols = [col for col in df.columns if df[col].dtype == pl.Null]
    if null_cols:
        df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in null_cols])
    # Use Arrow directly — Polars -> Arrow is near zero-copy (both Arrow-backed),
    # whereas .to_pandas() materializes numpy arrays and can trigger ArrayMemoryError
    arrow_table = df.to_arrow()
    # Cast null-typed Arrow columns to string to avoid pantab "unsupported type: null"
    for i, field in enumerate(arrow_table.schema):
        if pa.types.is_null(field.type):
            arrow_table = arrow_table.set_column(
                i, field.name, arrow_table.column(i).cast(pa.string())
            )
    pt.frame_to_hyper(arrow_table, database=hyper_path, table_mode="w", table=table_name)
    logger.info(f"Exported {df.height} rows to {hyper_path} (table: {table_name})")


def publish_hyper(hyper_path: Path, config: dict,
                  targets: list[str] | None = None,
                  datasource_name: str | None = None) -> None:
    """Publish a hyper file to one or more Tableau servers.

    Args:
        targets: List of server keys to publish to (e.g. ["tst", "prd"]).
                 If None, publishes to all configured servers.
        datasource_name: Name of the datasource on Tableau Server.
    """
    from csm_commonlib.tableau.publish import publish_hyper_to_tableau
    # tableau_session is a context manager that signs in, yields a connected
    # TSC.Server, and signs out on exit. Import path may differ across
    # csm_commonlib versions -- try the dedicated session module, then the
    # publish module.
    try:
        from csm_commonlib.tableau.session import tableau_session
    except ImportError:
        from csm_commonlib.tableau.publish import tableau_session

    tab_cfg = config["tableau"]

    if targets is None:
        targets = list(tab_cfg.keys())

    for target in targets:
        if target not in tab_cfg:
            logger.warning(f"Tableau target '{target}' not found in config, skipping")
            continue

        env_cfg = tab_cfg[target]
        if not env_cfg.get("server_url"):
            logger.warning(f"Tableau {target}: no server_url configured, skipping")
            continue

        label = target.upper()
        logger.info(f"Publishing {hyper_path.name} to Tableau {label} ({env_cfg['server_url']})")
        print_info(f"Publishing [bold]{hyper_path.name}[/] to Tableau [cyan]{label}[/]")

        # server_key (optional) selects stored credentials, mirroring the DB
        # side's use_stored_credentials — so no username/password lives in
        # config. Empty string in YAML is treated as "not set".
        server_key = env_cfg.get("server_key") or None

        try:
            with tableau_session(
                server_key=server_key,
                server_url=env_cfg["server_url"],
                site_id=env_cfg["site_id"],
            ) as server:
                publish_hyper_to_tableau(
                    server=server,
                    project_name=env_cfg["project_name"],
                    datasource_name=datasource_name,
                    hyper_path=hyper_path,
                    overwrite=env_cfg.get("overwrite", True),
                )
            logger.info(f"Published {hyper_path.name} to {label}: {env_cfg['project_name']}")
            print_success(f"Published to [bold]{label}[/] -> {env_cfg['project_name']}")
        except Exception as exc:
            logger.error(f"Publish to {label} failed: {exc}")
            print_error(f"Publish to {label} failed: {exc}")
            raise
