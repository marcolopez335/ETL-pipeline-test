import gc
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import yaml

# common.* and the rich-driven console module are imported lazily so this
# module can be imported (and its pure-Polars helpers tested) without the
# proprietary `common` package or the rich runtime.
try:
    from common.logging import get_logger
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

# Default locations — override via config.yaml `paths:` section.
SQL_DIR = ROOT_DIR / "sql"
CACHE_DIR = ROOT_DIR / "cache"
BACKUP_DIR = ROOT_DIR / "backups"
OUTPUT_DIR = ROOT_DIR / "output"

logger = get_logger(__name__)


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


def _use_stored_credentials(config: dict | None) -> bool:
    if not config:
        return True
    return bool(config.get("database", {}).get("use_stored_credentials", True))


def load_sql(filename: str) -> str:
    sql_path = SQL_DIR / filename
    if not sql_path.is_file():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    return sql_path.read_text(encoding="utf-8")


def run_query(
    sql_filename: str, database: str = "default", verbose: bool = True,
    config: dict | None = None,
) -> pl.DataFrame:
    from common.database.tibco import TibcoConnection
    logger.info(f"Running query: {sql_filename}")
    query = load_sql(sql_filename)
    conn = TibcoConnection()
    conn.connect(database=database, use_stored_credentials=_use_stored_credentials(config))

    try:
        pdf = conn.execute_query(query, verbose=verbose)
        df = pl.from_pandas(pdf)
        # Free the pandas copy immediately — it can be as large as the Polars one
        del pdf
        gc.collect()
        # Cast any Null-typed columns (all-null from pandas) to Utf8 early
        null_cols = [c for c in df.columns if df[c].dtype == pl.Null]
        if null_cols:
            df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in null_cols])
        logger.info(f"Query returned {df.height} rows")
    except Exception as exc:
        logger.error(f"Failed to execute {sql_filename}: {exc}")
        raise
    finally:
        conn.close()

    return df


def test_connection(database: str = "default", config: dict | None = None) -> bool:
    from common.database.tibco import TibcoConnection
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


def clean_dtypes(df: pl.DataFrame, schema: dict) -> pl.DataFrame:
    casts = []
    for col, dtype in schema.items():
        if col not in df.columns:
            continue

        if dtype == "datetime":
            casts.append(pl.col(col).cast(pl.Datetime, strict=False))
        elif dtype == "float":
            casts.append(pl.col(col).cast(pl.Float64, strict=False))
        elif dtype == "string":
            casts.append(pl.col(col).cast(pl.Utf8, strict=False).str.strip_chars())

    if casts:
        df = df.with_columns(casts)

    return df


def log_dataframe_summary(df: pl.DataFrame, label: str) -> None:
    total_mem = df.estimated_size()
    logger.info(f"--- {label} Summary ---")
    logger.info(f"  Rows: {df.height}  Columns: {df.width}  Memory: {total_mem:,} bytes")

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


def backup_file(file_path: Path, config: dict) -> None:
    backup_cfg = config.get("backup", {})
    if not backup_cfg.get("enabled", True):
        return

    if not file_path.exists():
        return

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"{file_path.stem}_{timestamp}{file_path.suffix}"
    backup_path = BACKUP_DIR / backup_name

    shutil.copy2(file_path, backup_path)
    logger.info(f"Backed up {file_path.name} -> {backup_path}")
    print_info(f"Backup: [dim]{backup_name}[/]")

    max_backups = backup_cfg.get("max_backups", 5)
    existing = sorted(
        BACKUP_DIR.glob(f"{file_path.stem}_*{file_path.suffix}"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for old in existing[max_backups:]:
        old.unlink()
        logger.info(f"Removed old backup: {old.name}")


def read_history_cache(cache_path: Path) -> pl.LazyFrame | None:
    if not cache_path.exists():
        logger.info(f"No cache found at {cache_path}")
        return None

    lf = pl.scan_parquet(cache_path)
    logger.info(f"Lazy-scanning cache: {cache_path}")
    return lf


def write_history_cache(df: pl.DataFrame, cache_path: Path, config: dict = None) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    # Backup existing cache before overwrite
    if config is not None:
        cache_cfg = config.get("cache", {})
        if cache_cfg.get("backup_enabled", False) and cache_path.exists():
            BACKUP_DIR.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"{cache_path.stem}_{timestamp}{cache_path.suffix}"
            backup_path = BACKUP_DIR / backup_name
            shutil.copy2(cache_path, backup_path)
            logger.info(f"Cache backup: {cache_path.name} -> {backup_name}")
            # Rotate old cache backups
            max_backups = cache_cfg.get("max_cache_backups", 3)
            existing = sorted(
                BACKUP_DIR.glob(f"{cache_path.stem}_*{cache_path.suffix}"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            for old in existing[max_backups:]:
                old.unlink()
                logger.info(f"Removed old cache backup: {old.name}")
    df.write_parquet(cache_path)
    logger.info(f"Wrote {df.height} rows to cache: {cache_path}")


def _safe_dtype(dtype: pl.DataType) -> pl.DataType:
    """Return Utf8 if dtype is Null, otherwise return as-is."""
    return pl.Utf8 if dtype == pl.Null else dtype


def _supertype(a: pl.DataType, b: pl.DataType) -> pl.DataType:
    """Pick a target dtype that both ``a`` and ``b`` can be cast to losslessly.

    Rules (in order):
      1. Identical dtypes → return as-is.
      2. Either is Null → use the other.
      3. Both numeric → Float64 (covers Int / Float mix).
      4. Both temporal → Datetime (covers Date / Datetime mix).
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
    if a.is_temporal() and b.is_temporal():
        return pl.Datetime
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


def fetch_history(sql_filename: str, key_col: str) -> pl.DataFrame:
    df = run_query(sql_filename)

    required = {key_col, "SNAPSHOT_DATE"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"Table missing columns: {missing}")

    df = df.with_columns(pl.col("SNAPSHOT_DATE").cast(pl.Date, strict=False))

    return df


def update_history_cache_with_recent(
    cached_lf: pl.LazyFrame, recent: pl.DataFrame, key_col: str,
    config: dict = None,
) -> pl.DataFrame:
    KEY_COLS = [key_col, "SNAPSHOT_DATE"]

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
    recent_keys = recent.lazy().select(KEY_COLS).unique()
    cached_keep = cached_lf.join(recent_keys, on=KEY_COLS, how="anti").collect()

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
    gc.collect()

    combined = combined.drop_nulls(subset=KEY_COLS).unique(subset=KEY_COLS, keep="last")

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


def build_and_cache_history(sql_full: str, key_col: str, cache_path: Path, config: dict = None) -> pl.DataFrame:
    logger.info("Building full history cache")
    df = fetch_history(sql_full, key_col)
    write_history_cache(df, cache_path, config=config)
    return df


def update_history(
    sql_full: str, sql_recent: str, key_col: str, cache_path: Path,
    config: dict = None, force: bool = False,
) -> pl.DataFrame:
    cached_lf = read_history_cache(cache_path)

    if cached_lf is None:
        return build_and_cache_history(sql_full, key_col, cache_path, config=config)

    recent = fetch_history(sql_recent, key_col)

    if force:
        # Skip shrinkage check when --force is used
        cfg_override = dict(config) if config else {}
        cfg_override.setdefault("cache", {})
        cfg_override["cache"] = {**cfg_override["cache"], "min_retention_pct": 0.0}
        updated = update_history_cache_with_recent(cached_lf, recent, key_col, config=cfg_override)
    else:
        updated = update_history_cache_with_recent(cached_lf, recent, key_col, config=config)

    write_history_cache(updated, cache_path, config=config)
    return updated


def get_last_n_snapshots(n: int, day_of_week: int = 0, from_date: datetime = None) -> list[datetime]:
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


# Keep old name as alias for backwards compatibility
def get_last_n_mondays(n: int, from_date: datetime = None) -> list[datetime]:
    return get_last_n_snapshots(n, day_of_week=0, from_date=from_date)


def fill_missing_snapshots(
    df_summary: pl.DataFrame,
    df_history: pl.DataFrame,
    key_col: str,
    config: dict = None,
) -> pl.DataFrame:
    """Fill missing Monday snapshots in history using summary data.

    Checks the last n_mondays Mondays. For any Monday where no snapshot
    exists in df_history, creates a synthetic snapshot from df_summary
    with SNAPSHOT_DATE set to that Monday and IS_SYNTHETIC = True.

    When the database later provides the real snapshot, the cache update's
    anti-join will replace the synthetic row automatically.
    """
    snap_cfg = (config or {}).get("snapshots", {})
    day_of_week = snap_cfg.get("day_of_week", 0)
    n_weeks = snap_cfg.get("lookback_weeks", 4)

    mondays = get_last_n_snapshots(n_weeks, day_of_week=day_of_week)

    # Get existing snapshot dates from history
    existing_dates = set()
    if "SNAPSHOT_DATE" in df_history.columns and df_history.height > 0:
        dates = df_history.select(
            pl.col("SNAPSHOT_DATE").cast(pl.Date)
        ).unique().to_series().to_list()
        existing_dates = {d for d in dates if d is not None}

    # Find missing Mondays — exclude today because the summary already
    # represents current-day data; synthesizing today duplicates it
    today = datetime.now().date()
    missing_mondays = [m for m in mondays if m.date() not in existing_dates and m.date() != today]

    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    day_name = day_names[day_of_week]

    if not missing_mondays:
        logger.info(f"No missing snapshots in the last {n_weeks} {day_name}s")
        return df_history

    logger.info(f"Found {len(missing_mondays)} missing {day_name} snapshot(s)")
    for m in sorted(missing_mondays):
        logger.info(f"  Missing: {m.strftime('%Y-%m-%d')} ({day_name})")

    # Build synthetic snapshots from summary data
    summary_cols = [c for c in df_summary.columns if c not in ("SNAPSHOT_DATE", "IS_SYNTHETIC")]
    base = df_summary.select(summary_cols)

    synthetic_frames = []
    for monday in sorted(missing_mondays):
        snapshot = base.with_columns([
            pl.lit(monday.date()).alias("SNAPSHOT_DATE"),
            pl.lit(True).alias("IS_SYNTHETIC"),
        ])
        synthetic_frames.append(snapshot)
        logger.info(f"  Synthesized {snapshot.height} rows for {monday.strftime('%Y-%m-%d')}")

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
    df_summary, df_history = _align_schemas(df_summary, df_history)
    unioned = pl.concat([df_summary, df_history])
    return unioned.unique()


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


def publish_hyper(hyper_path: Path, table_name: str, config: dict,
                   targets: list[str] = None, datasource_name: str = None) -> None:
    """Publish a hyper file to one or more Tableau servers.

    Args:
        targets: List of server keys to publish to (e.g. ["tst", "prd"]).
                 If None, publishes to all configured servers.
        datasource_name: Name of the datasource on Tableau Server.
    """
    from common.tableau.publish import TableauPublishConfig, publish_hyper_to_tableau

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

        publish_config = TableauPublishConfig(
            server_url=env_cfg["server_url"],
            site_id=env_cfg["site_id"],
            project_name=env_cfg["project_name"],
            datasource_name=datasource_name,
            overwrite=env_cfg.get("overwrite", True),
        )

        label = target.upper()
        logger.info(f"Publishing {hyper_path.name} to Tableau {label} ({env_cfg['server_url']})")
        print_info(f"Publishing [bold]{hyper_path.name}[/] to Tableau [cyan]{label}[/]")

        try:
            publish_hyper_to_tableau(
                hyper_path=hyper_path,
                config=publish_config,
            )
            logger.info(f"Published {hyper_path.name} to {label}: {env_cfg['project_name']}")
            print_success(f"Published to [bold]{label}[/] → {env_cfg['project_name']}")
        except Exception as exc:
            logger.error(f"Publish to {label} failed: {exc}")
            print_error(f"Publish to {label} failed: {exc}")
            raise
