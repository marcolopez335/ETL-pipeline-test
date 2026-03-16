import shutil
from datetime import datetime

import polars as pl
import pandas as pd
import pantab as pt
import yaml
from pathlib import Path
from common.database.tibco import TibcoConnection
from common.logging import get_logger
from common.tableau.publish import (
    TableauPublishConfig, publish_hyper_to_tableau, TABLEAU_SERVICE_NAME,
)
from conversion.console import (
    print_polars_summary, print_info, print_success, print_error,
)

ROOT_DIR = Path(__file__).resolve().parent.parent
SQL_DIR = ROOT_DIR / "sql"
CACHE_DIR = ROOT_DIR / "cache"
BACKUP_DIR = ROOT_DIR / "backups"

logger = get_logger(__name__)


def load_config() -> dict:
    config_path = ROOT_DIR / "config.yaml"
    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_sql(filename: str) -> str:
    sql_path = SQL_DIR / filename
    if not sql_path.is_file():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    return sql_path.read_text(encoding="utf-8")


def run_query(sql_filename: str, database: str = "default", verbose: bool = True) -> pl.DataFrame:
    logger.info(f"Running query: {sql_filename}")
    query = load_sql(sql_filename)
    conn = TibcoConnection()
    conn.connect(database=database, use_stored_credentials=True)

    try:
        pdf = conn.execute_query(query, verbose=verbose)
        df = pl.from_pandas(pdf)
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


def test_connection(database: str = "default") -> bool:
    logger.info("Testing database connection")
    conn = TibcoConnection()
    try:
        conn.connect(database=database, use_stored_credentials=True)
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

    print_polars_summary(df, label)


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


def read_history_cache(cache_path: Path) -> pl.DataFrame:
    if not cache_path.exists():
        logger.info(f"No cache found at {cache_path}")
        return pl.DataFrame()

    df = pl.read_parquet(cache_path)
    logger.info(f"Read {df.height} rows from cache")
    return df


def write_history_cache(df: pl.DataFrame, cache_path: Path) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df.write_parquet(cache_path)
    logger.info(f"Wrote {df.height} rows to cache: {cache_path}")


def _safe_dtype(dtype: pl.DataType) -> pl.DataType:
    """Return Utf8 if dtype is Null, otherwise return as-is."""
    return pl.Utf8 if dtype == pl.Null else dtype


def _align_schemas(df1: pl.DataFrame, df2: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Align two DataFrames to the same columns and types before concat."""
    all_cols = dict.fromkeys(df1.columns + df2.columns)

    for col in all_cols:
        if col in df1.columns and col in df2.columns:
            if df1[col].dtype == pl.Null and df2[col].dtype == pl.Null:
                # Both Null — cast both to Utf8 so downstream doesn't choke
                df1 = df1.with_columns(pl.col(col).cast(pl.Utf8))
                df2 = df2.with_columns(pl.col(col).cast(pl.Utf8))
            elif df1[col].dtype != df2[col].dtype:
                if df1[col].dtype == pl.Null:
                    df1 = df1.with_columns(pl.col(col).cast(df2[col].dtype))
                elif df2[col].dtype == pl.Null:
                    df2 = df2.with_columns(pl.col(col).cast(df1[col].dtype))
                else:
                    # Both non-null but different — cast to supertype
                    try:
                        super_type = pl.datatypes.unify_dtypes([df1[col].dtype, df2[col].dtype])
                        df1 = df1.with_columns(pl.col(col).cast(super_type, strict=False))
                        df2 = df2.with_columns(pl.col(col).cast(super_type, strict=False))
                    except Exception:
                        df1 = df1.with_columns(pl.col(col).cast(pl.Utf8, strict=False))
                        df2 = df2.with_columns(pl.col(col).cast(pl.Utf8, strict=False))
        elif col not in df1.columns:
            df1 = df1.with_columns(pl.lit(None).cast(_safe_dtype(df2[col].dtype)).alias(col))
        else:
            df2 = df2.with_columns(pl.lit(None).cast(_safe_dtype(df1[col].dtype)).alias(col))

    # Ensure same column order
    df2 = df2.select(df1.columns)
    return df1, df2


def fetch_history(sql_filename: str, key_col: str) -> pl.DataFrame:
    df = run_query(sql_filename)

    required = {key_col, "SNAPSHOT_DATE"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"Table missing columns: {missing}")

    df = df.with_columns(pl.col("SNAPSHOT_DATE").cast(pl.Datetime, strict=False))

    return df


def update_history_cache_with_recent(
    cached: pl.DataFrame, recent: pl.DataFrame, key_col: str
) -> pl.DataFrame:
    KEY_COLS = [key_col, "SNAPSHOT_DATE"]

    cached = cached.with_columns([
        pl.col("SNAPSHOT_DATE").cast(pl.Datetime, strict=False),
        pl.col(key_col).cast(pl.Utf8, strict=False),
    ])
    recent = recent.with_columns([
        pl.col("SNAPSHOT_DATE").cast(pl.Datetime, strict=False),
        pl.col(key_col).cast(pl.Utf8, strict=False),
    ])

    if cached.height == 0:
        return recent.drop_nulls(subset=KEY_COLS).unique(subset=KEY_COLS, keep="last")

    # Native anti-join
    cached_keep = cached.join(
        recent.select(KEY_COLS).unique(),
        on=KEY_COLS,
        how="anti",
    )

    # Align schemas before concat — cached from parquet may differ from fresh query
    cached_keep, recent = _align_schemas(cached_keep, recent)

    combined = pl.concat([cached_keep, recent])
    combined = combined.drop_nulls(subset=KEY_COLS)
    combined = combined.unique(subset=KEY_COLS, keep="last")

    if combined.height < 0.98 * cached.height:
        raise RuntimeError(
            f"Cache shrank unexpectedly: {combined.height} rows vs {cached.height} prior"
        )

    logger.info(f"Cache updated: {cached.height} -> {combined.height} rows")
    return combined


def build_and_cache_history(sql_full: str, key_col: str, cache_path: Path) -> pl.DataFrame:
    logger.info("Building full history cache")
    df = fetch_history(sql_full, key_col)
    write_history_cache(df, cache_path)
    return df


def update_history(
    sql_full: str, sql_recent: str, key_col: str, cache_path: Path
) -> pl.DataFrame:
    cached = read_history_cache(cache_path)

    if cached.height == 0:
        return build_and_cache_history(sql_full, key_col, cache_path)

    recent = fetch_history(sql_recent, key_col)
    updated = update_history_cache_with_recent(cached, recent, key_col)

    write_history_cache(updated, cache_path)
    return updated


def union_data(df_summary: pl.DataFrame, df_history: pl.DataFrame) -> pl.DataFrame:
    df_summary, df_history = _align_schemas(df_summary, df_history)
    unioned = pl.concat([df_summary, df_history])
    return unioned.unique()


def export_hyper(df: pl.DataFrame, hyper_path: Path, table_name: str, config: dict) -> None:
    hyper_path.parent.mkdir(parents=True, exist_ok=True)
    backup_file(hyper_path, config)
    # Cast Null-typed columns to String so pantab doesn't choke on Arrow na type
    null_cols = [col for col in df.columns if df[col].dtype == pl.Null]
    if null_cols:
        df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in null_cols])
    pdf = df.to_pandas()
    # Force all-null columns to string dtype — pantab infers Arrow "null" type otherwise
    for col in pdf.columns:
        if pdf[col].isna().all():
            pdf[col] = pdf[col].astype(str)
    pt.frame_to_hyper(pdf, database=hyper_path, table_mode="w", table=table_name)
    logger.info(f"Exported {df.height} rows to {hyper_path} (table: {table_name})")


def publish_hyper(hyper_path: Path, table_name: str, config: dict) -> None:
    tab_cfg = config["tableau"]

    publish_config = TableauPublishConfig(
        server_url=tab_cfg["server_url"],
        site_id=tab_cfg["site_id"],
        project_name=tab_cfg["project_name"],
        overwrite=tab_cfg.get("overwrite", True),
    )

    logger.info(f"Publishing {hyper_path.name} to Tableau ({tab_cfg['server_url']})")
    print_info(f"Publishing [bold]{hyper_path.name}[/] to Tableau")

    try:
        publish_hyper_to_tableau(
            hyper_path=hyper_path,
            table_name=table_name,
            config=publish_config,
        )
        logger.info(f"Published {hyper_path.name} to project: {tab_cfg['project_name']}")
        print_success(f"Published to [bold]{tab_cfg['project_name']}[/]")
    except Exception as exc:
        logger.error(f"Publish failed: {exc}")
        print_error(f"Publish failed: {exc}")
        raise
