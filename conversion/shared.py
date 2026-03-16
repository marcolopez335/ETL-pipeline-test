import shutil
from datetime import datetime

import pandas as pd
import pantab as pt
import yaml
from pathlib import Path
from common.database.tibco import TibcoConnection
from common.logging import get_logger
from conversion.console import (
    print_dataframe_summary, print_info, print_success, print_error,
)

ROOT_DIR = Path(__file__).resolve().parent.parent
SQL_DIR = ROOT_DIR / "sql"
CACHE_DIR = ROOT_DIR / "cache"
BACKUP_DIR = ROOT_DIR / "backups"

pd.options.mode.string_storage = "pyarrow"

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


def run_query(sql_filename: str, database: str = "default", verbose: bool = True) -> pd.DataFrame:
    logger.info(f"Running query: {sql_filename}")
    query = load_sql(sql_filename)
    conn = TibcoConnection()
    conn.connect(database=database, use_stored_credentials=True)

    try:
        df = conn.execute_query(query, verbose=verbose)
        logger.info(f"Query returned {len(df)} rows")
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


def clean_dtypes(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    df = df.copy()

    for col, dtype in schema.items():
        if col not in df.columns:
            continue

        if dtype == "datetime":
            df[col] = pd.to_datetime(df[col], errors="coerce")
        elif dtype == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif dtype == "string":
            df[col] = df[col].astype("string").str.strip()

    return df


def log_dataframe_summary(df: pd.DataFrame, label: str) -> None:
    # File logging
    logger.info(f"--- {label} Summary ---")
    logger.info(f"  Rows: {len(df)}")
    logger.info(f"  Columns: {len(df.columns)}")
    for col in df.columns:
        null_count = df[col].isna().sum()
        null_pct = (null_count / len(df) * 100) if len(df) > 0 else 0.0
        logger.info(f"    {col:<30} {str(df[col].dtype):<20} nulls: {null_count} ({null_pct:.1f}%)")
    total_nulls = df.isna().sum().sum()
    total_cells = df.shape[0] * df.shape[1]
    total_null_pct = (total_nulls / total_cells * 100) if total_cells > 0 else 0.0
    logger.info(f"  Total null %: {total_null_pct:.1f}%")

    # Rich console output
    print_dataframe_summary(df, label)


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

    # Clean old backups beyond max_backups
    max_backups = backup_cfg.get("max_backups", 5)
    existing = sorted(
        BACKUP_DIR.glob(f"{file_path.stem}_*{file_path.suffix}"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for old in existing[max_backups:]:
        old.unlink()
        logger.info(f"Removed old backup: {old.name}")


def read_history_cache(cache_path: Path) -> pd.DataFrame:
    if not cache_path.exists():
        logger.info(f"No cache found at {cache_path}")
        return pd.DataFrame()

    df = pd.read_parquet(cache_path)
    logger.info(f"Read {len(df)} rows from cache")
    return df


def write_history_cache(df: pd.DataFrame, cache_path: Path) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache_path, index=False)
    logger.info(f"Wrote {len(df)} rows to cache: {cache_path}")


def fetch_history(sql_filename: str, key_col: str) -> pd.DataFrame:
    df = run_query(sql_filename)

    if "SNAPSHOT_DATE" in df.columns:
        df["SNAPSHOT_DATE"] = pd.to_datetime(df["SNAPSHOT_DATE"], errors="coerce")

    required = {key_col, "SNAPSHOT_DATE"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"Table missing columns: {missing}")

    return df


def update_history_cache_with_recent(
    cached: pd.DataFrame, recent: pd.DataFrame, key_col: str
) -> pd.DataFrame:
    KEY_COLS = [key_col, "SNAPSHOT_DATE"]

    cached = cached.copy()
    recent = recent.copy()

    cached["SNAPSHOT_DATE"] = pd.to_datetime(cached["SNAPSHOT_DATE"], errors="coerce")
    recent["SNAPSHOT_DATE"] = pd.to_datetime(recent["SNAPSHOT_DATE"], errors="coerce")

    cached[key_col] = cached[key_col].astype(str)
    recent[key_col] = recent[key_col].astype(str)

    if cached.empty:
        return recent.dropna(subset=KEY_COLS).drop_duplicates(subset=KEY_COLS, keep="last")

    # Anti-join: keep cached rows whose keys aren't in recent
    merged = cached.merge(recent[KEY_COLS].drop_duplicates(), on=KEY_COLS, how="left", indicator=True)
    cached_keep = merged[merged["_merge"] == "left_only"].drop(columns="_merge")

    combined = pd.concat([cached_keep, recent], ignore_index=True)
    combined = combined.dropna(subset=KEY_COLS)
    combined = combined.drop_duplicates(subset=KEY_COLS, keep="last")

    if len(combined) < 0.98 * len(cached):
        raise RuntimeError(
            f"Cache shrank unexpectedly: {len(combined)} rows vs {len(cached)} prior"
        )

    logger.info(f"Cache updated: {len(cached)} -> {len(combined)} rows")
    return combined


def build_and_cache_history(sql_full: str, key_col: str, cache_path: Path) -> pd.DataFrame:
    logger.info("Building full history cache")
    df = fetch_history(sql_full, key_col)
    write_history_cache(df, cache_path)
    return df


def update_history(
    sql_full: str, sql_recent: str, key_col: str, cache_path: Path
) -> pd.DataFrame:
    cached = read_history_cache(cache_path)

    if cached.empty:
        return build_and_cache_history(sql_full, key_col, cache_path)

    recent = fetch_history(sql_recent, key_col)
    updated = update_history_cache_with_recent(cached, recent, key_col)

    write_history_cache(updated, cache_path)
    return updated


def union_data(df_summary: pd.DataFrame, df_history: pd.DataFrame) -> pd.DataFrame:
    unioned = pd.concat([df_summary, df_history], ignore_index=True, sort=False)
    return unioned.drop_duplicates()


def export_hyper(df: pd.DataFrame, hyper_path: Path, table_name: str, config: dict) -> None:
    hyper_path.parent.mkdir(parents=True, exist_ok=True)
    backup_file(hyper_path, config)
    pt.frame_to_hyper(df, database=hyper_path, table_mode="w", table=table_name)
    logger.info(f"Exported {len(df)} rows to {hyper_path} (table: {table_name})")
