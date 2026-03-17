"""
Run the SQL shell standalone with parquet/CSV files or directories.

Usage:
    python -m sql_shell data.parquet
    python -m sql_shell epics.parquet stories.parquet
    python -m sql_shell data.csv --name mytable
    python -m sql_shell epics.parquet --name epics stories.parquet --name stories
    python -m sql_shell ./cache/
"""

import argparse
from pathlib import Path
import polars as pl
from sql_shell.shell import interactive_sql
from sql_shell.display import console


def _load_file(path: Path, name: str, tables: dict) -> bool:
    """Load a single file into tables dict. Returns True on success."""
    try:
        if path.suffix == ".csv":
            df = pl.read_csv(path)
        elif path.suffix in (".parquet", ".pq"):
            df = pl.read_parquet(path)
        else:
            console.print(f"  [dim]Skipping unsupported file: {path.name}[/]")
            return False
        tables[name] = df
        console.print(f"  [green]Loaded [bold]{name}[/] from {path} ({df.height:,} rows x {df.width} cols)[/]")
        return True
    except Exception as exc:
        console.print(f"  [red]Failed to load {path}: {exc}[/]")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Interactive SQL shell for Parquet and CSV files",
    )
    parser.add_argument(
        "files", nargs="+",
        help="Parquet/CSV files or directories to load. Use --name before a file to set the table name.",
    )
    parser.add_argument(
        "--limit", type=int, default=100,
        help="Default row display limit (default: 100)",
    )
    args = parser.parse_args()

    # Parse files and optional --name flags
    tables = {}
    files = args.files
    i = 0
    while i < len(files):
        if files[i] == "--name" and i + 1 < len(files):
            # Next arg is the name, arg after is the file
            name = files[i + 1]
            i += 2
            if i < len(files):
                filepath = files[i]
                i += 1
            else:
                console.print(f"  [red]--name {name} missing file path[/]")
                return
        else:
            filepath = files[i]
            name = None  # auto-name from stem
            i += 1

        path = Path(filepath)
        if not path.exists():
            console.print(f"  [red]Not found: {filepath}[/]")
            return

        if path.is_dir():
            # Load all parquet/csv files in directory
            found = sorted(path.glob("*.parquet")) + sorted(path.glob("*.pq")) + sorted(path.glob("*.csv"))
            if not found:
                console.print(f"  [yellow]No parquet/csv files found in {filepath}[/]")
                continue
            for f in found:
                _load_file(f, f.stem, tables)
        else:
            _load_file(path, name or path.stem, tables)

    if tables:
        interactive_sql(tables, row_limit=args.limit)


if __name__ == "__main__":
    main()
