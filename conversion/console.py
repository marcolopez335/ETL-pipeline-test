import time
from contextlib import contextmanager

import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import box

console = Console()


def _format_bytes(nbytes: int) -> str:
    if nbytes < 1024:
        return f"{nbytes} B"
    elif nbytes < 1024 ** 2:
        return f"{nbytes / 1024:.1f} KB"
    elif nbytes < 1024 ** 3:
        return f"{nbytes / 1024 ** 2:.1f} MB"
    else:
        return f"{nbytes / 1024 ** 3:.1f} GB"


def _format_value(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "-"
    val_str = str(val)
    if len(val_str) > 20:
        return val_str[:17] + "..."
    return val_str


def print_header(title: str) -> None:
    console.print()
    console.print(Panel(
        f"[bold white]{title}[/]",
        border_style="cyan",
        padding=(0, 2),
    ))


def print_step(step: int, total: int, message: str, detail: str = "") -> None:
    step_label = f"[dim]\\[{step}/{total}][/]"
    check = "[green bold]\\u2713[/]"
    detail_text = f"  [dim]{detail}[/]" if detail else ""
    console.print(f"  {step_label} {check} {message}{detail_text}")


def print_step_fail(step: int, total: int, message: str, error: str = "") -> None:
    step_label = f"[dim]\\[{step}/{total}][/]"
    cross = "[red bold]\\u2717[/]"
    detail_text = f"  [red]{error}[/]" if error else ""
    console.print(f"  {step_label} {cross} {message}{detail_text}")


@contextmanager
def step_spinner(step: int, total: int, message: str):
    step_label = f"[dim]\\[{step}/{total}][/]"
    start = time.time()
    with console.status(f"  {step_label} {message}...", spinner="dots") as status:
        try:
            yield status
        except Exception:
            elapsed = time.time() - start
            print_step_fail(step, total, message, f"failed after {elapsed:.1f}s")
            raise
    elapsed = time.time() - start
    print_step(step, total, message, f"{elapsed:.1f}s")


def print_dataframe_summary(df: pd.DataFrame, label: str) -> None:
    total_mem = int(df.memory_usage(deep=True).sum())

    table = Table(
        title=f"[bold]{label}[/]  [dim]({len(df):,} rows x {len(df.columns)} cols | {_format_bytes(total_mem)})[/]",
        box=box.ROUNDED,
        header_style="bold cyan",
        border_style="dim",
        show_lines=False,
    )

    table.add_column("Column", style="white", min_width=20)
    table.add_column("Dtype", style="yellow")
    table.add_column("Nulls", justify="right", style="dim")
    table.add_column("Null %", justify="right")
    table.add_column("Uniques", justify="right", style="cyan")
    table.add_column("Min", style="dim")
    table.add_column("Max", style="dim")
    table.add_column("Memory", justify="right", style="dim")

    for col in df.columns:
        null_count = int(df[col].isna().sum())
        null_pct = (null_count / len(df) * 100) if len(df) > 0 else 0.0
        unique_count = int(df[col].nunique())
        col_mem = int(df[col].memory_usage(deep=True))

        if null_pct > 10:
            pct_style = "red bold"
        elif null_pct > 5:
            pct_style = "yellow"
        elif null_pct > 0:
            pct_style = "dim"
        else:
            pct_style = "green"

        # Min/Max for numeric and datetime columns
        if pd.api.types.is_numeric_dtype(df[col]) or pd.api.types.is_datetime64_any_dtype(df[col]):
            col_min = _format_value(df[col].min())
            col_max = _format_value(df[col].max())
        else:
            col_min = "-"
            col_max = "-"

        table.add_row(
            col,
            str(df[col].dtype),
            f"{null_count:,}",
            Text(f"{null_pct:.1f}%", style=pct_style),
            f"{unique_count:,}",
            col_min,
            col_max,
            _format_bytes(col_mem),
        )

    total_nulls = int(df.isna().sum().sum())
    total_cells = df.shape[0] * df.shape[1]
    total_pct = (total_nulls / total_cells * 100) if total_cells > 0 else 0.0

    table.add_section()
    table.add_row(
        "[bold]Total[/]", "", f"{total_nulls:,}",
        Text(f"{total_pct:.1f}%", style="bold"),
        "", "", "",
        f"[bold]{_format_bytes(total_mem)}[/]",
    )

    console.print(table)
    console.print()


def print_polars_summary(df, label: str) -> None:
    import polars as pl

    height = df.height
    width = df.width
    total_mem = df.estimated_size()

    table = Table(
        title=f"[bold]{label}[/]  [dim]({height:,} rows x {width} cols | {_format_bytes(total_mem)})[/]",
        box=box.ROUNDED,
        header_style="bold cyan",
        border_style="dim",
        show_lines=False,
    )

    table.add_column("Column", style="white", min_width=20)
    table.add_column("Dtype", style="yellow")
    table.add_column("Nulls", justify="right", style="dim")
    table.add_column("Null %", justify="right")
    table.add_column("Uniques", justify="right", style="cyan")
    table.add_column("Min", style="dim")
    table.add_column("Max", style="dim")
    table.add_column("Memory", justify="right", style="dim")

    # Pre-compute stats in one pass
    null_counts = df.null_count()
    try:
        n_unique = df.select(pl.all().n_unique())
    except Exception:
        n_unique = None

    total_nulls = 0
    for col in df.columns:
        null_count = null_counts[col][0]
        total_nulls += null_count
        null_pct = (null_count / height * 100) if height > 0 else 0.0
        if n_unique is not None:
            unique_count = n_unique[col][0]
        else:
            try:
                unique_count = df[col].n_unique()
            except Exception:
                unique_count = -1
        col_mem = df[col].estimated_size()

        if null_pct > 10:
            pct_style = "red bold"
        elif null_pct > 5:
            pct_style = "yellow"
        elif null_pct > 0:
            pct_style = "dim"
        else:
            pct_style = "green"

        # Min/Max for numeric and temporal columns
        dtype = df[col].dtype
        if dtype.is_numeric() or dtype.is_temporal():
            try:
                col_min = _format_value(df[col].min())
                col_max = _format_value(df[col].max())
            except Exception:
                col_min = "-"
                col_max = "-"
        else:
            col_min = "-"
            col_max = "-"

        table.add_row(
            col,
            str(dtype),
            f"{null_count:,}",
            Text(f"{null_pct:.1f}%", style=pct_style),
            f"{unique_count:,}" if unique_count >= 0 else "n/a",
            col_min,
            col_max,
            _format_bytes(col_mem),
        )

    total_cells = height * width
    total_pct = (total_nulls / total_cells * 100) if total_cells > 0 else 0.0

    table.add_section()
    table.add_row(
        "[bold]Total[/]", "", f"{total_nulls:,}",
        Text(f"{total_pct:.1f}%", style="bold"),
        "", "", "",
        f"[bold]{_format_bytes(total_mem)}[/]",
    )

    console.print(table)
    console.print()


def print_info(message: str) -> None:
    console.print(f"  [cyan]>[/] {message}")


def print_success(message: str) -> None:
    console.print(f"  [green bold]\\u2713[/] {message}")


def print_error(message: str) -> None:
    console.print(f"  [red bold]\\u2717[/] {message}")


def print_pipeline_complete(name: str, elapsed: float) -> None:
    console.print()
    console.print(
        f"  [green bold]\\u2713 {name} complete[/]  [dim]({elapsed:.1f}s)[/]"
    )
    console.print()


MAX_COL_WIDTH = 50
DEFAULT_ROW_LIMIT = 100


def _truncate_value(val, max_width: int = MAX_COL_WIDTH) -> str:
    if val is None:
        return "[dim]-[/]"
    s = str(val)
    if len(s) > max_width:
        return s[: max_width - 3] + "..."
    return s


def _render_result_table(result, display_limit: int, has_explicit_limit: bool) -> None:
    total_rows = result.height
    truncated = not has_explicit_limit and total_rows > display_limit
    display = result.head(display_limit) if truncated else result

    table = Table(box=box.SIMPLE, header_style="bold cyan", border_style="dim", show_lines=False)
    for col in display.columns:
        table.add_column(col)
    for row in display.iter_rows():
        table.add_row(*[_truncate_value(v) for v in row])
    console.print(table)
    if truncated:
        console.print(f"  [dim]Showing first {display_limit} of {total_rows:,} rows — use LIMIT to control[/]")
    else:
        console.print(f"  [dim]{total_rows:,} row(s)[/]")


def _describe_table(df) -> None:
    import polars as pl

    height = df.height
    null_counts = df.null_count()
    try:
        n_unique = df.select(pl.all().n_unique())
    except Exception:
        n_unique = None

    table = Table(box=box.SIMPLE, header_style="bold cyan", border_style="dim", show_lines=False)
    table.add_column("Column", style="white", min_width=25)
    table.add_column("Dtype", style="yellow")
    table.add_column("Nulls", justify="right", style="dim")
    table.add_column("Null %", justify="right")
    table.add_column("Uniques", justify="right", style="cyan")
    table.add_column("Min", style="dim")
    table.add_column("Max", style="dim")

    for col in df.columns:
        null_count = null_counts[col][0]
        null_pct = (null_count / height * 100) if height > 0 else 0.0
        if n_unique is not None:
            unique_count = n_unique[col][0]
        else:
            try:
                unique_count = df[col].n_unique()
            except Exception:
                unique_count = -1

        dtype = df[col].dtype
        if dtype.is_numeric() or dtype.is_temporal():
            try:
                col_min = _truncate_value(df[col].min(), 20)
                col_max = _truncate_value(df[col].max(), 20)
            except Exception:
                col_min = col_max = "-"
        else:
            col_min = col_max = "-"

        pct_style = "green" if null_pct == 0 else "dim" if null_pct <= 5 else "yellow" if null_pct <= 10 else "red bold"

        table.add_row(
            col, str(dtype), f"{null_count:,}",
            Text(f"{null_pct:.1f}%", style=pct_style),
            f"{unique_count:,}" if unique_count >= 0 else "n/a",
            col_min, col_max,
        )

    console.print(table)
    console.print(f"  [dim]{height:,} rows x {df.width} cols | {_format_bytes(df.estimated_size())}[/]")


def _print_help() -> None:
    help_table = Table(box=box.SIMPLE, header_style="bold cyan", border_style="dim", show_lines=False)
    help_table.add_column("Command", style="bold white", min_width=30)
    help_table.add_column("Description", style="dim")
    help_table.add_row("SELECT ... FROM <table>", "Run a SQL query")
    help_table.add_row("tables", "List available tables and row counts")
    help_table.add_row("schema <table>", "Show column names and dtypes")
    help_table.add_row("describe <table>", "Column stats (nulls, uniques, min/max)")
    help_table.add_row("sample <table> [n]", "Show n random rows (default: 10)")
    help_table.add_row("count <table>", "Row count for a table")
    help_table.add_row("export csv <filename>", "Export last result to CSV")
    help_table.add_row("export parquet <filename>", "Export last result to Parquet")
    help_table.add_row("save <name>", "Save last result as a new queryable table")
    help_table.add_row("history", "Show query history")
    help_table.add_row("!<n>", "Re-run query #n from history")
    help_table.add_row("clear", "Clear the screen")
    help_table.add_row("help", "Show this help")
    help_table.add_row("exit / quit / q", "Exit the SQL shell")
    console.print(help_table)


def interactive_sql(tables: dict) -> None:
    """Interactive SQL shell over Polars DataFrames using pl.SQLContext."""
    import os
    import polars as pl

    # Try to enable readline for arrow-key history
    try:
        import readline  # noqa: F401
    except ImportError:
        pass

    ctx = pl.SQLContext(tables)
    table_names = list(tables.keys())
    query_history = []
    last_result = None

    console.print()
    console.rule("[bold cyan]Interactive SQL Query Mode[/]", style="cyan")
    console.print()
    console.print(f"  [cyan]Available tables:[/] {', '.join(f'[bold]{t}[/]' for t in table_names)}")
    for name, df in tables.items():
        console.print(f"    [dim]{name}: {df.height:,} rows x {df.width} cols[/]")
    console.print()
    console.print("  [dim]Type 'help' for commands, or enter a SQL query. End multi-line queries with ;[/]")
    console.print()

    while True:
        # Read input — support multi-line queries ending with ;
        try:
            line = console.input("[bold cyan]sql>[/] ").strip()
        except (EOFError, KeyboardInterrupt):
            console.print()
            break

        if not line:
            continue

        # Multi-line: if no ; at end, keep reading
        if line.lower() not in ("exit", "quit", "q", "tables", "clear", "help", "history") \
                and not line.lower().startswith(("schema ", "describe ", "sample ", "count ",
                                                 "export ", "save ", "!")) \
                and not line.endswith(";"):
            buffer = [line]
            while True:
                try:
                    cont = console.input("[bold cyan] ..>[/] ").strip()
                except (EOFError, KeyboardInterrupt):
                    console.print()
                    buffer = []
                    break
                buffer.append(cont)
                if cont.endswith(";"):
                    break
            if not buffer:
                continue
            line = " ".join(buffer)

        # Strip trailing semicolon
        query = line.rstrip(";").strip()
        if not query:
            continue

        cmd = query.lower()

        # --- Built-in commands ---
        if cmd in ("exit", "quit", "q"):
            break

        if cmd == "help":
            _print_help()
            console.print()
            continue

        if cmd == "clear":
            os.system("cls" if os.name == "nt" else "clear")
            continue

        if cmd == "tables":
            for name in table_names:
                df = tables[name]
                console.print(f"  [bold]{name}[/]  [dim]({df.height:,} rows x {df.width} cols)[/]")
            console.print()
            continue

        if cmd == "history":
            if not query_history:
                console.print("  [dim]No queries yet.[/]")
            else:
                for i, q in enumerate(query_history, 1):
                    console.print(f"  [dim]{i:>3}[/]  {q}")
            console.print()
            continue

        if cmd.startswith("!"):
            idx_str = cmd[1:]
            try:
                idx = int(idx_str) - 1
                if 0 <= idx < len(query_history):
                    query = query_history[idx]
                    console.print(f"  [dim]Re-running:[/] {query}")
                else:
                    console.print(f"  [red]Invalid history index. Range: 1-{len(query_history)}[/]")
                    console.print()
                    continue
            except ValueError:
                console.print(f"  [red]Usage: !<number>[/]")
                console.print()
                continue

        if cmd.startswith("schema "):
            tbl = query.split(None, 1)[1].strip()
            if tbl in tables:
                for col_name, dtype in tables[tbl].schema.items():
                    console.print(f"  [white]{col_name:<35}[/] [yellow]{dtype}[/]")
            else:
                console.print(f"  [red]Unknown table. Available: {', '.join(table_names)}[/]")
            console.print()
            continue

        if cmd.startswith("describe "):
            tbl = query.split(None, 1)[1].strip()
            if tbl in tables:
                _describe_table(tables[tbl])
            else:
                console.print(f"  [red]Unknown table. Available: {', '.join(table_names)}[/]")
            console.print()
            continue

        if cmd.startswith("sample"):
            parts = query.split()
            tbl = parts[1] if len(parts) > 1 else None
            n = int(parts[2]) if len(parts) > 2 else 10
            if tbl and tbl in tables:
                sampled = tables[tbl].sample(min(n, tables[tbl].height))
                _render_result_table(sampled, n, True)
                last_result = sampled
            else:
                console.print(f"  [red]Usage: sample <table> [n]. Available: {', '.join(table_names)}[/]")
            console.print()
            continue

        if cmd.startswith("count "):
            tbl = query.split(None, 1)[1].strip()
            if tbl in tables:
                console.print(f"  [bold]{tbl}[/]: [cyan]{tables[tbl].height:,}[/] rows")
            else:
                console.print(f"  [red]Unknown table. Available: {', '.join(table_names)}[/]")
            console.print()
            continue

        if cmd.startswith("export "):
            parts = query.split(None, 2)
            if len(parts) < 3 or parts[1] not in ("csv", "parquet"):
                console.print("  [red]Usage: export csv <filename> | export parquet <filename>[/]")
                console.print()
                continue
            if last_result is None:
                console.print("  [red]No query result to export. Run a query first.[/]")
                console.print()
                continue
            fmt, filename = parts[1], parts[2]
            try:
                if fmt == "csv":
                    last_result.write_csv(filename)
                else:
                    last_result.write_parquet(filename)
                console.print(f"  [green]Exported {last_result.height:,} rows to [bold]{filename}[/][/]")
            except Exception as exc:
                console.print(f"  [red]Export failed: {exc}[/]")
            console.print()
            continue

        if cmd.startswith("save "):
            name = query.split(None, 1)[1].strip()
            if last_result is None:
                console.print("  [red]No query result to save. Run a query first.[/]")
                console.print()
                continue
            tables[name] = last_result
            table_names = list(tables.keys())
            ctx = pl.SQLContext(tables)
            console.print(f"  [green]Saved as [bold]{name}[/] ({last_result.height:,} rows x {last_result.width} cols)[/]")
            console.print()
            continue

        # --- SQL query execution ---
        query_history.append(query)
        has_explicit_limit = "limit" in query.lower()
        start_time = time.time()

        try:
            result = ctx.execute(query).collect()
            elapsed = time.time() - start_time
            last_result = result

            if result.height == 0:
                console.print(f"  [dim]No results. ({elapsed:.3f}s)[/]")
            else:
                _render_result_table(result, DEFAULT_ROW_LIMIT, has_explicit_limit)
                console.print(f"  [dim]Query time: {elapsed:.3f}s[/]")
        except Exception as exc:
            console.print(f"  [red]Error: {exc}[/]")

        console.print()
