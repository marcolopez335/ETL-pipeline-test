"""Display helpers for rendering Polars DataFrames in the terminal."""

from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich import box

console = Console()

MAX_COL_WIDTH = 50
DEFAULT_ROW_LIMIT = 100


def _format_bytes(nbytes: int) -> str:
    if nbytes < 1024:
        return f"{nbytes} B"
    elif nbytes < 1024 ** 2:
        return f"{nbytes / 1024:.1f} KB"
    elif nbytes < 1024 ** 3:
        return f"{nbytes / 1024 ** 2:.1f} MB"
    else:
        return f"{nbytes / 1024 ** 3:.1f} GB"


def truncate_value(val, max_width: int = MAX_COL_WIDTH) -> str:
    if val is None:
        return "[dim]-[/]"
    s = str(val)
    if len(s) > max_width:
        return s[: max_width - 3] + "..."
    return s


def render_result_table(result, display_limit: int = DEFAULT_ROW_LIMIT,
                        has_explicit_limit: bool = False) -> None:
    total_rows = result.height
    truncated = not has_explicit_limit and total_rows > display_limit
    display = result.head(display_limit) if truncated else result

    table = Table(box=box.SIMPLE, header_style="bold cyan", border_style="dim", show_lines=False)
    for col in display.columns:
        table.add_column(col)
    for row in display.iter_rows():
        table.add_row(*[truncate_value(v) for v in row])
    console.print(table)
    if truncated:
        console.print(f"  [dim]Showing first {display_limit} of {total_rows:,} rows — use LIMIT to control[/]")
    else:
        console.print(f"  [dim]{total_rows:,} row(s)[/]")


def describe_table(df) -> None:
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
                col_min = truncate_value(df[col].min(), 20)
                col_max = truncate_value(df[col].max(), 20)
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


def print_help() -> None:
    table = Table(box=box.SIMPLE, header_style="bold cyan", border_style="dim", show_lines=False)
    table.add_column("Command", style="bold white", min_width=30)
    table.add_column("Description", style="dim")
    table.add_row("SELECT ... FROM <table>", "Run a SQL query")
    table.add_row("tables", "List available tables and row counts")
    table.add_row("schema <table>", "Show column names and dtypes")
    table.add_row("describe <table>", "Column stats (nulls, uniques, min/max)")
    table.add_row("sample <table> [n]", "Show n random rows (default: 10)")
    table.add_row("count <table>", "Row count for a table")
    table.add_row("export csv <filename>", "Export last result to CSV")
    table.add_row("export parquet <filename>", "Export last result to Parquet")
    table.add_row("save <name>", "Save last result as a new queryable table")
    table.add_row("history", "Show query history")
    table.add_row("!<n>", "Re-run query #n from history")
    table.add_row("clear", "Clear the screen")
    table.add_row("help", "Show this help")
    table.add_row("exit / quit / q", "Exit the SQL shell")
    console.print(table)
