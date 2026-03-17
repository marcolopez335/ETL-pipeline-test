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



# Re-export interactive_sql from the sql_shell package for backwards compatibility
from sql_shell import interactive_sql  # noqa: F401
