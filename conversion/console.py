import time
from contextlib import contextmanager

import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import box

console = Console()


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
    table = Table(
        title=f"[bold]{label}[/]  [dim]({len(df):,} rows x {len(df.columns)} cols)[/]",
        box=box.ROUNDED,
        header_style="bold cyan",
        border_style="dim",
        show_lines=False,
    )

    table.add_column("Column", style="white", min_width=20)
    table.add_column("Dtype", style="yellow")
    table.add_column("Nulls", justify="right", style="dim")
    table.add_column("Null %", justify="right")

    for col in df.columns:
        null_count = int(df[col].isna().sum())
        null_pct = (null_count / len(df) * 100) if len(df) > 0 else 0.0

        if null_pct > 10:
            pct_style = "red bold"
        elif null_pct > 5:
            pct_style = "yellow"
        elif null_pct > 0:
            pct_style = "dim"
        else:
            pct_style = "green"

        table.add_row(
            col,
            str(df[col].dtype),
            f"{null_count:,}",
            Text(f"{null_pct:.1f}%", style=pct_style),
        )

    total_nulls = int(df.isna().sum().sum())
    total_cells = df.shape[0] * df.shape[1]
    total_pct = (total_nulls / total_cells * 100) if total_cells > 0 else 0.0

    table.add_section()
    table.add_row(
        "[bold]Total[/]", "", f"{total_nulls:,}",
        Text(f"{total_pct:.1f}%", style="bold"),
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
