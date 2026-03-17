"""Interactive SQL shell over Polars DataFrames using pl.SQLContext."""

import os
import time
import polars as pl
from sql_shell.display import (
    console, render_result_table, describe_table, print_help,
    DEFAULT_ROW_LIMIT,
)

# Try to enable readline for arrow-key history
try:
    import readline  # noqa: F401
except ImportError:
    pass


def interactive_sql(tables: dict, row_limit: int = DEFAULT_ROW_LIMIT) -> None:
    """Launch an interactive SQL shell over the given Polars DataFrames.

    Args:
        tables: Dict mapping table names to pl.DataFrames.
        row_limit: Default max rows to display (override with LIMIT in queries).
    """
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

        # Builtin single-word/prefix commands that don't need ;
        _builtin_prefixes = (
            "schema ", "describe ", "sample ", "count ",
            "export ", "save ", "!",
        )
        _builtin_words = ("exit", "quit", "q", "tables", "clear", "help", "history")

        is_builtin = line.lower() in _builtin_words or line.lower().startswith(_builtin_prefixes)

        # Multi-line: if not a builtin and no ; at end, keep reading
        if not is_builtin and not line.endswith(";"):
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
            print_help()
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
                console.print("  [red]Usage: !<number>[/]")
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
                describe_table(tables[tbl])
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
                render_result_table(sampled, n, True)
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
                render_result_table(result, row_limit, has_explicit_limit)
                console.print(f"  [dim]Query time: {elapsed:.3f}s[/]")
        except Exception as exc:
            console.print(f"  [red]Error: {exc}[/]")

        console.print()
