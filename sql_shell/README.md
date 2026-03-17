# sql_shell

A standalone interactive SQL query shell for Polars DataFrames. Query, explore, and export data using standard SQL — no database required.

## Quick Start

### As a library

```python
import polars as pl
from sql_shell import interactive_sql

df = pl.read_parquet("data.parquet")
interactive_sql({"mytable": df})
```

### From the command line

```bash
# Load one or more parquet/CSV files
python -m sql_shell data.parquet
python -m sql_shell epics.parquet stories.parquet

# Custom table names
python -m sql_shell --name epics epics.parquet --name stories stories.parquet

# Custom row limit
python -m sql_shell data.parquet --limit 50
```

## Commands

| Command | Description |
|---------|-------------|
| `SELECT ... FROM <table>` | Run a SQL query |
| `tables` | List available tables and row counts |
| `schema <table>` | Show column names and dtypes |
| `describe <table>` | Column stats (nulls, uniques, min/max) |
| `sample <table> [n]` | Show n random rows (default: 10) |
| `count <table>` | Quick row count |
| `export csv <filename>` | Export last result to CSV |
| `export parquet <filename>` | Export last result to Parquet |
| `save <name>` | Save last result as a new queryable table |
| `history` | Show query history |
| `!<n>` | Re-run query #n from history |
| `clear` | Clear the screen |
| `help` | Show all commands |
| `exit` | Exit the shell |

## Features

- **Standard SQL** via Polars `SQLContext` — SELECT, WHERE, GROUP BY, JOIN, ORDER BY, LIMIT, etc.
- **Multi-line queries** — keep typing across lines, end with `;` to execute
- **Query timing** — execution time shown after every query
- **Default row limit** — caps at 100 rows, override with `LIMIT`
- **Column truncation** — long values trimmed to 50 chars for clean display
- **Export results** — write query results to CSV or Parquet
- **Save as table** — persist a query result as a new table you can query against
- **Query history** — view past queries and re-run with `!n`
- **Describe** — full column stats with null counts, unique values, min/max
- **Rich terminal output** — colored tables, formatted results via Rich

## Dependencies

- `polars`
- `rich`
