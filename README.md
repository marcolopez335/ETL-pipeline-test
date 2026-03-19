# AMMM Jira ETL Pipeline

A production ETL pipeline that extracts Jira backlog data from a Tibco database via ODBC, transforms it with [Polars](https://pola.rs/), and exports to Tableau Hyper files for reporting and analytics.

---

## Table of Contents

- [Project Structure](#project-structure)
- [Setup](#setup)
- [Configuration](#configuration)
- [Usage](#usage)
- [Pipelines](#pipelines)
- [Interactive SQL Query Mode](#interactive-sql-query-mode)
- [Synthetic Snapshots](#synthetic-snapshots)
- [Architecture](#architecture)
- [Features](#features)

---

## Project Structure

```
ETL-pipeline-test/
├── main.py                              # CLI entry point
├── config.yaml                          # Configuration (database, Tableau, paths)
├── requirements.txt                     # Python dependencies
├── conversion/
│   ├── shared.py                        # Shared utilities (query, cache, export, snapshots)
│   ├── console.py                       # Rich console output (progress, tables)
│   ├── stories_table.py                 # Stories pipeline
│   └── epics_table.py                   # Epics pipeline + ACRP + sprint range
├── sql_shell/                           # Standalone interactive SQL shell (reusable)
│   ├── __init__.py
│   ├── __main__.py                      # CLI: python -m sql_shell data.parquet
│   ├── shell.py                         # REPL loop, command parsing
│   └── display.py                       # Rich table rendering
├── sql/                                 # SQL query files (CTE hierarchy)
├── schemas/                             # Column dtype definitions
├── cache/                               # Parquet history caches (auto-generated)
├── output/                              # Hyper file output (auto-generated)
├── backups/                             # Hyper file backups (auto-generated)
└── logs/                                # Log files (auto-generated)
```

## Setup

```bash
python -m venv .odbcenv
.odbcenv\Scripts\activate
pip install -r requirements.txt
```

> Requires the internal `common` package for database connectivity, logging, and Tableau publishing.

## Configuration

All settings are managed in `config.yaml`:

```yaml
database:
  name: "default"
  use_stored_credentials: true

backup:
  enabled: true
  max_backups: 5

cache:
  backup_enabled: true
  max_cache_backups: 3
  min_retention_pct: 0.98

snapshots:
  day_of_week: 0        # 0=Monday, 1=Tuesday, ..., 6=Sunday
  lookback_weeks: 4

tableau:
  tst:
    server_url: "https://tableau-tst.example.com"
    site_id: "your-site"
    project_name: "Your Project"
    overwrite: true
  prd:
    server_url: "https://tableau.example.com"
    site_id: "your-site"
    project_name: "Your Project"
    overwrite: true
```

## Usage

```
python main.py [pipeline] [action] [options]
```

**Pipelines** — pick which data to process (default: both):

| Flag | Description |
|------|-------------|
| *(none)* | Run both stories and epics |
| `--stories` | Stories only |
| `--epics` | Epics only |

**Actions** — what to do (default: full run + export):

| Flag | Description |
|------|-------------|
| *(none)* | Full pipeline: fetch, cache, transform, export hyper |
| `--update-cache` | Update history caches only (no hyper export) |
| `--test` | Test database connection and exit |
| `--query` | Open interactive SQL shell after pipeline completes |
| `--query-only` | Open SQL shell from cached data (skip pipeline entirely) |

**Publishing** — push hyper files to Tableau Server:

| Flag | Description |
|------|-------------|
| `--publish` | Publish to all configured Tableau servers (tst + prd) |
| `--publish-tst` | Publish to TST only |
| `--publish-prd` | Publish to PRD only |

**Options:**

| Flag | Description |
|------|-------------|
| `--force` | Bypass cache shrinkage safety check (use if cache needs to shrink) |

### Examples

```bash
# Run everything (stories + epics), export hyper files
python main.py

# Run only epics, publish to TST
python main.py --epics --publish-tst

# Update stories cache without exporting
python main.py --update-cache --stories

# Force a cache rebuild when data legitimately shrank
python main.py --epics --update-cache --force

# Explore cached data without hitting the database
python main.py --query-only

# Run epics pipeline, then drop into SQL shell to inspect results
python main.py --epics --query

# Publish both pipelines to production
python main.py --publish-prd
```

Flags can be combined freely: `python main.py --stories --publish-tst --query`

## Pipelines

### Stories

1. Fetch summary data and history snapshots from Tibco
2. Update incremental history cache
3. Fill missing Monday snapshots (synthetic)
4. Union summary with history, fetch and join epics lookup
5. Apply transformations (`LAST_UPDATED`, `PROJECT_NAME_VERSION`, `SPRINT_NAME_ALT`, `SNAPSHOT_DATE_ALT`, `PI_FROM_SPRINT`, column renaming)
6. Export to `STORIES.hyper`
7. Optionally publish to Tableau Server

### Epics

1. Fetch summary data and history snapshots from Tibco
2. Update incremental history cache
3. Fill missing Monday snapshots (synthetic)
4. Union summary with history, apply transformations (`LAST_UPDATED`, sprint parsing, `MIN_SPRINT`/`MAX_SPRINT`)
5. Export to `EPICS.hyper`
6. Build ACRP release range view and export to `EPICS_ACRP.hyper`
7. Optionally publish both hyper files to Tableau Server

### ACRP (Active Capability Release Plan)

A derived view from epics data that maps features and sub-capabilities to their target release ranges:

1. Filters rows where `SNAPSHOT_DATE` is null and `FEATURE_KEY` or `SUBCAPABILITY_KEY` is not null
2. Splits comma-delimited `FEATURE_FIX_VERSION` into individual rows
3. Computes min/max target release per `FEATURE_KEY`
4. Joins the release range back to produce the final dataset

### Sprint Range

Parses `SPRINT_NAME` (e.g., `"Team Alpha PI 26.1.2"`) to extract the sprint version and computes `MIN_SPRINT` / `MAX_SPRINT` per `SNAPSHOT_DATE` + `PROGRAM_INCREMENT`. The `IP` (Innovation & Planning) sprint sorts as the highest value in each PI.

## Interactive SQL Query Mode

Run any pipeline with `--query` to open an interactive SQL shell powered by Polars `SQLContext`. Query the final DataFrames directly without loading Tableau or hitting the database.

```
sql> SELECT FEATURE_KEY, MIN_SPRINT, MAX_SPRINT FROM epics WHERE PROGRAM_INCREMENT = 'PI 26.1' LIMIT 5

 FEATURE_KEY   MIN_SPRINT   MAX_SPRINT
 FEAT-1234     26.1.1       26.1.IP
 FEAT-1235     26.1.1       26.1.IP
 FEAT-1236     26.1.1       26.1.IP
 FEAT-1237     26.1.1       26.1.IP
 FEAT-1238     26.1.1       26.1.IP
  5 row(s)
```

**Available commands:**

| Command | Description |
|---------|-------------|
| Any SQL query | Runs against in-memory DataFrames |
| `tables` | List available tables and row counts |
| `schema <table>` | Show column names and dtypes |
| `describe <table>` | Column stats (nulls, uniques, min/max) |
| `sample <table> [n]` | Show n random rows (default: 10) |
| `count <table>` | Quick row count |
| `export csv <file>` | Export last result to CSV |
| `export parquet <file>` | Export last result to Parquet |
| `save <name>` | Save last result as a new queryable table |
| `history` | Show query history |
| `!<n>` | Re-run query #n from history |
| `exit` | Exit the SQL shell |

**Available tables:** `stories`, `epics`, `acrp` (depending on which pipelines ran)

> Results are capped at 100 rows by default. Use `LIMIT` to override.

### Standalone SQL Shell

The `sql_shell` package can also be used independently — no pipeline required:

```bash
# Load individual files
python -m sql_shell data.parquet

# Load all parquet/CSV files from a directory
python -m sql_shell ./cache/

# Custom table names
python -m sql_shell --name epics epics.parquet --name stories stories.parquet
```

See [`sql_shell/README.md`](sql_shell/README.md) for full documentation.

## Synthetic Snapshots

The pipeline checks the last 4 Mondays and fills any gaps in the history cache automatically. If the database is missing a Monday snapshot, the pipeline synthesizes one from the current summary data:

- Rows are stamped with the missing Monday's `SNAPSHOT_DATE` and `IS_SYNTHETIC = True`
- When the database later provides the real snapshot, the next pipeline run's anti-join replaces the synthetic rows with real data automatically
- This ensures Tableau reports always have continuous weekly data, even when the source database has gaps

```
History cache:  Feb 23 ✓  |  Mar 2 ✓  |  Mar 9 ✗  |  Mar 16 ✗
                                          ↓              ↓
After fill:     Feb 23 ✓  |  Mar 2 ✓  |  Mar 9 ★  |  Mar 16 ★
                                        (synthetic)   (synthetic)

Next run (DB has Mar 9 now):
                Feb 23 ✓  |  Mar 2 ✓  |  Mar 9 ✓  |  Mar 16 ★
                                        (replaced)   (synthetic)
```

## Architecture

```
   Tibco DB ──ODBC──> run_query() ──> pl.DataFrame
                                          │
                          ┌───────────────┤
                          │               │
                    summary data    history cache
                          │          (scan_parquet)
                          │               │
                          │    fill_missing_snapshots()
                          │     (synthesize missing Mondays)
                          │               │
                          └──── union ────┘
                                  │
                           data_functions()
                            (transforms)
                                  │
                        ┌─────────┴─────────┐
                        │                   │
                   export_hyper()      build_acrp()
                   (Arrow → pantab)         │
                        │              export_hyper()
                        │                   │
                   publish_hyper()    publish_hyper()
                  (tst / prd)        (tst / prd)
```

## Features

- **Polars** — Multi-threaded DataFrame operations, native anti-joins, and Arrow-based memory for fast processing at 4M+ rows
- **Zero-copy Hyper export** — Exports via Polars → Arrow → pantab, bypassing pandas entirely to avoid memory doubling on large datasets
- **Lazy caching** — History data cached as `.parquet`; `scan_parquet` lazily reads only the rows needed for the incremental merge, avoiding full cache loads into memory
- **Synthetic snapshots** — Automatically fills missing snapshots from summary data; replaced by real data on the next pipeline run. Snapshot day and lookback weeks are configurable.
- **Interactive SQL** — Query final DataFrames with standard SQL via `--query` for debugging and data validation; also available standalone via `python -m sql_shell`
- **Sprint parsing** — Extracts sprint versions from names, handles IP sprints, and computes min/max per snapshot and program increment using numeric sort keys
- **Automatic backups** — Previous hyper files and cache files are timestamped and saved before overwrite, with configurable rotation
- **Summary statistics** — Each step logs a formatted table with column dtypes, null counts/percentages, unique values, min/max, and memory usage
- **Rich console output** — Color-coded progress spinners, step indicators, and formatted tables via [Rich](https://github.com/Textualize/rich)
- **Multi-environment Tableau publishing** — Publish hyper files to TST, PRD, or all Tableau servers with `--publish`, `--publish-tst`, `--publish-prd`
- **Incremental updates** — Only recent history is fetched and merged on subsequent runs, with a safety check preventing cache shrinkage beyond a configurable threshold (default: 2%)
- **Memory-efficient pipeline** — Intermediate DataFrames are freed eagerly, stats are computed once and shared across logger and console display
