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
│   ├── shared.py                        # Shared utilities (query, cache, export, publish)
│   ├── console.py                       # Rich console output (progress, tables, SQL shell)
│   ├── stories_table.py                 # Stories pipeline
│   └── epics_table.py                   # Epics pipeline + ACRP + sprint range
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

tableau:
  server_url: "https://tableau.example.com"
  site_id: "your-site"
  project_name: "Your Project"
  overwrite: true

backup:
  enabled: true
  max_backups: 5
```

## Usage

| Command | Description |
|---------|-------------|
| `python main.py` | Run all pipelines (stories + epics) |
| `python main.py --stories` | Run stories pipeline only |
| `python main.py --epics` | Run epics pipeline only |
| `python main.py --publish` | Run and publish to Tableau Server |
| `python main.py --epics --publish` | Run epics and publish |
| `python main.py --update-cache` | Update history caches only (no export) |
| `python main.py --update-cache --stories` | Update stories cache only |
| `python main.py --test` | Test database connection |
| `python main.py --epics --query` | Run epics then open SQL shell |
| `python main.py --query` | Run all pipelines then open SQL shell |

Flags can be combined: `python main.py --stories --publish --query`

## Pipelines

### Stories

1. Fetch summary data and history snapshots from Tibco
2. Union summary with incremental history cache
3. Join with epics lookup data
4. Apply transformations (`LAST_UPDATED`, `PROJECT_NAME_VERSION`, column renaming)
5. Export to `STORIES.hyper`
6. Optionally publish to Tableau Server

### Epics

1. Fetch summary data and history snapshots from Tibco
2. Union summary with incremental history cache
3. Apply transformations (`LAST_UPDATED`, sprint version parsing, `MIN_SPRINT`/`MAX_SPRINT`)
4. Export to `EPICS.hyper`
5. Build ACRP release range view and export to `EPICS_ACRP.hyper`
6. Optionally publish both hyper files to Tableau Server

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
| `exit` | Exit the SQL shell |

**Available tables:** `stories`, `epics`, `acrp` (depending on which pipelines ran)

> Results are capped at 100 rows by default. Use `LIMIT` to override.

## Architecture

```
   Tibco DB ──ODBC──> run_query() ──> pl.DataFrame
                                          │
                          ┌───────────────┤
                          │               │
                    summary data    history cache
                          │          (scan_parquet)
                          │               │
                          └──── union ────┘
                                  │
                           data_functions()
                            (transforms)
                                  │
                        ┌─────────┴─────────┐
                        │                   │
                   export_hyper()      build_acrp()
                     (pantab)               │
                        │              export_hyper()
                        │                   │
                   publish_hyper()    publish_hyper()
                    (optional)         (optional)
```

## Features

- **Polars** — Multi-threaded DataFrame operations, native anti-joins, and Arrow-based memory for fast processing at 4M+ rows
- **Lazy caching** — History data cached as parquet; `scan_parquet` lazily reads only the rows needed for the incremental merge, avoiding full cache loads into memory
- **Interactive SQL** — Query final DataFrames with standard SQL via `--query` for debugging and data validation
- **Sprint parsing** — Extracts sprint versions from names, handles IP sprints, and computes min/max per snapshot and program increment using numeric sort keys
- **Automatic backups** — Previous hyper files are timestamped and saved before overwrite, with configurable rotation (default: keep 5)
- **Summary statistics** — Each step logs a formatted table with column dtypes, null counts/percentages, unique values, min/max, and memory usage
- **Rich console output** — Color-coded progress spinners, step indicators, and formatted tables via [Rich](https://github.com/Textualize/rich)
- **Tableau publishing** — Publish hyper files directly to Tableau Server with `--publish`
- **Incremental updates** — Only recent history is fetched and merged on subsequent runs, with a safety check preventing cache shrinkage beyond 2%
