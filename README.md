# AMMM Jira ETL Pipeline

A production ETL pipeline that extracts Jira backlog data from a Tibco database via ODBC, transforms it with [Polars](https://pola.rs/), and exports to Tableau Hyper files for reporting and analytics.

---

## Table of Contents

- [Project Structure](#project-structure)
- [Setup](#setup)
- [Configuration](#configuration)
- [Usage](#usage)
- [Data Model](#data-model)
- [Pipelines](#pipelines)
- [Interactive SQL Query Mode](#interactive-sql-query-mode)
- [Dashboard Mockup](#dashboard-mockup)
- [Synthetic Snapshots](#synthetic-snapshots)
- [Architecture](#architecture)
- [Testing](#testing)
- [Features](#features)

---

## Project Structure

```
ETL-pipeline-test/
├── main.py                              # CLI entry point
├── config.yaml                          # Configuration (database, Tableau, paths, SQL file names)
├── CLAUDE.md                            # Project brief: data model, conventions, definition of done
├── requirements.txt                     # Python dependencies
├── conversion/
│   ├── shared.py                        # Shared utilities (config, query, cache, snapshots, export, publish)
│   ├── console.py                       # Rich console output (progress, tables)
│   ├── stories_table.py                 # Stories pipeline
│   ├── epics_table.py                   # Epics pipeline + ACRP + sprint range
│   └── burnup_table.py                  # Feature burn-up output
├── sql_shell/                           # Standalone interactive SQL shell (reusable)
│   ├── __init__.py
│   ├── __main__.py                      # CLI: python -m sql_shell data.parquet
│   ├── shell.py                         # REPL loop, command parsing
│   └── display.py                       # Rich table rendering
├── sql/                                 # SQL query files (CTE hierarchy)
├── schemas/                             # Column dtype definitions (checked against the SQL by tests)
├── tests/                               # pytest suite -- runs without the database or csm_commonlib
├── dashboard/                           # Tableau-style dashboard mockup on mock STORIES data
│   ├── index.html                       # Self-contained dashboard (filters, KPIs, burn-up, grid)
│   ├── generate_mock_data.py            # Deterministic mock data generator (stdlib only)
│   └── data/                            # Generated mock extract (JSON + CSV)
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

> Requires the internal `csm_commonlib` package (`csm_commonlib.database.tibco`, `csm_commonlib.logging`, `csm_commonlib.tableau`) for database connectivity, logging, and Tableau publishing. The build and test code runs without it — see [Testing](#testing).

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
  external:            # published only with --publish-external
    server_url: "https://tableau-external.example.com"
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
| `--publish` | Publish to the internal Tableau servers (tst + prd) |
| `--publish-tst` | Publish to TST only |
| `--publish-prd` | Publish to PRD only |
| `--publish-external` | Publish to the external Tableau server — never implied by `--publish` |

**Options:**

| Flag | Description |
|------|-------------|
| `--force` | Bypass cache shrinkage safety check (use if cache needs to shrink) |
| `--rebuild-cache` | Ignore the existing history cache and reseed it from the full history query — run once after adding a column to a history SQL file |
| `--verbose` | Show full per-column stats tables after each step (slower at large row counts) |

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

# Reseed the epics cache after adding a column to EpicHistory*.sql
python main.py --epics --rebuild-cache

# Explore cached data without hitting the database
python main.py --query-only

# Run epics pipeline, then drop into SQL shell to inspect results
python main.py --epics --query

# Publish both pipelines to production
python main.py --publish-prd

# Publish to the external server (always requires the explicit flag)
python main.py --publish-external
```

Flags can be combined freely: `python main.py --stories --publish-tst --query`

## Data Model

Both pipelines have the same shape: a **summary** query (the live state — `SNAPSHOT_DATE` is NULL) unioned with a cached **history** of weekly snapshots, then joined to the level above it.

```
STORIES  =  (story history ∪ story summary)
            ⟕ (feature history ∪ feature summary)        on FEATURE_ID + SNAPSHOT_DATE

EPICS    =  (epic history ∪ epic summary)                 one row per Epic, with the hierarchy
            ⟕ agile rollups  (points per feature + PI)    above it flattened on: Feature →
            ⟕ sprint lookups (min / max / current sprint) Sub-Capability → Customer Capability → Customer Epic
```

| Output | Grain | Columns | Built by |
|--------|-------|---------|----------|
| `STORIES.hyper` | story × snapshot | Title Case | `stories_table.build_stories()` |
| `EPICS.hyper` | epic × program increment × snapshot | Title Case | `epics_table.build_epics()` |
| `EPICS_ACRP.hyper` | feature fix version (summary rows only) | SCREAMING_SNAKE_CASE | `epics_table.build_acrp()` |
| `FEATURE_BURNUP.hyper` | feature × date event | SCREAMING_SNAKE_CASE | `burnup_table.build_burnup()` |

Rules that hold everywhere:

- **The summary owns today.** History rows dated today are dropped before the union and the summary rows are stamped with today's date, so nothing is double-counted on snapshot days. The cache still stores the official snapshot; tomorrow's export serves today from history.
- **`SNAPSHOT_DATE` is the time axis.** NULL means "live" until the final stamp. It is a timestamp in `STORIES.hyper` and a date in `EPICS.hyper` — the types the workbooks were built on.
- **Casing.** SCREAMING_SNAKE_CASE from the database through the build; Title Case only at export. The `build_*` functions are pure DataFrame → DataFrame and are what the tests exercise.
- **EPICS date names are the workbook's, not the database's.** `Planned Start` / `Planned End` are the database `TARGET_START` / `TARGET_END` of the feature; `Baseline Planned End` is the database `PLANNED_END`. Confusing, but that is what the workbooks reference — keep it.
- **Baseline columns come from the summary.** The history table has no `PLANNED_END`, so the history queries select `NULL AS BASELINE_PLANNED_END` and the epics build fills every snapshot row from the feature's current value: one baseline per feature across time. Declare such columns in the schema — an all-NULL column arrives untyped, and without the cast the union would turn the summary's dates into text.
- **A new column in a history query needs a cache rebuild.** The incremental update only refreshes the last 30 days, so every snapshot already in the parquet cache would stay null for the new column. The pipeline warns when it sees this; run once with `--rebuild-cache`.

## Pipelines

### Stories

1. Fetch the story summary, story history and feature lookup from Tibco (in parallel)
2. Update the incremental history cache
3. Fill missing weekly snapshots (synthetic)
4. Drop history rows dated today — the live summary supplies today's rows (the cache still keeps the official snapshot)
5. Union summary with history and join the feature lookup on `FEATURE_ID` + `SNAPSHOT_DATE`
6. Apply transformations (`LAST_UPDATED`, `PROJECT_NAME_VERSION`, `SPRINT_NAME_ALT`, `SNAPSHOT_DATE_ALT`, `PI_FROM_SPRINT`) and rename to Title Case
7. Export to `STORIES.hyper`
8. Optionally publish to Tableau Server

### Epics

1. Fetch the epic summary, epic history, agile rollups, sprint ranges and burn-up source from Tibco (in parallel)
2. Update the incremental history cache
3. Fill missing weekly snapshots (synthetic)
4. Drop history rows dated today (same rule as stories); join the agile rollups onto history (`FEATURE_KEY` + `SNAPSHOT_DATE`) and onto summary (`FEATURE_KEY`)
5. Union summary with history; carry baseline columns (`BASELINE_PLANNED_END`) from the live summary onto every snapshot row of the same feature; apply transformations (`LAST_UPDATED`, `SNAPSHOT_DATE_ALT`, `MIN_SPRINT` / `MAX_SPRINT`, `CURRENT_SPRINT`)
6. Build the ACRP release range view from the summary rows, then stamp summary rows with today's `SNAPSHOT_DATE` and rename to Title Case
7. Build the feature burn-up view
8. Export `EPICS.hyper`, `EPICS_ACRP.hyper` and `FEATURE_BURNUP.hyper`
9. Optionally publish all hyper files to Tableau Server

### Feature Burn-Up

A long-format date-event dataset built from `FeatureBurnup.sql` (Feature rows of the epic summary source, with `SUMMARY` pulled from the Customer Epic level):

1. Renames the database `LAST_UPDATED` to `SNAPSHOT_DATE` (midnight-normalized); keeps the raw value as `IMET_SUMMARY_LAST_UPDATED`; derives `SOURCE_TYPE` (`"summary"`) and `TARGET_END_REF`
2. Unpivots `TARGET_END` / `RESOLVED` / `PLANNED_END` into `DATE_TYPE` + `DATE_VALUE` (midnight-normalized) rows
3. Flags per row: `DONE_FEATURES` (resolved + status done/accepted), `PROJECTED_FEATURES` (target end + status **not** done/accepted/cancelled), `PLANNED_FEATURES` (planned end, any status)
4. Adds a fresh `LAST_UPDATED` = pipeline run time converted to US Central (DST-aware) as a data-freshness stamp
5. Drops rows with a null `DATE_VALUE`

### ACRP (Active Capability Release Plan)

A derived view from epics data that maps features and sub-capabilities to their target release ranges. Built from the transformed frame *before* the `SNAPSHOT_DATE` fill, so it holds the live summary rows only:

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

**Available tables:** `stories`, `epics`, `acrp`, `burnup` (depending on which pipelines ran)

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

## Dashboard Mockup

[`dashboard/`](dashboard/README.md) contains a self-contained, Tableau-style
dashboard mockup built on fabricated data shaped like the `STORIES.hyper`
extract: a PI / Program / Team filter shelf, KPI tiles, a burn-up over the
weekly snapshots, and a spreadsheet-like feature completion grid with health
chips and expandable story detail. Open `dashboard/index.html` in a browser —
no server or dependencies. Regenerate the mock data with
`python dashboard/generate_mock_data.py` (add `--hyper` to also produce
`output/STORIES_MOCK.hyper` via pantab).

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
   Tibco DB ──ODBC──> run_query() ──> pl.DataFrame ──> clean_dtypes()
                                          │
                          ┌───────────────┼───────────────┐
                          │               │               │
                    summary data    history cache      lookups
                          │          (scan_parquet)    (features / agile / sprints)
                          │               │               │
                          │    fill_missing_snapshots()   │
                          │    drop_todays_history()      │
                          │               │               │
                          └──── union ────┴──── join ─────┘
                                  │
                      build_stories() / build_epics()
                     (transforms, ACRP, Title Case rename)
                                  │
                        ┌─────────┴─────────┐
                        │                   │
                   export_hyper()      build_burnup()
                   (Arrow → pantab)         │
                        │              export_hyper()
                        │                   │
                   publish_hyper()    publish_hyper()
               (tst / prd / external) (tst / prd / external)
```

## Testing

The build chains (`build_stories`, `build_epics`, `build_acrp`, `build_burnup`) and the shared helpers are pure Polars, so the suite runs without the database or `csm_commonlib`:

```bash
pip install pytest
pytest                                          # tests/ — in-memory frames through the real union / join / transform code
ruff check . --select E,F,W --ignore E501       # lint, same command as CI
```

`tests/test_schemas.py` parses each query's select list and fails if `schemas/datatypes.py` names a column no query returns, so a renamed SQL column can't silently lose its cast. CI (`.gitlab-ci.yml`) runs both commands.

## Features

- **Polars** — Multi-threaded DataFrame operations, native anti-joins, and Arrow-based memory for fast processing at 4M+ rows
- **Zero-copy Hyper export** — Exports via Polars → Arrow → pantab, bypassing pandas entirely to avoid memory doubling on large datasets
- **Lazy caching** — History data cached as `.parquet`; `scan_parquet` lazily reads only the rows needed for the incremental merge, avoiding full cache loads into memory
- **Synthetic snapshots** — Automatically fills missing snapshots from summary data; replaced by real data on the next pipeline run. Snapshot day and lookback weeks are configurable.
- **Interactive SQL** — Query final DataFrames with standard SQL via `--query` for debugging and data validation; also available standalone via `python -m sql_shell`
- **Sprint parsing** — Extracts sprint versions from names, handles IP sprints, and computes min/max per snapshot and program increment using numeric sort keys
- **Automatic backups** — Previous hyper files and cache files are timestamped and saved before overwrite, with configurable rotation
- **Summary statistics** — With `--verbose`, each step logs a formatted table with column dtypes, null counts/percentages, unique values, min/max, and memory usage; by default a fast one-line summary is shown
- **Parallel fetching** — Independent SQL queries run concurrently (configurable via `database.parallel_fetch` / `database.max_parallel_queries`)
- **Rich console output** — Color-coded progress spinners, step indicators, and formatted tables via [Rich](https://github.com/Textualize/rich)
- **Multi-environment Tableau publishing** — Publish hyper files to TST and PRD with `--publish` (or one of them with `--publish-tst` / `--publish-prd`); the external server publishes only with an explicit `--publish-external`
- **Incremental updates** — Only recent history is fetched and merged on subsequent runs, with a safety check preventing cache shrinkage beyond a configurable threshold (default: 2%)
- **Memory-efficient pipeline** — Intermediate DataFrames are freed eagerly, stats are computed once and shared across logger and console display
