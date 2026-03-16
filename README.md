# ETL Pipeline

ETL pipeline that extracts data from a Tibco database via ODBC, processes it with Polars, and exports to Tableau Hyper files. Optionally publishes directly to Tableau Server.

## Project Structure

```
ETL-pipeline-test/
├── main.py                              # CLI entry point
├── config.yaml                          # Configuration (database, Tableau, paths)
├── conversion/
│   ├── shared.py                        # Shared utilities (query, cache, export, publish)
│   ├── console.py                       # Rich console output (progress, tables, colors)
│   ├── stories_table.py                 # Stories pipeline
│   └── epics_table.py                   # Epics pipeline + ACRP
├── sql/                                 # SQL query files
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

Requires the internal `common` package for database connectivity, logging, and Tableau publishing.

## Configuration

All settings are in `config.yaml`:

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

```bash
# Run the full pipeline (stories + epics)
python main.py

# Run a single pipeline
python main.py --stories
python main.py --epics

# Run and publish to Tableau
python main.py --publish
python main.py --stories --publish
python main.py --epics --publish

# Update history caches only (no hyper export)
python main.py --update-cache
python main.py --update-cache --stories
python main.py --update-cache --epics

# Test database connection
python main.py --test
```

## Pipelines

### Stories

1. Fetches summary data and history data
2. Unions summary and history
3. Joins with epic data
4. Applies column transformations and exports to `STORIES.hyper`
5. Optionally publishes to Tableau Server (`--publish`)

### Epics

1. Fetches summary data and history data
2. Unions summary and history
3. Exports to `EPICS.hyper`
4. Builds ACRP release range view and exports to `EPICS_ACRP.hyper`
5. Optionally publishes both hyper files to Tableau Server (`--publish`)

### ACRP (Active Capability Release Plan)

A derived view built from the epics data that identifies features and sub-capabilities and maps their target release ranges. Steps:

1. Filters to rows where `SNAPSHOT_DATE` is null and `FEATURE_KEY` or `SUBCAPABILITY_KEY` is not null
2. Splits comma-delimited `FEATURE_FIX_VERSION` values into individual rows
3. Summarizes min/max target release per `FEATURE_KEY`
4. Joins the release range back to produce the final dataset

## Features

- **Polars** — Multi-threaded operations, native anti-joins, and lazy parquet I/O for fast processing at 4M+ rows.
- **Lazy caching** — History data is cached as parquet. On subsequent runs, `scan_parquet` lazily reads only the rows needed for the anti-join merge, avoiding full cache loads into memory.
- **Backups** — Before overwriting a hyper file, the previous version is saved to `backups/` with a timestamp. Old backups are automatically pruned (default: keep 5).
- **Summary stats** — Each pipeline step logs a formatted table with column dtypes, null counts/percentages, unique values, min/max, and memory usage.
- **Rich console output** — Color-coded progress spinners, step indicators, and formatted summary tables in the terminal.
- **Tableau publishing** — Optionally publish hyper files directly to Tableau Server with `--publish`.
