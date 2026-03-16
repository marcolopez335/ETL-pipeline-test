# ETL Pipeline

ETL pipeline that extracts data from a Tibco database via ODBC, processes it with pandas, and exports to Tableau Hyper files. Optionally publishes directly to Tableau Server.

## Project Structure

```
ETL-pipeline-test/
├── main.py                          # CLI entry point
├── config.yaml                      # Configuration (database, Tableau, paths)
├── conversion/
│   ├── shared.py                    # Shared utilities (query, caching, export, publish)
│   ├── console.py                   # Rich console output (progress, tables, colors)
│   ├── stories_table_python.py      # Stories pipeline
│   └── epics_table_python.py        # Epics pipeline
├── sql/                             # SQL query files
├── schemas/                         # Column dtype definitions
├── tools/                           # Utilities (spinner, etc.)
├── cache/                           # Parquet history caches (auto-generated)
├── output/                          # Hyper file output (auto-generated)
├── backups/                         # Hyper file backups (auto-generated)
└── logs/                            # Log files (auto-generated)
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

1. Fetches summary data (`Asum.sql`) and history data (`Ahist.sql` / `Ahist_recent.sql`)
2. Unions summary and history
3. Joins with epic data (`EsumEhist.sql`)
4. Applies column transformations and exports to `STORIES.hyper`
5. Optionally publishes to Tableau Server (`--publish`)

### Epics

1. Fetches summary data (`EpicSummary.sql`) and history data (`EpicHistory.sql` / `EpicHistory_recent.sql`)
2. Unions summary and history
3. Exports to `EPICS.hyper`
4. Optionally publishes to Tableau Server (`--publish`)

## Features

- **Caching** — History data is cached as parquet files in `cache/`. On subsequent runs, only recent history is fetched and merged with the existing cache to reduce query load. A full rebuild is triggered automatically if no cache exists.
- **Backups** — Before overwriting a hyper file, the previous version is saved to `backups/` with a timestamp. Old backups are automatically pruned (default: keep 5).
- **Summary stats** — Each pipeline step logs row counts, column dtypes, null counts, and null percentages in both the log file and a formatted console table.
- **Rich console output** — Color-coded progress spinners, step indicators, and formatted summary tables in the terminal.
