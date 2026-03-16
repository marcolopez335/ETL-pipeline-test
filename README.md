# ODBC Data Pipeline

ETL pipeline that extracts data from a Tibco database via ODBC, processes it with pandas, and exports to Tableau Hyper files.

## Project Structure

```
ODBC-test/
├── main.py                          # CLI entry point
├── conversion/
│   ├── shared.py                    # Shared utilities (query, caching, export)
│   ├── stories_table_python.py      # Stories pipeline
│   └── epics_table_python.py        # Epics pipeline
├── sql/                             # SQL query files
├── schemas/                         # Column dtype definitions
├── tools/                           # Utilities (spinner, etc.)
├── cache/                           # Parquet history caches (auto-generated)
├── output/                          # Hyper file output (auto-generated)
└── logs/                            # Log files (auto-generated)
```

## Setup

```bash
python -m venv .odbcenv
.odbcenv\Scripts\activate
pip install pandas pantab tableauhyperapi pyarrow
```

Requires the internal `common` package for database connectivity and logging.

## Usage

```bash
# Run the full pipeline (stories + epics)
python main.py

# Run a single pipeline
python main.py --stories
python main.py --epics

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

### Epics

1. Fetches summary data (`EpicSummary.sql`) and history data (`EpicHistory.sql` / `EpicHistory_recent.sql`)
2. Unions summary and history
3. Exports to `EPICS.hyper`

## Caching

History data is cached as parquet files in `cache/`. On subsequent runs, only recent history is fetched and merged with the existing cache to reduce query load. A full rebuild is triggered automatically if no cache exists.
