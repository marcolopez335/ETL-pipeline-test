# conversion/

ETL pipeline modules that extract data from Tibco via ODBC, transform it with Polars, and export to Tableau Hyper files.

## File Overview

| File | Description |
|------|-------------|
| `shared.py` | Shared utilities — config loading, SQL execution, lazy caching, schema alignment, backup rotation, Hyper export, Tableau publishing |
| `stories_table.py` | Stories pipeline — fetches summary + epics, joins, adds computed columns, exports to Hyper |
| `epics_table.py` | Epics pipeline — fetches summary + history, builds ACRP release range view, exports to Hyper |
| `console.py` | Rich terminal output — spinners, colored summary tables, progress indicators |

## Architecture

Each pipeline follows the same pattern:

1. **Fetch** — Run SQL against Tibco database via ODBC (`run_query`)
2. **Clean** — Cast columns to expected dtypes (`clean_dtypes`)
3. **Cache** — Incremental history updates using lazy parquet scans (`update_history`)
4. **Transform** — Pipeline-specific logic (joins, computed columns, ACRP)
5. **Export** — Write to Tableau Hyper via pantab (`export_hyper`)
6. **Publish** — Optional upload to Tableau Server (`publish_hyper`)

## Key Implementation Details

- `run_query` uses the ODBC driver (returns pandas), then converts via `pl.from_pandas()`. Any all-null columns are cast from `Null` to `Utf8` immediately.
- `export_hyper` converts back to pandas for pantab compatibility. All-null columns in the pandas DataFrame are forced to `str` dtype to prevent pantab's "unsupported Arrow type: na" error.
- `_align_schemas` reconciles column types and order before `pl.concat` — Polars is strict about schema matching. Handles `Null` vs typed columns, missing columns, and type mismatches.
- Batch `n_unique()` is wrapped in try/except to handle unsupported types like `Decimal`, with per-column fallback.

## Caching

History data is cached as parquet in `../cache/`. On each run:
- If no cache exists, a full history query runs and seeds the cache
- If a cache exists, `scan_parquet` lazily reads it — the anti-join with recent data runs without loading the full cache into memory
- Only the filtered rows (not in recent) are collected, merged with recent data, and written back
- A safety check prevents the cache from shrinking by more than 2%

## ACRP (Active Capability Release Plan)

Built from the epics data. Filters rows where `SNAPSHOT_DATE` is null and either `FEATURE_KEY` or `SUBCAPABILITY_KEY` is not null (feature/subcap levels of the hierarchy). Splits `FEATURE_FIX_VERSION` on commas and computes min/max target release per `FEATURE_KEY`.
