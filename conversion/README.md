# conversion/

ETL pipeline modules that extract data from Tibco via ODBC, transform it, and export to Tableau Hyper files.

## File Overview

| File | Description |
|------|-------------|
| `shared.py` | Shared utilities for the **pandas** pipelines — config loading, SQL execution, caching, backup rotation, Hyper export, Tableau publishing |
| `shared_polars.py` | Same shared utilities rewritten for **Polars** — includes schema alignment (`_align_schemas`) for safe concat operations |
| `stories_table_python.py` | Stories pipeline (pandas) — fetches summary + epics, joins, adds computed columns, exports to Hyper |
| `stories_table_polars.py` | Stories pipeline (Polars) — same logic, optimized for large datasets (4M+ rows) |
| `epics_table_python.py` | Epics pipeline (pandas) — fetches summary + history, builds ACRP release range view, exports to Hyper |
| `epics_table_polars.py` | Epics pipeline (Polars) — same logic with native Polars operations |
| `console.py` | Rich terminal output — spinners, colored summary tables, progress indicators |

## Architecture

Each pipeline follows the same pattern:

1. **Fetch** — Run SQL against Tibco database via ODBC (`run_query`)
2. **Clean** — Cast columns to expected dtypes (`clean_dtypes`)
3. **Cache** — Incremental history updates using parquet files (`update_history`)
4. **Transform** — Pipeline-specific logic (joins, computed columns, ACRP)
5. **Export** — Write to Tableau Hyper via pantab (`export_hyper`)
6. **Publish** — Optional upload to Tableau Server (`publish_hyper`)

## Pandas vs Polars

Both implementations exist side-by-side. The pandas versions (`*_python.py`, `shared.py`) are the original. The Polars versions (`*_polars.py`, `shared_polars.py`) were added for performance at scale.

Key differences in the Polars version:
- `run_query` still uses the ODBC driver (returns pandas), then converts via `pl.from_pandas()`
- `export_hyper` converts back to pandas for pantab compatibility, casting `Null`-typed columns to `Utf8` to avoid Arrow type errors
- `_align_schemas` reconciles column types/order before `pl.concat` (Polars is stricter than pandas about schema matching)
- Batch `n_unique()` is wrapped in try/except to handle unsupported types like `Decimal`

## Caching

History data is cached as parquet in `../cache/`. On each run:
- If no cache exists, a full history query runs and seeds the cache
- If a cache exists, only recent data is fetched and merged via anti-join (avoids re-pulling the full dataset)
- A safety check prevents the cache from shrinking by more than 2%

## ACRP (Active Capability Release Plan)

Built from the epics data. Filters rows where `SNAPSHOT_DATE` is null and either `FEATURE_KEY` or `SUBCAPABILITY_KEY` is not null (feature/subcap levels of the hierarchy). Splits `FEATURE_FIX_VERSION` on commas and computes min/max target release per `FEATURE_KEY`.
