# conversion/

ETL pipeline modules that extract data from Tibco via ODBC, transform it with Polars, and export to Tableau Hyper files.

## File Overview

| File | Description |
|------|-------------|
| `shared.py` | Shared utilities — config & paths, SQL loading and ODBC queries, dtype cleaning and schema alignment, history cache, synthetic snapshots, the "summary owns today" filter, column renaming, backup rotation, Hyper export, Tableau publishing |
| `stories_table.py` | Stories pipeline — story summary ∪ history, joined to the feature lookup on `FEATURE_ID` + `SNAPSHOT_DATE`; `build_stories()` |
| `epics_table.py` | Epics pipeline — epic summary ∪ history with the hierarchy above each epic, agile rollups and sprint lookups; `build_epics()` returns the EPICS and ACRP frames |
| `burnup_table.py` | Feature burn-up — unpivots date columns into long-format date events; `build_burnup()` |
| `console.py` | Rich terminal output — spinners, colored summary tables, prompt guard for credential re-entry |

## Architecture

Each pipeline module has the same layout, top to bottom:

1. **Fetch** — `fetch_*()` run a config-named SQL file (`run_query`) and normalize dtypes right away (`clean_dtypes` with the matching `schemas.datatypes` mapping). `run()` fires the independent queries concurrently through `parallel_fetch`.
2. **Cache** — `update_history` merges the recent history query into the parquet cache with a lazy anti-join; `fill_missing_snapshots` synthesizes missing weekly snapshots from the summary.
3. **Build** — `build_stories()` / `build_epics()` are pure `DataFrame -> DataFrame` functions with an injectable `now`: drop today's history, union summary + history, join the lookups, apply transforms, rename to Title Case. They are what `tests/` exercise.
4. **Export** — `export_hyper` writes Tableau Hyper via Arrow → pantab.
5. **Publish** — `publish_hyper` uploads to the configured Tableau servers.

`run()` in each module is orchestration only (spinners, logging, cache, export, publish). New data logic belongs in a build function, with a test next to it.

## Key Implementation Details

- `run_query` uses the ODBC driver (returns pandas), then converts via `pl.from_pandas()`. All-null columns become `Utf8` and `Datetime("ns")` becomes `Datetime("us")` immediately, so cached and fresh frames never clash on units.
- `clean_dtypes` understands `datetime`, `date`, `float` and `string`. String-typed date columns are parsed with `str.to_datetime` (the String → Datetime cast is deprecated in Polars and only accepts ISO "T" timestamps).
- `drop_todays_history` implements the "summary owns today" rule for both pipelines; `fill_missing_snapshots` never synthesizes today for the same reason.
- `_align_schemas` reconciles column types and order before `pl.concat` — Polars is strict about schema matching. Handles `Null` vs typed columns, missing columns, `Date` vs `Datetime`, and `us` vs `ns` units.
- `JOIN_NULLS_KWARG` resolves the `join_nulls` → `nulls_equal` rename (Polars 1.24) once. The stories feature join needs NULL-equal matching so live stories (NULL `SNAPSHOT_DATE`) meet live features.
- `export_hyper` hands pantab an Arrow table (Polars → Arrow is near zero-copy). All-null columns are cast to string first to prevent pantab's "unsupported Arrow type: na" error.
- Batch `n_unique()` is wrapped in try/except to handle unsupported types like `Decimal`, with per-column fallback.
- `csm_commonlib` is imported lazily inside the functions that use it; `get_logger` falls back to stdlib logging, and the pipeline modules take it from `conversion.shared`, so everything here imports without the proprietary package.

## Caching

History data is cached as parquet in `../cache/`. On each run:
- If no cache exists, the full history query runs and seeds the cache (`Ahist.sql` is windowed to the last 12 months; `EpicHistory.sql` is not)
- If a cache exists, `scan_parquet` lazily reads it — the anti-join with recent data runs without loading the full cache into memory
- Only the filtered rows (not in recent) are collected, merged with recent data, and written back
- A safety check prevents the cache from shrinking by more than 2% (`--force` overrides it)
- The merge warns when the recent query returns columns the cache lacks (or vice versa): a column added to a history SQL file only reaches the recent window, and `_align_schemas` fills it with nulls for every cached snapshot — which downstream looks like a broken join. `--rebuild-cache` reseeds the cache from the full history query (the old file is backed up first)

## Epics specifics

- **Grain.** The agile rollup is per `FEATURE_ID` + `PROGRAM_INCREMENT` (+ `SNAPSHOT_DATE` for history), so joining it fans an epic out to one row per PI its feature has stories in. That is how `PROGRAM_INCREMENT` and the points columns reach the epic row; features without stories keep one row with nulls.
- **Sprint lookups** (`SprintLookups`): min/max sprint per snapshot + PI (history) and per PI (summary), plus the sprint whose date range contains today. History rows use the per-snapshot lookup, summary rows the PI-level fallback. `IP` sorts after every numbered sprint.
- **Feature dates** use the workbook's names: `PLANNED_START` / `PLANNED_END` are the database `TARGET_START` / `TARGET_END` and come from both tables, so each snapshot keeps its own value.
- **Baseline columns** (`BASELINE_COLUMNS`, currently `BASELINE_PLANNED_END`): the history table has no `PLANNED_END`, so the history queries select NULL and `carry_baseline_from_summary` fills every snapshot row from the feature's current summary value, keyed on `FEATURE_KEY`. Rows already holding a value keep it; features gone from the summary stay null.
- **ACRP** is built from the transformed frame *before* the `SNAPSHOT_DATE` fill: summary rows (NULL snapshot) at feature or sub-capability level, `FEATURE_FIX_VERSION` exploded on commas, min/max target release per `FEATURE_KEY`. Output stays SCREAMING_SNAKE_CASE.
