# AMMM Jira ETL pipeline — project brief

Read this first. It is the distilled version of how this project is built and
what "done" means for a change to it.

## What this is

Tibco (ODBC) → Polars → Tableau `.hyper` files for Jira backlog reporting.
Entry point `main.py`; pipelines in `conversion/`; queries in `sql/`; dtype
schemas in `schemas/`; settings and SQL file names in `config.yaml`;
tests in `tests/`.

## Data model — the part to get right

- Two pipelines with the same shape: **summary** (live rows, `SNAPSHOT_DATE`
  is NULL) unioned with **history** (weekly snapshots in a parquet cache),
  then joined to the level above.
- **STORIES** = (story history ∪ story summary) ⟕ (feature history ∪ feature
  summary) on `FEATURE_ID` + `SNAPSHOT_DATE`. `conversion/stories_table.py`.
- **EPICS** = (epic history ∪ epic summary) with everything above the epic
  flattened onto the row (Epic → Feature → Sub-Capability → Customer
  Capability → Customer Epic), ⟕ agile rollups (story points per feature per
  PI — this sets the grain to epic × PI × snapshot), ⟕ sprint lookups
  (min / max / current sprint). Also produces **ACRP** (summary rows only,
  one per fix version, min/max target release per feature) and the
  **feature burn-up**. `conversion/epics_table.py`, `conversion/burnup_table.py`.
- **The summary owns today.** `shared.drop_todays_history` removes history
  rows dated today before the union; `fill_missing_snapshots` never
  synthesizes today. The cache still keeps the official snapshot.
- **EPICS date names are the workbook's, not the database's.** In the epic
  queries `TARGET_START AS PLANNED_START` and `TARGET_END AS PLANNED_END`
  (feature level, present in both tables), while `BASELINE_PLANNED_END`
  is the database `PLANNED_END`. It reads backwards on purpose; do not
  "correct" it.
- **Baseline columns come from the summary.** The history table has no
  `PLANNED_END`; the history queries select `NULL AS BASELINE_PLANNED_END`
  and `epics_table.carry_baseline_from_summary` fills each snapshot row
  from the feature's current value (`BASELINE_COLUMNS`). Declare such
  columns in the schema: an all-NULL column arrives untyped, and without
  the cast the union demotes the summary's dates to text.
- **A column added to a history SQL file needs `--rebuild-cache` once.** The
  parquet cache predates it and the incremental update only refreshes the
  last 30 days, so cached snapshots would stay null for it (the merge warns
  about this drift). Add the column to the summary query and to
  `schemas/datatypes.py` in the same change.
- **Output contracts Tableau depends on — do not change casually:**
  Title Case columns in `STORIES.hyper` / `EPICS.hyper`, SCREAMING_SNAKE_CASE
  in `EPICS_ACRP.hyper` / `FEATURE_BURNUP.hyper`; `SNAPSHOT_DATE` is a
  timestamp for stories and a Date for epics; colliding feature columns keep
  the `_epics` suffix ("Target Start Epics").

## Conventions

- Internal library is `csm_commonlib` (`.logging`, `.database.tibco.TibcoConnection`,
  `.tableau.publish` / `.tableau.session`). It is imported lazily in
  `conversion/shared.py`; pipeline modules take `get_logger` from
  `conversion.shared` so they import (and test) without it.
- SQL file names live only in `config.yaml` (`sql_*` keys). Database columns
  are SCREAMING_SNAKE_CASE; config keys are snake_case.
- Every returned column that needs a cast is declared in
  `schemas/datatypes.py` (`datetime` / `date` / `float` / `string`);
  `tests/test_schemas.py` checks the schema against the SQL select lists.
- `build_*` functions are pure DataFrame → DataFrame with an injectable
  `now`; `run()` only orchestrates (spinners, cache, export, publish). New
  data logic goes in a build function with a test beside it.
- Console output is ASCII-only (Windows code pages). Logging is configured
  once, in `main.py`.

## Definition of done

- `pytest` green and `ruff check . --select E,F,W --ignore E501` clean —
  both run in CI and need neither the database nor `csm_commonlib`.
- Output column names and dtypes unchanged unless the change is about them;
  say so explicitly when they do change.
- `README.md` and the package READMEs updated when behavior or layout changes.
- Work on the assigned `claude/*` branch; merge to `main` through a PR;
  delete feature branches once merged.

## How to ask for changes

Name the output (`STORIES`, `EPICS`, `ACRP`, `BURNUP`), the layer (SQL,
schema, build, run / CLI, publish), and whether output columns may change.
Example: "EPICS, build layer: add `FEATURE_TEAM` from the feature level;
output gains one column." That is enough to route the change to the right
file and test.
