# sql/

SQL query files executed by the ETL pipelines against the Tibco database.

## Naming

Filenames are legacy and abbreviated in places (`A*` = agile/story data,
`E*` = epic data). The suffix tells you the query's role:

| Suffix | Purpose |
|--------|---------|
| `*sum` / `*Summary` | Current-state summary query (no history) |
| `*hist` / `*History` | Full historical snapshot query (used to seed the cache) |
| `*_recent` | Recent history only (used for incremental cache updates) |

## Files

### Stories pipeline
- `Asum.sql` — Current stories (`sql_summary`); `NULL AS SNAPSHOT_DATE` marks the rows as live
- `Ahist.sql` — Story snapshot history for the last 12 months (`WHERE SNAPSHOT_DATE >= DATEADD('yyyy',-1, CURRENT_TIMESTAMP)`); seeds the cache (`sql_history_full`)
- `Ahist_recent.sql` — Last 30 days of story snapshots, merged into the cache on every run (`sql_history_recent`)
- `EsumEhist.sql` — Feature summary `UNION ALL` feature history, joined onto stories on `FEATURE_ID` + `SNAPSHOT_DATE` (`sql_features`)

### Epics pipeline
- `EpicSummary.sql` — Current epic hierarchy (CTE per level, flattened onto the epic row; `sql_summary`)
- `EpicHistory.sql` — Full epic snapshot history, no date window; seeds the cache (`sql_history_full`)
- `EpicHistory_recent.sql` — Last 30 days of epic snapshots (`sql_history_recent`)
- Feature dates use the workbook's names on purpose, even though they read backwards: `TARGET_START AS PLANNED_START` and `TARGET_END AS PLANNED_END` (both tables have the target dates), while `BASELINE_PLANNED_END` is the database `PLANNED_END`
- Both history queries select `NULL AS BASELINE_PLANNED_END` — `AMMM_JIRA_EPIC_HISTORY` has no `PLANNED_END`; the epics build fills it per feature from `EpicSummary.sql`
- `AgileHistory.sql` — Story-point rollups per feature and snapshot
- `AgileSummary.sql` — Story-point rollups per feature (current)
- `AgileSprintRange.sql` — Sprint names and dates per snapshot/PI
- `AgileSprintRange_summary.sql` — Sprint names and dates (current)
- `FeatureBurnup.sql` — Feature rows with raw date fields for the burn-up output

## Notes

- Filenames are referenced in `config.yaml` under each pipeline's section (`sql_summary`, `sql_history_full`, `sql_history_recent`, `sql_features`, `sql_agile_*`, `sql_burnup`) — the code never hardcodes them
- History queries must return a `SNAPSHOT_DATE` column and the pipeline's key column (`STORY_NUMBER`, `EPIC_KEY`)
- A pipeline's summary and history queries return the same select list (the stories summary selects `NULL AS SNAPSHOT_DATE`; the epic history adds `SNAPSHOT_DATE`) — `tests/test_schemas.py` enforces this and checks `schemas/datatypes.py` against the column names here, so alias every computed column
- Column names are SCREAMING_SNAKE_CASE because that is what the Tibco database returns
- Adding a column to a history query (`Ahist*.sql`, `EpicHistory*.sql`): add it to the summary query and to `schemas/datatypes.py` in the same change, then run the pipeline once with `--rebuild-cache` — the parquet cache was seeded before the column existed and the incremental update only refreshes the last 30 days
- The epic SQL uses a CTE hierarchy: epic -> feature -> subcapability -> customercapability -> customerepic
- `ORDER BY` clauses in these queries do not affect pipeline logic — they are for manual inspection only
