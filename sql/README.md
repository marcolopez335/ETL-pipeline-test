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
- `Asum.sql` — Current stories snapshot
- `Ahist.sql` — Full story snapshot history
- `Ahist_recent.sql` — Recent story snapshots
- `EsumEhist.sql` — Epic/feature attributes joined onto stories

### Epics pipeline
- `EpicSummary.sql` — Current epics summary (CTE hierarchy)
- `EpicHistory.sql` — Full epic snapshot history
- `EpicHistory_recent.sql` — Recent epic snapshots
- `AgileHistory.sql` — Story-point rollups per feature and snapshot
- `AgileSummary.sql` — Story-point rollups per feature (current)
- `AgileSprintRange.sql` — Sprint names and dates per snapshot/PI
- `AgileSprintRange_summary.sql` — Sprint names and dates (current)
- `FeatureBurnup.sql` — Feature rows with raw date fields for the burn-up output

## Notes

- Filenames are referenced in `config.yaml` under each pipeline's section (`sql_summary`, `sql_history_full`, `sql_history_recent`, etc.) — the code never hardcodes them
- History queries must return a `SNAPSHOT_DATE` column and the pipeline's key column (e.g., `STORY_NUMBER`, `EPIC_KEY`)
- Column names are SCREAMING_SNAKE_CASE because that is what the Tibco database returns
- The epic SQL uses a CTE hierarchy: epic -> feature -> subcapability -> customercapability -> customerepic
- `ORDER BY` clauses in these queries do not affect pipeline logic — they are for manual inspection only
