# sql/

SQL query files executed by the ETL pipelines against the Tibco database.

## Naming Convention

| Pattern | Purpose |
|---------|---------|
| `*Summary.sql` | Current-state summary query (no history) |
| `*History.sql` | Full historical snapshot query (used to seed the cache) |
| `*History_recent.sql` | Recent history only (used for incremental cache updates) |

## Expected Files

### Stories
- `StorySummary.sql` — Current stories with epic join data
- `EpicsFull.sql` — Epics lookup for the stories join
- `StoryHistory.sql` — Full story snapshot history
- `StoryHistory_recent.sql` — Recent story snapshots

### Epics
- `EpicSummary.sql` — Current features summary (one row per feature)
- `EpicHistory.sql` — Full feature snapshot history
- `EpicHistory_recent.sql` — Recent feature snapshots

## Notes

- Filenames are referenced in `config.yaml` under each pipeline's section (`sql_summary`, `sql_history_full`, `sql_history_recent`, etc.)
- History queries must return a `SNAPSHOT_DATE` column and the pipeline's key column (e.g., `STORY_NUMBER`, `FEATURE_KEY`)
- The epic queries use a CTE hierarchy: feature -> subcapability -> customercapability -> customerepic. The Epic level is commented out so each feature is one row (joining epics repeated the feature once per child epic)
- Planned-end naming is intentional — do not "correct" it:

  | Query | Database column | Output column |
  |-------|-----------------|---------------|
  | Epic queries (summary + history) | `TARGET_END` | `PLANNED_END` |
  | `EpicSummary.sql` | `PLANNED_END_DATE` | `BASELINE_PLANNED_END` |
  | `EpicHistory.sql` / `EpicHistory_recent.sql` | *(none)* — `NULL` | `BASELINE_PLANNED_END` |
  | `FeatureBurnup.sql` | `PLANNED_END` | `PLANNED_END` (raw name, not renamed) |

  The epics substitute `TARGET_END` as their planned end; the burn-up uses the real `PLANNED_END`. `PLANNED_END_DATE` exists only in `AMMM_JIRA_EPIC_SUMMARY`; `AMMM_JIRA_EPIC_HISTORY` has no such column, so the history queries select `NULL AS BASELINE_PLANNED_END` (selecting the missing column makes Tibco reject the query, which surfaces in Python as a misleading `UnicodeDecodeError` from `cur.execute`).
- `ORDER BY` clauses in these queries do not affect pipeline logic — they are for manual inspection only
