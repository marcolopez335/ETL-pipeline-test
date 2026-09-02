# schemas/

Expected column dtypes applied by `conversion.shared.clean_dtypes()` right after each query returns.

## Mappings

One mapping per pipeline, covering every query that pipeline runs (columns a query does not return are skipped):

| Mapping | Queries it describes |
|---------|----------------------|
| `EXPECTED_DTYPES_STORIES` | `Asum.sql`, `Ahist.sql`, `Ahist_recent.sql` (stories) and `EsumEhist.sql` (feature lookup) |
| `EXPECTED_DTYPES_EPICS` | `EpicSummary.sql`, `EpicHistory.sql`, `EpicHistory_recent.sql` (epic hierarchy), `AgileHistory.sql`, `AgileSummary.sql` (rollups) and `AgileSprintRange*.sql` (sprint ranges) |

## Types

| Schema value | Polars cast |
|--------------|-------------|
| `"datetime"` | `Datetime("us")` — string sources are parsed with `str.to_datetime` |
| `"date"` | `Date` — daily grain (epics `SNAPSHOT_DATE`, agile join keys) |
| `"float"` | `Float64` |
| `"string"` | `Utf8` + `str.strip_chars()` |

## Rules

- Keys must match the SQL output names exactly (SCREAMING_SNAKE_CASE, as Tibco returns them). `tests/test_schemas.py` parses the SQL select lists and fails on any key no query returns.
- Columns not present in a DataFrame are skipped, so one mapping covers every query a pipeline runs: summary, history and its lookups.
- Casts are non-strict: unconvertible values become null rather than raising.
- Join keys must agree on both sides: `SNAPSHOT_DATE` is `datetime` for stories and their feature lookup, `date` for epics and the agile data.
- Columns of uncertain database type (`BV`, `SWAG`, `SPRINT_COUNT`) are left out on purpose so their native type reaches the `.hyper` file unchanged.
- When a SQL query gains a column that needs a cast, add it here in the same commit.
