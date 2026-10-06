# schemas/

Expected column dtypes applied by `conversion.shared.clean_dtypes()` right after each query returns.

## Mappings

One mapping per pipeline, covering the queries it casts (columns a query does not return are skipped):

| Mapping | Queries it describes |
|---------|----------------------|
| `EXPECTED_DTYPES_STORIES` | `Asum.sql`, `Ahist.sql`, `Ahist_recent.sql` (stories) and `EsumEhist.sql` (feature lookup) |
| `EXPECTED_DTYPES_EPICS` | `EpicSummary.sql`, `EpicHistory.sql`, `EpicHistory_recent.sql` (one row per feature) |

## Types

| Schema value | Polars cast |
|--------------|-------------|
| `"datetime"` | `Datetime` — string sources are parsed with `str.to_datetime` |
| `"float"` | `Float64` |
| `"string"` | `Utf8` + `str.strip_chars()` |

An unknown schema value raises `ValueError`, so a typo fails loudly instead of skipping the cast.

## Rules

- Keys must match the SQL output names exactly (SCREAMING_SNAKE_CASE, as Tibco returns them).
- Columns not present in a DataFrame are skipped, so one mapping covers every query a pipeline casts: summary, history and its lookups.
- Casts are non-strict: unconvertible values become null rather than raising.
- Join keys must agree on both sides: stories `SNAPSHOT_DATE` is `datetime` for both the stories and the feature lookup.
- Columns of uncertain database type (`BV`, `SWAG`) are left out on purpose so their native type reaches the `.hyper` file unchanged.
- When a SQL query gains a column that needs a cast, add it here in the same commit.
