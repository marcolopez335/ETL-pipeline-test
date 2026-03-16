# schemas/

Expected column dtype definitions used by `clean_dtypes()` during pipeline execution.

## Usage

Each pipeline imports its schema dict from `schemas.datatypes`:

```python
from schemas.datatypes import EXPECTED_DTYPES_STORIES
from schemas.datatypes import EXPECTED_DTYPES_EPICS
```

These are passed to `clean_dtypes(df, schema)` which casts columns to the expected types:

| Schema Value | Pandas Cast | Polars Cast |
|-------------|-------------|-------------|
| `"datetime"` | `pd.to_datetime(errors="coerce")` | `pl.Datetime` |
| `"float"` | `pd.to_numeric(errors="coerce")` | `pl.Float64` |
| `"string"` | `.astype("string").str.strip()` | `pl.Utf8` + `str.strip_chars()` |

## Notes

- Columns not present in the DataFrame are silently skipped
- All casts use coerce/non-strict mode to avoid runtime errors on bad data
- Add new dtype mappings here when new columns are added to the SQL queries
