"""Lightweight regression tests for the pure-Polars helpers in conversion.shared.

Designed to run in CI without the proprietary `common` package — each helper
under test is imported lazily and only the pure-Polars surface is exercised.
Run with: ``python test_fill_snapshots.py``.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

import polars as pl

from conversion.shared import (
    _align_schemas,
    _supertype,
    fill_missing_snapshots,
    get_last_n_snapshots,
    parallel_fetch,
    update_history_cache_with_recent,
    validate_history,
)


_FAILURES: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    if condition:
        print(f"  PASS  {label}")
    else:
        msg = f"{label}{f' — {detail}' if detail else ''}"
        print(f"  FAIL  {msg}")
        _FAILURES.append(msg)


def test_get_last_n_snapshots_returns_requested_weekday() -> None:
    # 2026-04-29 is a Wednesday; last Monday is 2026-04-27
    ref = datetime(2026, 4, 29)
    mondays = get_last_n_snapshots(4, day_of_week=0, from_date=ref)
    check("get_last_n_snapshots returns 4 entries", len(mondays) == 4)
    check("entries are Mondays", all(d.weekday() == 0 for d in mondays))
    check("most-recent Monday is 2026-04-27",
          mondays[0].date() == (ref - timedelta(days=2)).date(),
          detail=f"got {mondays[0].date()}")
    check("entries are 7 days apart",
          all((mondays[i] - mondays[i + 1]).days == 7 for i in range(3)))


def test_supertype_rules() -> None:
    check("identical dtypes return as-is", _supertype(pl.Int64, pl.Int64) == pl.Int64)
    check("Null + Utf8 -> Utf8", _supertype(pl.Null, pl.Utf8) == pl.Utf8)
    check("Int + Float -> Float64", _supertype(pl.Int32, pl.Float32) == pl.Float64)
    check("Date + Datetime -> Datetime",
          _supertype(pl.Date, pl.Datetime) == pl.Datetime)
    check("incompatible falls back to Utf8",
          _supertype(pl.Int64, pl.Utf8) == pl.Utf8)


def test_align_schemas_handles_missing_and_mixed_dtypes() -> None:
    df1 = pl.DataFrame({"id": [1, 2], "value": [1.0, 2.0]})
    df2 = pl.DataFrame({"id": [3], "label": ["x"]})  # different cols + same id but matching dtype

    a, b = _align_schemas(df1, df2)
    check("align: same column set", a.columns == b.columns)
    check("align: column order matches", a.columns == b.columns)
    check("align: missing 'label' added to df1", "label" in a.columns)
    check("align: missing 'value' added to df2", "value" in b.columns)
    check("align: row counts preserved",
          a.height == 2 and b.height == 1,
          detail=f"a={a.height}, b={b.height}")

    # Int + Float should promote to Float64
    df3 = pl.DataFrame({"x": pl.Series([1, 2], dtype=pl.Int64)})
    df4 = pl.DataFrame({"x": pl.Series([1.5, 2.5], dtype=pl.Float64)})
    a, b = _align_schemas(df3, df4)
    check("align: Int + Float promoted to Float64",
          a.schema["x"] == pl.Float64 and b.schema["x"] == pl.Float64)


def test_fill_missing_snapshots_fills_gap_and_marks_synthetic() -> None:
    # Pick a Monday well in the past so today's exclusion doesn't affect us.
    monday = datetime(2026, 4, 6)  # Monday
    cfg = {"snapshots": {"day_of_week": 0, "lookback_weeks": 4}}

    summary = pl.DataFrame({"EPIC_KEY": ["E1", "E2"], "STATUS": ["Open", "Done"]})
    history = pl.DataFrame({
        "EPIC_KEY": ["E1"],
        "STATUS": ["Open"],
        "SNAPSHOT_DATE": [monday.date()],  # only one Monday present
    })

    out = fill_missing_snapshots(summary, history, key_col="EPIC_KEY", config=cfg)
    check("fill: result has IS_SYNTHETIC column", "IS_SYNTHETIC" in out.columns)
    check("fill: original row preserved",
          out.filter(pl.col("EPIC_KEY") == "E1").height >= 1)

    # Should have synthetic rows for the other Mondays in the lookback window
    synth = out.filter(pl.col("IS_SYNTHETIC").fill_null(False))
    check("fill: produced at least one synthetic row", synth.height >= 1)
    check("fill: synthetic rows carry summary keys",
          set(synth["EPIC_KEY"].to_list()).issubset({"E1", "E2"}))


def test_cache_merge_replaces_overlapping_keys() -> None:
    """update_history_cache_with_recent: recent rows replace same-key cached rows."""
    import tempfile
    from pathlib import Path

    cached = pl.DataFrame({
        "EPIC_KEY": ["E1", "E2", "E3"],
        "SNAPSHOT_DATE": [datetime(2026, 4, 6).date()] * 3,
        "STATUS": ["Old", "Old", "Old"],
    })
    recent = pl.DataFrame({
        "EPIC_KEY": ["E2", "E4"],
        "SNAPSHOT_DATE": [datetime(2026, 4, 6).date()] * 2,
        "STATUS": ["New", "New"],
    })

    with tempfile.TemporaryDirectory() as tmp:
        cache_path = Path(tmp) / "cache.parquet"
        cached.write_parquet(cache_path)
        merged = update_history_cache_with_recent(
            pl.scan_parquet(cache_path), recent, "EPIC_KEY",
            config={"cache": {"min_retention_pct": 0.0}},
        )

    check("merge: 4 rows total (E1, E2-new, E3, E4)", merged.height == 4,
          detail=f"got {merged.height}")
    e2_status = merged.filter(pl.col("EPIC_KEY") == "E2")["STATUS"].item()
    check("merge: overlapping key E2 replaced by recent", e2_status == "New",
          detail=f"got {e2_status}")
    check("merge: new key E4 added", merged.filter(pl.col("EPIC_KEY") == "E4").height == 1)
    check("merge: untouched key E1 kept",
          merged.filter(pl.col("EPIC_KEY") == "E1")["STATUS"].item() == "Old")


def test_validate_history_and_parallel_fetch() -> None:
    df = pl.DataFrame({
        "EPIC_KEY": ["E1"],
        "SNAPSHOT_DATE": [datetime(2026, 4, 6)],  # Datetime in, Date out
    })
    out = validate_history(df, "EPIC_KEY")
    check("validate_history: SNAPSHOT_DATE cast to Date", out.schema["SNAPSHOT_DATE"] == pl.Date)

    try:
        validate_history(pl.DataFrame({"EPIC_KEY": ["E1"]}), "EPIC_KEY")
        check("validate_history: missing SNAPSHOT_DATE raises", False)
    except KeyError:
        check("validate_history: missing SNAPSHOT_DATE raises", True)

    # parallel_fetch returns results keyed correctly, parallel and sequential
    jobs = {"a": lambda: 1, "b": lambda: 2, "c": lambda: 3}
    par = parallel_fetch(jobs, config={"database": {"parallel_fetch": True}})
    seq = parallel_fetch(jobs, config={"database": {"parallel_fetch": False}})
    check("parallel_fetch: parallel results match", par == {"a": 1, "b": 2, "c": 3})
    check("parallel_fetch: sequential fallback matches", seq == par)


def main() -> int:
    test_get_last_n_snapshots_returns_requested_weekday()
    test_supertype_rules()
    test_align_schemas_handles_missing_and_mixed_dtypes()
    test_fill_missing_snapshots_fills_gap_and_marks_synthetic()
    test_cache_merge_replaces_overlapping_keys()
    test_validate_history_and_parallel_fetch()

    print()
    if _FAILURES:
        print(f"FAILED: {len(_FAILURES)} check(s)")
        for f in _FAILURES:
            print(f"  - {f}")
        return 1
    print("All checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
