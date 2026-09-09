"""Pure-Polars helpers in conversion.shared -- runs without csm_commonlib."""

import logging
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from conversion.shared import (
    _align_schemas,
    _supertype,
    clean_dtypes,
    drop_todays_history,
    fill_missing_snapshots,
    get_last_n_snapshots,
    history_fetch_plan,
    parallel_fetch,
    rename_to_snake_case,
    rename_to_title_case,
    union_data,
    update_history_cache_with_recent,
    validate_history,
)


# --- snapshot days -----------------------------------------------------------

def test_get_last_n_snapshots_returns_requested_weekday():
    ref = datetime(2026, 4, 29)  # a Wednesday; last Monday is 2026-04-27
    mondays = get_last_n_snapshots(4, day_of_week=0, from_date=ref)
    assert len(mondays) == 4
    assert all(d.weekday() == 0 for d in mondays)
    assert mondays[0].date() == (ref - timedelta(days=2)).date()
    assert all((mondays[i] - mondays[i + 1]).days == 7 for i in range(3))


def test_fill_missing_snapshots_fills_gap_and_marks_synthetic():
    monday = datetime(2026, 4, 6)  # well in the past, so today's exclusion is moot
    cfg = {"snapshots": {"day_of_week": 0, "lookback_weeks": 4}}
    summary = pl.DataFrame({"EPIC_KEY": ["E1", "E2"], "STATUS": ["Open", "Done"]})
    history = pl.DataFrame({
        "EPIC_KEY": ["E1"], "STATUS": ["Open"], "SNAPSHOT_DATE": [monday.date()],
    })

    out = fill_missing_snapshots(summary, history, key_col="EPIC_KEY", config=cfg)
    assert "IS_SYNTHETIC" in out.columns
    assert out.filter(pl.col("EPIC_KEY") == "E1").height >= 1
    synth = out.filter(pl.col("IS_SYNTHETIC").fill_null(False))
    assert synth.height >= 1
    assert set(synth["EPIC_KEY"].to_list()) <= {"E1", "E2"}


# --- today belongs to the summary --------------------------------------------

def test_drop_todays_history_keeps_prior_and_null_dates():
    today = date(2026, 4, 8)
    df = pl.DataFrame({
        "K": ["today", "prior", "no-date"],
        "SNAPSHOT_DATE": [today, today - timedelta(days=7), None],
    })
    assert drop_todays_history(df, today=today)["K"].to_list() == ["prior", "no-date"]


def test_drop_todays_history_handles_timestamp_column_and_defaults_to_today():
    today = datetime.now().date()
    df = pl.DataFrame({
        "K": ["today-3am", "yesterday"],
        "SNAPSHOT_DATE": pl.Series([
            datetime(today.year, today.month, today.day, 3, 0),
            datetime(today.year, today.month, today.day, 3, 0) - timedelta(days=1),
        ], dtype=pl.Datetime("us")),
    })
    assert drop_todays_history(df)["K"].to_list() == ["yesterday"]


# --- dtypes ------------------------------------------------------------------

def test_supertype_rules():
    assert _supertype(pl.Int64, pl.Int64) == pl.Int64
    assert _supertype(pl.Null, pl.Utf8) == pl.Utf8
    assert _supertype(pl.Int32, pl.Float32) == pl.Float64
    assert _supertype(pl.Date(), pl.Datetime("us")) == pl.Datetime("us")
    assert _supertype(pl.Datetime("us"), pl.Datetime("ns")) == pl.Datetime("us")
    assert _supertype(pl.Int64, pl.Utf8) == pl.Utf8


def test_align_schemas_unifies_datetime_units():
    """Regression: cache (us) + fresh-from-pandas (ns) must concat cleanly."""
    us = pl.DataFrame({"K": ["a"]}).with_columns(
        pl.lit(datetime(2026, 4, 6, 12, 0)).cast(pl.Datetime("us")).alias("CREATED"))
    ns = pl.DataFrame({"K": ["b"]}).with_columns(
        pl.lit(datetime(2026, 4, 7, 13, 0)).cast(pl.Datetime("ns")).alias("CREATED"))

    a, b = _align_schemas(us, ns)
    assert a.schema["CREATED"] == b.schema["CREATED"]
    merged = pl.concat([a, b])
    assert merged.height == 2
    assert isinstance(merged.schema["CREATED"], pl.Datetime)


def test_align_schemas_handles_missing_and_mixed_dtypes():
    df1 = pl.DataFrame({"id": [1, 2], "value": [1.0, 2.0]})
    df2 = pl.DataFrame({"id": [3], "label": ["x"]})
    a, b = _align_schemas(df1, df2)
    assert a.columns == b.columns
    assert {"label", "value"} <= set(a.columns)
    assert (a.height, b.height) == (2, 1)

    df3 = pl.DataFrame({"x": pl.Series([1, 2], dtype=pl.Int64)})
    df4 = pl.DataFrame({"x": pl.Series([1.5, 2.5], dtype=pl.Float64)})
    a, b = _align_schemas(df3, df4)
    assert a.schema["x"] == pl.Float64 and b.schema["x"] == pl.Float64


def test_clean_dtypes_casts_each_declared_type_and_skips_unknown_columns():
    df = pl.DataFrame({
        "S": [" a ", None],
        "F": ["1.5", "x"],
        "DT": ["2026-04-06 12:30:00", None],       # string-typed date column
        "D": pl.Series([datetime(2026, 4, 6, 12, 30), None], dtype=pl.Datetime("ns")),
        "KEEP": [1, 2],
    })
    out = clean_dtypes(df, {
        "S": "string", "F": "float", "DT": "datetime", "D": "date", "ABSENT": "string",
    })
    assert out.schema["S"] == pl.Utf8 and out["S"].to_list() == ["a", None]
    assert out.schema["F"] == pl.Float64 and out["F"].to_list() == [1.5, None]
    assert out.schema["DT"] == pl.Datetime("us")
    assert out["DT"].to_list() == [datetime(2026, 4, 6, 12, 30), None]
    assert out.schema["D"] == pl.Date and out["D"].to_list() == [date(2026, 4, 6), None]
    assert out.schema["KEEP"] == pl.Int64


def test_clean_dtypes_all_null_text_column_becomes_typed():
    # `NULL AS SNAPSHOT_DATE` reaches Polars as an all-null string column
    df = pl.DataFrame({"SNAPSHOT_DATE": pl.Series([None, None], dtype=pl.Utf8)})
    out = clean_dtypes(df, {"SNAPSHOT_DATE": "datetime"})
    assert out.schema["SNAPSHOT_DATE"] == pl.Datetime("us")
    assert out["SNAPSHOT_DATE"].null_count() == 2


def test_clean_dtypes_rejects_unknown_type_name():
    with pytest.raises(ValueError):
        clean_dtypes(pl.DataFrame({"A": [1]}), {"A": "integer"})


# --- column naming -----------------------------------------------------------

def test_rename_title_case_round_trips_to_snake_case():
    df = pl.DataFrame({"FEATURE_KEY": [1], "BV": [2], "SNAPSHOT_DATE_ALT": [3]})
    titled = rename_to_title_case(df)
    assert titled.columns == ["Feature Key", "Bv", "Snapshot Date Alt"]
    assert rename_to_snake_case(titled).columns == df.columns


def test_union_data_keeps_summary_column_order_and_dedups():
    summary = pl.DataFrame({"K": ["a"], "V": [1], "SNAPSHOT_DATE": pl.Series([None], dtype=pl.Date)})
    history = pl.DataFrame({"SNAPSHOT_DATE": [date(2026, 4, 6)] * 2, "K": ["a", "a"], "V": [1, 1]})
    out = union_data(summary, history)
    assert out.columns == ["K", "V", "SNAPSHOT_DATE"]
    assert out.height == 2  # exact duplicate history row collapsed


# --- cache -------------------------------------------------------------------

def test_cache_merge_replaces_overlapping_keys():
    cached = pl.DataFrame({
        "EPIC_KEY": ["E1", "E2", "E3"],
        "SNAPSHOT_DATE": [date(2026, 4, 6)] * 3,
        "STATUS": ["Old", "Old", "Old"],
    })
    recent = pl.DataFrame({
        "EPIC_KEY": ["E2", "E4"],
        "SNAPSHOT_DATE": [date(2026, 4, 6)] * 2,
        "STATUS": ["New", "New"],
    })
    with tempfile.TemporaryDirectory() as tmp:
        cache_path = Path(tmp) / "cache.parquet"
        cached.write_parquet(cache_path)
        merged = update_history_cache_with_recent(
            pl.scan_parquet(cache_path), recent, "EPIC_KEY",
            config={"cache": {"min_retention_pct": 0.0}},
        )
    assert merged.height == 4
    assert merged.filter(pl.col("EPIC_KEY") == "E2")["STATUS"].item() == "New"
    assert merged.filter(pl.col("EPIC_KEY") == "E4").height == 1
    assert merged.filter(pl.col("EPIC_KEY") == "E1")["STATUS"].item() == "Old"


def test_validate_history_and_parallel_fetch():
    df = pl.DataFrame({"EPIC_KEY": ["E1"], "SNAPSHOT_DATE": [datetime(2026, 4, 6)]})
    assert validate_history(df, "EPIC_KEY").schema["SNAPSHOT_DATE"] == pl.Date
    with pytest.raises(KeyError):
        validate_history(pl.DataFrame({"EPIC_KEY": ["E1"]}), "EPIC_KEY")

    jobs = {"a": lambda: 1, "b": lambda: 2, "c": lambda: 3}
    par = parallel_fetch(jobs, config={"database": {"parallel_fetch": True}})
    seq = parallel_fetch(jobs, config={"database": {"parallel_fetch": False}})
    assert par == {"a": 1, "b": 2, "c": 3} == seq


# --- cache vs. SQL column drift ----------------------------------------------

def test_history_fetch_plan_rebuild_forces_the_full_query(tmp_path):
    cache = tmp_path / "cache.parquet"
    assert history_fetch_plan(cache, "full.sql", "recent.sql") == ("full.sql", "full")
    cache.write_bytes(b"")
    assert history_fetch_plan(cache, "full.sql", "recent.sql") == ("recent.sql", "recent")
    assert history_fetch_plan(cache, "full.sql", "recent.sql", rebuild=True) == ("full.sql", "full")


def test_cache_merge_warns_when_the_query_gained_a_column(tmp_path, caplog):
    """A column added to the history SQL after the cache was seeded is null for
    every cached snapshot (only the recent window carries it). The merge must
    say so, because downstream it looks exactly like a broken join."""
    old_day, new_day = date(2026, 3, 2), date(2026, 8, 31)
    cached = pl.DataFrame({"EPIC_KEY": ["E1"], "SNAPSHOT_DATE": [old_day]})
    recent = pl.DataFrame({
        "EPIC_KEY": ["E1"], "SNAPSHOT_DATE": [new_day],
        "BASELINE_PLANNED_END": [date(2026, 10, 1)],
    })
    cache_path = tmp_path / "cache.parquet"
    cached.write_parquet(cache_path)

    with caplog.at_level(logging.WARNING, logger="conversion.shared"):
        merged = update_history_cache_with_recent(
            pl.scan_parquet(cache_path), recent, "EPIC_KEY",
            config={"cache": {"min_retention_pct": 0.0}},
        )

    by_day = {r["SNAPSHOT_DATE"]: r["BASELINE_PLANNED_END"] for r in merged.to_dicts()}
    assert by_day[old_day] is None                 # the cached snapshot cannot have it
    assert by_day[new_day] == date(2026, 10, 1)    # the recent window does
    assert any("BASELINE_PLANNED_END" in m and "--rebuild-cache" in m for m in caplog.messages)


def test_update_history_rebuild_reseeds_from_the_full_prefetch(tmp_path, monkeypatch):
    from conversion import shared

    monkeypatch.setattr(shared, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(shared, "BACKUP_DIR", tmp_path / "backups")
    cache_path = tmp_path / "epics_history_cache.parquet"
    pl.DataFrame({"EPIC_KEY": ["E1"], "SNAPSHOT_DATE": [date(2026, 3, 2)]}).write_parquet(cache_path)

    full = pl.DataFrame({
        "EPIC_KEY": ["E1", "E1"],
        "SNAPSHOT_DATE": [date(2026, 3, 2), date(2026, 8, 31)],
        "BASELINE_PLANNED_END": [date(2026, 10, 1)] * 2,
    })
    out = shared.update_history(
        "full.sql", "recent.sql", "EPIC_KEY", cache_path,
        config={"cache": {"backup_enabled": True, "max_cache_backups": 3}},
        prefetched=full, prefetched_kind="full", rebuild=True,
    )

    assert out["BASELINE_PLANNED_END"].null_count() == 0
    reseeded = pl.read_parquet(cache_path)
    assert "BASELINE_PLANNED_END" in reseeded.columns and reseeded.height == 2
    assert list((tmp_path / "backups").glob("epics_history_cache_*.parquet"))   # old cache kept
