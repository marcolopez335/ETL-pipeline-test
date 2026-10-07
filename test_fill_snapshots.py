"""Lightweight regression tests for the pure-Polars helpers in conversion.shared.

Designed to run in CI without the proprietary `csm_commonlib` package — each helper
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
        msg = f"{label}{f' - {detail}' if detail else ''}"
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
    check("Date + Datetime -> Datetime(us)",
          _supertype(pl.Date(), pl.Datetime("us")) == pl.Datetime("us"))
    check("Datetime us + ns -> concrete Datetime(us)",
          _supertype(pl.Datetime("us"), pl.Datetime("ns")) == pl.Datetime("us"))
    check("incompatible falls back to Utf8",
          _supertype(pl.Int64, pl.Utf8) == pl.Utf8)


def test_align_schemas_unifies_datetime_units() -> None:
    """Regression: cache (us) + fresh-from-pandas (ns) must concat cleanly.

    The bare pl.Datetime class compares equal to instances of any unit, so a
    supertype of `pl.Datetime` (not Datetime("us")) silently skipped the cast
    and pl.concat raised at the cache-merge step.
    """
    us = pl.DataFrame({"K": ["a"]}).with_columns(
        pl.lit(datetime(2026, 4, 6, 12, 0)).cast(pl.Datetime("us")).alias("CREATED"))
    ns = pl.DataFrame({"K": ["b"]}).with_columns(
        pl.lit(datetime(2026, 4, 7, 13, 0)).cast(pl.Datetime("ns")).alias("CREATED"))

    a, b = _align_schemas(us, ns)
    check("align: us/ns unified to a single unit",
          a.schema["CREATED"] == b.schema["CREATED"],
          detail=f"{a.schema['CREATED']} vs {b.schema['CREATED']}")

    merged = pl.concat([a, b])
    check("align: concat of us + ns frames succeeds", merged.height == 2)
    check("align: datetime values preserved (not stringified)",
          isinstance(merged.schema["CREATED"], pl.Datetime))


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


def test_build_burnup() -> None:
    from datetime import timezone
    from conversion.burnup_table import build_burnup

    df = pl.DataFrame({
        "LAST_UPDATED": ["2026-07-01 12:30:00"] * 3,      # DB value -> SNAPSHOT_DATE
        "TEAM_NAME": ["Alpha"] * 3,
        "ISSUE_KEY": ["F-1", "F-2", "F-3"],
        "SUMMARY": ["Program A"] * 3,
        "ISSUE_TYPE": ["Feature"] * 3,
        "STATUS": ["Done", "In Progress", "Cancelled"],
        "TARGET_END": ["2026-08-01 00:00:00", "2026-09-15 10:00:00", None],
        "RESOLVED": ["2026-06-20 09:15:00", None, None],
        "PLANNED_END": [None, "2026-09-01 00:00:00", "2026-07-15 00:00:00"],
        "PARENT_KEY": ["SC-1"] * 3,
    })

    # Fixed run time: 2026-07-01 15:00 UTC -> 10:00 CDT (Central, DST -5)
    run_ts = datetime(2026, 7, 1, 15, 0, tzinfo=timezone.utc)
    out = build_burnup(df, run_timestamp=run_ts)

    # 3 features x 3 date cols = 9 melted rows, minus 4 with null dates
    check("burnup: null-date rows filtered (9 -> 5)", out.height == 5,
          detail=f"got {out.height}")
    check("burnup: SOURCE_TYPE is 'summary'",
          out["SOURCE_TYPE"].unique().to_list() == ["summary"])
    check("burnup: TARGET_END_REF retained", "TARGET_END_REF" in out.columns)

    # DONE: only F-1 (RESOLVED row, status Done)
    done = out.filter(pl.col("DONE_FEATURES").is_not_null())
    check("burnup: DONE flags exactly F-1's resolved row",
          done.height == 1 and done["ISSUE_KEY"].item() == "F-1"
          and done["DATE_TYPE"].item() == "RESOLVED")

    # PROJECTED: only F-2 (TARGET_END row, status not terminal);
    # F-1 is Done and F-3's target is null
    proj = out.filter(pl.col("PROJECTED_FEATURES").is_not_null())
    check("burnup: PROJECTED flags exactly F-2's target row",
          proj.height == 1 and proj["ISSUE_KEY"].item() == "F-2")

    # PLANNED: F-2 and F-3 (no status filter)
    planned = out.filter(pl.col("PLANNED_FEATURES").is_not_null())
    check("burnup: PLANNED flags F-2 and F-3 regardless of status",
          sorted(planned["ISSUE_KEY"].to_list()) == ["F-2", "F-3"])

    # LAST_UPDATED = run timestamp 15:00 UTC -> 10:00 US Central (CDT -5)
    lu = out["LAST_UPDATED"][0]
    check("burnup: LAST_UPDATED is run time in US Central (CDT -5)",
          (lu.hour, lu.minute) == (10, 0), detail=f"got {lu}")

    # IMET_SUMMARY_LAST_UPDATED keeps the raw DB timestamp
    imet = out["IMET_SUMMARY_LAST_UPDATED"][0]
    check("burnup: IMET_SUMMARY_LAST_UPDATED keeps raw DB timestamp",
          (imet.hour, imet.minute) == (12, 30))

    # SNAPSHOT_DATE = DB LAST_UPDATED at midnight (the rename target)
    snap = out["SNAPSHOT_DATE"][0]
    check("burnup: SNAPSHOT_DATE is midnight of DB LAST_UPDATED",
          (snap.year, snap.month, snap.day, snap.hour) == (2026, 7, 1, 0))

    # DATE_VALUE normalized to midnight (resolved 09:15 -> 00:00)
    dv = out.filter(pl.col("DATE_TYPE") == "RESOLVED")["DATE_VALUE"].item()
    check("burnup: DATE_VALUE truncated to midnight",
          (dv.hour, dv.minute) == (0, 0), detail=f"got {dv}")


def test_prompt_guard_and_spinner_pause() -> None:
    import builtins
    import getpass
    import io

    from conversion.console import install_prompt_guard, spinner_paused

    install_prompt_guard()
    install_prompt_guard()  # idempotent — must not double-wrap

    check("guard: input() is wrapped", builtins.input.__name__ == "guarded_input")
    check("guard: getpass() is wrapped", getpass.getpass.__name__ == "guarded_getpass")

    old_stdin = sys.stdin
    sys.stdin = io.StringIO("hello\n")
    try:
        result = builtins.input()
    finally:
        sys.stdin = old_stdin
    check("guard: wrapped input() still reads stdin", result == "hello")

    # Pause context must be safe with no spinner running
    with spinner_paused():
        pass
    check("guard: spinner_paused() no-ops cleanly without a spinner", True)


def test_drop_todays_history_prevents_double_count() -> None:
    """Regression: on snapshot days the history table already has rows dated
    today; the summary rows (null SNAPSHOT_DATE, later filled with today's
    date) then doubled every story on the latest date in STORIES.hyper."""
    from conversion.stories_table import data_functions, join_stories_data
    from conversion.shared import drop_todays_history, union_data

    today = datetime.now().date()
    prior_snap = today - timedelta(days=14)

    summary = pl.DataFrame({
        "STORY_NUMBER": ["S-1", "S-2"],
        "PROJECT_NAME": ["AMMM"] * 2,
        "SPRINT_NAME": ["AMMM 26.1.2"] * 2,
        "FIX_VERSION": ["R26.2"] * 2,
        "STATUS": ["Done", "In Progress"],           # live state at run time
        "SNAPSHOT_DATE": pl.Series([None, None], dtype=pl.Datetime("us")),
        "LAST_UPDATED": [datetime.now()] * 2,
        "FEATURE_ID": ["F-1"] * 2,
    })
    history = pl.DataFrame({
        "STORY_NUMBER": ["S-1", "S-2", "S-1", "S-2"],
        "PROJECT_NAME": ["AMMM"] * 4,
        "SPRINT_NAME": ["AMMM 26.1.2"] * 4,
        "FIX_VERSION": ["R26.2"] * 4,
        "STATUS": ["In Progress"] * 4,               # morning-snapshot state
        "SNAPSHOT_DATE": [prior_snap, prior_snap, today, today],
        "LAST_UPDATED": pl.Series([None] * 4, dtype=pl.Datetime("us")),
        "FEATURE_ID": ["F-1"] * 4,
    }).with_columns(pl.col("SNAPSHOT_DATE").cast(pl.Date))

    kept = drop_todays_history(history)
    check("drop: today's history rows removed, prior snapshot kept",
          kept.height == 2
          and kept["SNAPSHOT_DATE"].unique().to_list() == [prior_snap],
          detail=f"kept={kept.height}")

    # Null snapshot dates should not exist in history, but must never be
    # swallowed by the filter (ne_missing keeps them)
    with_null = history.vstack(history.head(1).with_columns(
        pl.lit(None).cast(pl.Date).alias("SNAPSHOT_DATE")))
    check("drop: null snapshot dates are kept, not dropped",
          drop_todays_history(with_null).height == 3)

    epics_lookup = pl.DataFrame({
        "FEATURE_ID": ["F-1", "F-1"],
        "SNAPSHOT_DATE": pl.Series(
            [None, datetime(prior_snap.year, prior_snap.month, prior_snap.day)],
            dtype=pl.Datetime("us")),
        "FEATURE_STATUS": ["Committed"] * 2,
    })

    df = data_functions(join_stories_data(union_data(summary, kept), epics_lookup))

    latest = df.filter(pl.col("Snapshot Date").cast(pl.Date) == today)
    check("no double count: one row per story on the latest date",
          latest.height == 2 and latest["Story Number"].n_unique() == 2,
          detail=f"rows={latest.height}")
    check("today's rows come from the live summary (S-1 is Done)",
          latest.filter(pl.col("Story Number") == "S-1")["Status"].item() == "Done")

    prior = df.filter(pl.col("Snapshot Date").cast(pl.Date) == prior_snap)
    check("prior snapshot untouched", prior.height == 2,
          detail=f"rows={prior.height}")
    check("feature attributes joined on the prior snapshot",
          prior["Feature Status"].null_count() == 0)

    # Same bug in EPICS: summary rows get today's date after ACRP is built
    from conversion.epics_table import build_sprint_lookups
    from conversion.epics_table import data_functions as epics_transforms
    feat_summary = pl.DataFrame({
        "FEATURE_KEY": ["F1", "F2"], "PROGRAM_INCREMENT": ["PI 26.1"] * 2,
        "STATUS": ["Done", "Open"], "SNAPSHOT_DATE": pl.Series([None, None], dtype=pl.Date),
    })
    feat_history = pl.DataFrame({
        "FEATURE_KEY": ["F1", "F2", "F1", "F2"], "PROGRAM_INCREMENT": ["PI 26.1"] * 4,
        "STATUS": ["Open"] * 4, "SNAPSHOT_DATE": [prior_snap, prior_snap, today, today],
    })
    sprints = pl.DataFrame({
        "PROGRAM_INCREMENT": ["PI 26.1"], "SPRINT_NAME": ["AMMM 26.1.2"],
        "BEGIN_DATE": [datetime(2026, 4, 1)], "END_DATE": [datetime(2026, 4, 14)],
    })
    hist_lookup, sum_lookup = build_sprint_lookups(
        sprints.with_columns(pl.lit(today).alias("SNAPSHOT_DATE")), sprints)
    ep = epics_transforms(union_data(feat_summary, drop_todays_history(feat_history)),
                          hist_lookup, sum_lookup)
    ep = ep.with_columns(  # same fill as epics_table.run()
        pl.col("Snapshot Date").fill_null(pl.col("Last Updated").cast(pl.Date, strict=False)))
    ep_latest = ep.filter(pl.col("Snapshot Date") == today)
    check("no double count (epics): one row per feature on the latest date",
          ep_latest.height == 2 and ep_latest["Feature Key"].n_unique() == 2,
          detail=f"rows={ep_latest.height}")
    check("epics: today's rows come from the live summary (F1 is Done)",
          ep_latest.filter(pl.col("Feature Key") == "F1")["Status"].item() == "Done")


def test_sprint_lookup_guards() -> None:
    """Epics sprint lookups: only a PI's own sprints, unparseable names
    ignored (not "0.0.0"), no row fan-out downstream, no current sprint."""
    from conversion.epics_table import data_functions, build_sprint_lookups

    now = datetime.now()
    day = timedelta(days=1)
    # One story tagged "PI 26.1" sits in sprint 26.2.1 (the stray row); that
    # sprint and 26.1.IP both contain today. "(extended)" cannot be parsed.
    sprints = pl.DataFrame({
        "PROGRAM_INCREMENT": ["PI 26.1", "PI 26.1", "PI 26.1", "PI 26.1", "PI 26.2", "PI 26.3"],
        "SPRINT_NAME": ["AMMM 26.1.1", "AMMM 26.1.2", "AMMM 26.1.IP", "AMMM 26.2.1",
                        "AMMM 26.2.1", "AMMM 26.3.1 (extended)"],
        "BEGIN_DATE": [now - 40 * day, now - 25 * day, now - 10 * day, now - 5 * day,
                       now - 5 * day, now + 60 * day],
        "END_DATE": [now - 26 * day, now - 11 * day, now + 4 * day, now + 9 * day,
                     now + 9 * day, now + 74 * day],
    })
    history = sprints.head(0).with_columns(pl.lit(now.date()).alias("SNAPSHOT_DATE"))
    hist_lookup, sum_lookup = build_sprint_lookups(history, sprints)

    pi1 = sum_lookup.filter(pl.col("PROGRAM_INCREMENT") == "PI 26.1")
    pi1_range = (pi1["MIN_SPRINT"].item(), pi1["MAX_SPRINT"].item())
    check("sprint guard: PI 26.1 range stays inside 26.1",
          pi1_range == ("26.1.1", "26.1.IP"), detail=str(pi1_range))
    names = pi1["SPRINT_NAMES"].item()
    check("sprint names: the PI's own sprints in sprint order, stray sprint excluded",
          names == "AMMM 26.1.1, AMMM 26.1.2, AMMM 26.1.IP", detail=str(names))
    pi3 = sum_lookup.filter(pl.col("PROGRAM_INCREMENT") == "PI 26.3")
    pi3_range = (pi3["MIN_SPRINT"].item(), pi3["MAX_SPRINT"].item())
    check("sprint guard: unparseable sprint name gives a null range, not 0.0.0",
          pi3_range == (None, None), detail=str(pi3_range))
    check("sprint names: an unparseable sprint name is still visible",
          pi3["SPRINT_NAMES"].item() == "AMMM 26.3.1 (extended)", detail=str(pi3["SPRINT_NAMES"].item()))

    epics = pl.DataFrame({
        "EPIC_KEY": ["E1"], "FEATURE_KEY": ["F1"], "PROGRAM_INCREMENT": ["PI 26.1"],
        "SNAPSHOT_DATE": pl.Series([None], dtype=pl.Date),
    })
    out = data_functions(epics, hist_lookup, sum_lookup)
    check("sprint guard: data_functions keeps one row per epic (no fan-out)",
          out.height == 1, detail=f"rows={out.height}")
    check("sprint guard: no Current Sprint column; Max Sprint from the PI's own sprints",
          "Current Sprint" not in out.columns and out["Max Sprint"].item() == "26.1.IP",
          detail=f"sprint cols={[c for c in out.columns if 'Sprint' in c]}")
    check("sprint names: Sprint Names reaches the epic row",
          out["Sprint Names"].item() == "AMMM 26.1.1, AMMM 26.1.2, AMMM 26.1.IP",
          detail=str(out["Sprint Names"].item()))


def test_feature_pi_counts_features_by_their_own_pi() -> None:
    """PROGRAM_INCREMENT on an epic row comes from the feature's stories, so a
    feature appears under every PI any of its stories is tagged with. Counting
    features by that column overcounts; FEATURE_PI (the feature's own field)
    must pass through untouched so Tableau can count by it instead."""
    from conversion.epics_table import data_functions, build_sprint_lookups, join_agile
    from conversion.shared import union_data

    # 10 Done features whose own PI is 26.1, plus a 26.2 feature with one
    # stale-tagged story and a 25.4 feature with one story carried into 26.1
    keys = [f"F{i:02d}" for i in range(1, 11)] + ["F11", "F12"]
    epics = pl.DataFrame({
        "EPIC_KEY": ["E-" + k for k in keys], "FEATURE_KEY": keys,
        "FEATURE_PI": ["PI 26.1"] * 10 + ["PI 26.2", "PI 25.4"],
        "STATUS": ["Done"] * 12, "FEATURE_TEAM": ["Team A"] * 12,
    })
    rollup = pl.DataFrame({
        "FEATURE_ID": keys + ["F11", "F12"],
        "PROGRAM_INCREMENT": ["PI 26.1"] * 10 + ["PI 26.2", "PI 25.4", "PI 26.1", "PI 26.1"],
        "TOTAL_ESTIMATE": [8.0] * 10 + [5.0, 8.0, 2.0, 1.0],
        "SPRINT_COUNT": [2] * 10 + [1, 1, 1, 1],
    })
    sprints = pl.DataFrame({
        "PROGRAM_INCREMENT": ["PI 26.1", "PI 26.2", "PI 25.4"],
        "SPRINT_NAME": ["AMMM 26.1.2", "AMMM 26.2.1", "AMMM 25.4.5"],
        "BEGIN_DATE": [datetime(2026, 4, 1), datetime(2026, 5, 1), datetime(2026, 3, 4)],
        "END_DATE": [datetime(2026, 4, 14), datetime(2026, 5, 14), datetime(2026, 3, 17)],
    })
    no_snapshot = pl.lit(None).cast(pl.Date).alias("SNAPSHOT_DATE")
    df = join_agile(epics, rollup, has_snapshot=False)
    df = union_data(df, df.head(0).with_columns(no_snapshot))
    hist_lookup, sum_lookup = build_sprint_lookups(sprints.head(0).with_columns(no_snapshot), sprints)
    out = data_functions(df, hist_lookup, sum_lookup)

    by_story_pi = out.filter(pl.col("Program Increment") == "PI 26.1")["Feature Key"].n_unique()
    check("feature count: by story-derived Program Increment a feature shows in every PI its stories touch (12)",
          by_story_pi == 12, detail=f"got {by_story_pi}")
    by_own_pi = out.filter((pl.col("Feature Pi") == "PI 26.1")
                           & (pl.col("Status") == "Done"))["Feature Key"].n_unique()
    check("feature count: by the feature's own Feature Pi it is exactly the 10 Done features",
          by_own_pi == 10, detail=f"got {by_own_pi}")
    strays = out.filter((pl.col("Program Increment") == "PI 26.1") & (pl.col("Feature Pi") != "PI 26.1"))
    check("feature count: the two extras are the stray-story rows (Sprint Count 1)",
          sorted(strays["Feature Key"].to_list()) == ["F11", "F12"]
          and strays["Sprint Count"].unique().to_list() == [1],
          detail=str(strays.select(["Feature Key", "Sprint Count"]).rows()))


def test_export_csv_round_trips() -> None:
    """--csv: the CSV carries the same columns and rows as the frame, in a
    folder that is created on demand, readable back with the BOM stripped."""
    import tempfile
    from pathlib import Path
    from conversion.shared import export_csv

    df = pl.DataFrame({
        "Feature Key": ["F-1", "F-2"],
        "Snapshot Date": [datetime(2026, 4, 6).date(), None],
        "Last Updated": [datetime(2026, 4, 6, 10, 30), datetime(2026, 4, 6, 10, 30)],
        "Bv": [10.0, None],
        "Sprint Names": ["AMMM 26.1.1, AMMM 26.1.2", None],
        "Is Synthetic": [False, True],
    })
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "csv" / "EPICS.csv"
        export_csv(df, path)
        check("csv: folder created and file written", path.is_file())
        check("csv: starts with a UTF-8 BOM for Excel", path.read_bytes()[:3] == b"\xef\xbb\xbf")
        back = pl.read_csv(path)
        check("csv: same columns in the same order", back.columns == df.columns, detail=str(back.columns))
        check("csv: same row count", back.height == 2)
        check("csv: comma inside a value survives quoting",
              back["Sprint Names"][0] == "AMMM 26.1.1, AMMM 26.1.2", detail=str(back["Sprint Names"][0]))
        check("csv: nulls come back empty", back["Bv"][1] is None and back["Snapshot Date"][1] is None)


def test_schemas_match_sql_and_parse_string_dates() -> None:
    """Every schema key is a column its queries return (a renamed SQL column
    must not silently skip its cast), and text dates are parsed, not nulled."""
    import re
    from pathlib import Path
    from conversion.shared import clean_dtypes
    from schemas.datatypes import EXPECTED_DTYPES_EPICS, EXPECTED_DTYPES_STORIES

    sql_dir = Path(__file__).resolve().parent / "sql"

    def output_names(*files: str) -> set[str]:
        names = set()
        for f in files:
            text = re.sub(r"--[^\n]*", "", (sql_dir / f).read_text())  # drop commented-out lines
            names |= set(re.findall(r"\b[A-Z][A-Z0-9_]*\b", text.replace('"', "")))
        return names

    for label, schema, files in [
        ("stories", EXPECTED_DTYPES_STORIES, ["Asum.sql", "Ahist.sql", "Ahist_recent.sql", "EsumEhist.sql"]),
        ("epics", EXPECTED_DTYPES_EPICS, ["EpicSummary.sql", "EpicHistory.sql", "EpicHistory_recent.sql"]),
    ]:
        missing = sorted(k for k in schema if k not in output_names(*files))
        check(f"schema: every {label} key appears in its SQL", not missing, detail=str(missing))

    df = pl.DataFrame({
        "PLANNED_START": ["2026-01-05", None],
        "RESOLVED": ["2026-01-05 10:30:00", None],
        "FEATURE_ESTIMATE": ["8", "x"],
    })
    out = clean_dtypes(df, EXPECTED_DTYPES_EPICS)
    check("clean_dtypes: text date parsed, not nulled",
          out["PLANNED_START"][0] == datetime(2026, 1, 5), detail=str(out["PLANNED_START"].to_list()))
    check("clean_dtypes: text timestamp parsed, not nulled",
          out["RESOLVED"][0] == datetime(2026, 1, 5, 10, 30), detail=str(out["RESOLVED"].to_list()))
    check("clean_dtypes: bad number becomes null, good one casts",
          out["FEATURE_ESTIMATE"].to_list() == [8.0, None], detail=str(out["FEATURE_ESTIMATE"].to_list()))
    try:
        clean_dtypes(df, {"PLANNED_START": "datetme"})
        check("clean_dtypes: unknown dtype raises", False)
    except ValueError:
        check("clean_dtypes: unknown dtype raises", True)


def test_rebuild_cache_and_column_drift_warning() -> None:
    """--rebuild-cache reseeds the history cache from the full query (backing
    up the old file), and an incremental update warns when the history SQL
    gained or lost columns since the cache was seeded."""
    import logging
    import tempfile
    from pathlib import Path
    import conversion.shared as shared

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        cache = tmp / "epics_history_cache.parquet"
        old_backup_dir = shared.BACKUP_DIR
        shared.BACKUP_DIR = tmp / "backups"
        shared.BACKUP_DIR.mkdir()
        cfg = {"cache": {"backup_enabled": True, "max_cache_backups": 3, "min_retention_pct": 0.98}}
        try:
            check("fetch plan: no cache -> full query",
                  shared.history_fetch_plan(cache, "full.sql", "recent.sql") == ("full.sql", "full"))
            # Old epic-grain cache: 3 epic rows for one feature, no FEATURE_PI column
            old = pl.DataFrame({
                "EPIC_KEY": ["E1", "E2", "E3"], "FEATURE_KEY": ["F1"] * 3,
                "SNAPSHOT_DATE": [datetime(2026, 9, 28).date()] * 3,
            })
            old.write_parquet(cache)
            check("fetch plan: cache present -> recent query",
                  shared.history_fetch_plan(cache, "full.sql", "recent.sql") == ("recent.sql", "recent"))
            check("fetch plan: rebuild -> full query even with a cache",
                  shared.history_fetch_plan(cache, "full.sql", "recent.sql", rebuild=True) == ("full.sql", "full"))

            # Drift: the recent query has FEATURE_PI, the cache does not (and still has EPIC_KEY)
            records = []
            handler = logging.Handler()
            handler.emit = records.append
            shared.logger.addHandler(handler)
            try:
                recent = pl.DataFrame({
                    "FEATURE_KEY": ["F1"], "FEATURE_PI": ["PI 26.1"],
                    "SNAPSHOT_DATE": [datetime(2026, 10, 5).date()],
                })
                shared.update_history_cache_with_recent(pl.scan_parquet(cache), recent, "FEATURE_KEY",
                                                        config={"cache": {"min_retention_pct": 0.0}})
            finally:
                shared.logger.removeHandler(handler)
            msgs = " ".join(r.getMessage() for r in records)
            check("drift: warns about a column the cache lacks",
                  "has no column(s) ['FEATURE_PI']" in msgs, detail=msgs[:200])
            check("drift: warns about a column the query no longer returns",
                  "no longer returns column(s) ['EPIC_KEY']" in msgs, detail=msgs[:200])

            # Rebuild: full feature-grain history replaces the epic-grain cache
            full = pl.DataFrame({
                "FEATURE_KEY": ["F1", "F1"], "FEATURE_PI": ["PI 26.1"] * 2,
                "SNAPSHOT_DATE": [datetime(2026, 9, 28).date(), datetime(2026, 10, 5).date()],
            })
            out = shared.update_history("full.sql", "recent.sql", "FEATURE_KEY", cache, config=cfg,
                                        prefetched=full, prefetched_kind="full", rebuild=True)
            reread = pl.read_parquet(cache)
            check("rebuild: cache reseeded from the full query (feature grain, new column)",
                  out.height == 2 and reread.height == 2 and "EPIC_KEY" not in reread.columns
                  and "FEATURE_PI" in reread.columns, detail=str(reread.columns))
            check("rebuild: old cache backed up first",
                  len(list(shared.BACKUP_DIR.glob("epics_history_cache_*.parquet"))) == 1)
        finally:
            shared.BACKUP_DIR = old_backup_dir


def main() -> int:
    test_get_last_n_snapshots_returns_requested_weekday()
    test_drop_todays_history_prevents_double_count()
    test_sprint_lookup_guards()
    test_feature_pi_counts_features_by_their_own_pi()
    test_export_csv_round_trips()
    test_rebuild_cache_and_column_drift_warning()
    test_schemas_match_sql_and_parse_string_dates()
    test_supertype_rules()
    test_align_schemas_unifies_datetime_units()
    test_align_schemas_handles_missing_and_mixed_dtypes()
    test_fill_missing_snapshots_fills_gap_and_marks_synthetic()
    test_cache_merge_replaces_overlapping_keys()
    test_validate_history_and_parallel_fetch()
    test_build_burnup()
    test_prompt_guard_and_spinner_pause()

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
