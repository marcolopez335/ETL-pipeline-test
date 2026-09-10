"""Epics build chain on in-memory frames: agile joins, union, sprint lookups, ACRP."""

from datetime import datetime, timedelta

import polars as pl

from conversion.epics_table import (
    _sort_key_to_version,
    _sprint_sort_key,
    build_acrp,
    build_epics,
    build_sprint_lookups,
    carry_baseline_from_summary,
    join_agile,
)

NOW = datetime(2026, 4, 8, 10, 30)          # run time; inside sprint 26.1.2 (Apr 1-14)
TODAY = NOW.date()
PRIOR = TODAY - timedelta(days=14)          # an earlier snapshot day


def summary_frame() -> pl.DataFrame:
    """Live hierarchy: E1 under F1 (two fix versions), E2 under F2, E3 has no parent."""
    return pl.DataFrame({
        "EPIC_KEY": ["E1", "E2", "E3"],
        "EPIC_SUMMARY": ["Epic one", "Epic two", "Orphan epic"],
        "FEATURE_KEY": ["F1", "F2", None],
        "SUBCAPABILITY_KEY": ["SC1", "SC2", None],
        "FEATURE_FIX_VERSION": ["R26.1, R26.2", "R26.2", None],
        "PLANNED_START": [datetime(2026, 3, 1), datetime(2026, 4, 1), None],
        "PLANNED_END": [datetime(2026, 10, 15), datetime(2026, 12, 1), None],       # DB TARGET_END, live
        "BASELINE_PLANNED_END": [datetime(2026, 10, 1), datetime(2026, 11, 15), None],
        "PROGRAM": ["Prog A", "Prog A", None],
    })


def history_frame() -> pl.DataFrame:
    """E1/E2 on the prior snapshot, plus a snapshot taken this morning (dropped)."""
    return pl.DataFrame({
        "SNAPSHOT_DATE": [PRIOR, PRIOR, TODAY, TODAY],
        "EPIC_KEY": ["E1", "E2", "E1", "E2"],
        "EPIC_SUMMARY": ["Epic one (old)", "Epic two (old)", "Epic one (morning)", "Epic two (morning)"],
        "FEATURE_KEY": ["F1", "F2", "F1", "F2"],
        "SUBCAPABILITY_KEY": ["SC1", "SC2", "SC1", "SC2"],
        "FEATURE_FIX_VERSION": ["R26.1", "R26.2", "R26.1", "R26.2"],
        "PLANNED_START": [datetime(2026, 3, 1), datetime(2026, 4, 1)] * 2,
        "PLANNED_END": [datetime(2026, 10, 1), datetime(2026, 11, 15)] * 2,          # DB TARGET_END on that snapshot
        "BASELINE_PLANNED_END": pl.Series([None] * 4, dtype=pl.Datetime("us")),   # NULL in the history SQL
        "PROGRAM": ["Prog A"] * 4,
        "IS_SYNTHETIC": [False] * 4,
    })


def agile_summary_frame() -> pl.DataFrame:
    """F1 has stories in two PIs -> its epic fans out to two rows."""
    return pl.DataFrame({
        "FEATURE_ID": ["F1", "F1", "F2"],
        "PROGRAM_INCREMENT": ["PI 26.1", "PI 26.2", "PI 26.1"],
        "TOTAL_ESTIMATE": [10.0, 5.0, 8.0],
        "DONE_ESTIMATE": [4.0, 0.0, 8.0],
        "OPEN_ESTIMATE": [6.0, 5.0, 0.0],
    })


def agile_history_frame() -> pl.DataFrame:
    return pl.DataFrame({
        "FEATURE_ID": ["F1", "F2"],
        "SNAPSHOT_DATE": [PRIOR, PRIOR],
        "PROGRAM_INCREMENT": ["PI 26.1", "PI 26.1"],
        "TOTAL_ESTIMATE": [10.0, 8.0],
        "DONE_ESTIMATE": [2.0, 3.0],
        "OPEN_ESTIMATE": [8.0, 5.0],
    })


def sprint_range_summary_frame() -> pl.DataFrame:
    """Three sprints in PI 26.1 (today is inside 26.1.2) and one in PI 26.2."""
    return pl.DataFrame({
        "PROGRAM_INCREMENT": ["PI 26.1"] * 3 + ["PI 26.2"],
        "SPRINT_NAME": ["AMMM 26.1.1", "AMMM 26.1.2", "AMMM 26.1.IP", "AMMM 26.2.1"],
        "BEGIN_DATE": [datetime(2026, 3, 18), datetime(2026, 4, 1), datetime(2026, 4, 15), datetime(2026, 5, 1)],
        "END_DATE": [datetime(2026, 3, 31), datetime(2026, 4, 14), datetime(2026, 4, 28), datetime(2026, 5, 14)],
    })


def sprint_range_history_frame() -> pl.DataFrame:
    return (
        sprint_range_summary_frame()
        .filter(pl.col("PROGRAM_INCREMENT") == "PI 26.1")
        .with_columns(pl.lit(PRIOR).alias("SNAPSHOT_DATE"))
    )


def build() -> tuple[pl.DataFrame, pl.DataFrame]:
    return build_epics(
        summary_frame(), history_frame(),
        agile_history_frame(), agile_summary_frame(),
        sprint_range_history_frame(), sprint_range_summary_frame(),
        now=NOW,
    )


def test_build_epics_grain_is_epic_by_program_increment_by_snapshot():
    df, _ = build()

    assert "Epic Key" in df.columns                          # Title Case for Tableau
    assert df["Snapshot Date"].null_count() == 0
    assert df.schema["Snapshot Date"] == pl.Date             # EPICS.hyper keeps a Date

    today_rows = df.filter(pl.col("Snapshot Date") == TODAY)
    # summary owns today: E1 x 2 PIs + E2 + E3 (no feature) -- the morning snapshot is gone
    assert today_rows.height == 4
    assert not today_rows["Epic Summary"].str.contains("morning").any()
    e1_today = today_rows.filter(pl.col("Epic Key") == "E1")
    assert sorted(e1_today["Program Increment"].to_list()) == ["PI 26.1", "PI 26.2"]
    assert sorted(e1_today["Total Estimate"].to_list()) == [5.0, 10.0]
    e3 = today_rows.filter(pl.col("Epic Key") == "E3")
    assert e3.height == 1 and e3["Program Increment"].item() is None

    prior_rows = df.filter(pl.col("Snapshot Date") == PRIOR)
    assert prior_rows.height == 2
    assert prior_rows.filter(pl.col("Epic Key") == "E1")["Done Estimate"].item() == 2.0

    assert df["Last Updated"].unique().to_list() == [NOW]


def test_build_epics_sprint_columns():
    df, _ = build()

    prior_e1 = df.filter((pl.col("Snapshot Date") == PRIOR) & (pl.col("Epic Key") == "E1"))
    assert prior_e1["Min Sprint"].item() == "26.1.1"
    assert prior_e1["Max Sprint"].item() == "26.1.IP"       # IP sorts last within the PI
    assert prior_e1["Current Sprint"].item() == "26.1.2"    # sprint containing today
    assert prior_e1["Snapshot Date Alt"].item() == datetime(PRIOR.year, PRIOR.month, PRIOR.day)

    today_e1 = df.filter((pl.col("Snapshot Date") == TODAY) & (pl.col("Epic Key") == "E1"))
    by_pi = {r["Program Increment"]: r for r in today_e1.to_dicts()}
    assert (by_pi["PI 26.1"]["Min Sprint"], by_pi["PI 26.1"]["Max Sprint"]) == ("26.1.1", "26.1.IP")
    assert (by_pi["PI 26.2"]["Min Sprint"], by_pi["PI 26.2"]["Max Sprint"]) == ("26.2.1", "26.2.1")
    assert by_pi["PI 26.1"]["Current Sprint"] == "26.1.2"
    assert by_pi["PI 26.2"]["Current Sprint"] is None       # today is not inside PI 26.2
    assert by_pi["PI 26.1"]["Snapshot Date Alt"] == NOW      # summary rows carry the run time

    orphan = df.filter(pl.col("Epic Key") == "E3")
    assert orphan["Min Sprint"].item() is None and orphan["Current Sprint"].item() is None


def test_build_acrp_release_range_per_feature():
    _, acrp = build()

    assert all(col == col.upper() for col in acrp.columns)   # SCREAMING_SNAKE_CASE output
    assert acrp["SNAPSHOT_DATE"].null_count() == acrp.height  # summary rows only
    assert "E3" not in acrp["EPIC_KEY"].to_list()             # no feature / sub-capability

    e1 = acrp.filter(pl.col("EPIC_KEY") == "E1")
    assert e1.height == 4                                     # 2 PIs x 2 fix versions
    assert set(e1["FEATURE_FIX_VERSION"].to_list()) == {"R26.1", "R26.2"}   # split + stripped
    assert e1["MIN_TARGET_RELEASE"].unique().to_list() == ["R26.1"]
    assert e1["MAX_TARGET_RELEASE"].unique().to_list() == ["R26.2"]

    e2 = acrp.filter(pl.col("EPIC_KEY") == "E2")
    assert e2.height == 1
    assert (e2["MIN_TARGET_RELEASE"].item(), e2["MAX_TARGET_RELEASE"].item()) == ("R26.2", "R26.2")
    # ACRP carries the transformed columns too (workbooks reference them)
    assert {"LAST_UPDATED", "SNAPSHOT_DATE_ALT", "MIN_SPRINT", "MAX_SPRINT", "CURRENT_SPRINT"} <= set(acrp.columns)


def test_build_acrp_on_a_frame_without_summary_rows_is_empty():
    df = pl.DataFrame({
        "SNAPSHOT_DATE": [PRIOR], "EPIC_KEY": ["E1"], "FEATURE_KEY": ["F1"],
        "SUBCAPABILITY_KEY": ["SC1"], "FEATURE_FIX_VERSION": ["R26.1"],
    })
    out = build_acrp(df)
    assert out.height == 0
    assert {"MIN_TARGET_RELEASE", "MAX_TARGET_RELEASE"} <= set(out.columns)


def test_join_agile_fans_out_per_program_increment_and_drops_the_right_key():
    epics = pl.DataFrame({"EPIC_KEY": ["E1", "E3"], "FEATURE_KEY": ["F1", None]})
    out = join_agile(epics, agile_summary_frame(), by_snapshot=False)
    assert out.height == 3
    assert "FEATURE_ID" not in out.columns
    assert out.filter(pl.col("EPIC_KEY") == "E3")["PROGRAM_INCREMENT"].item() is None


def test_sprint_lookups_current_sprint_is_at_most_one_row_per_partition():
    lookups = build_sprint_lookups(sprint_range_history_frame(), sprint_range_summary_frame(), today=TODAY)
    assert lookups.range_summary.sort("PROGRAM_INCREMENT").to_dicts() == [
        {"PROGRAM_INCREMENT": "PI 26.1", "MIN_SPRINT": "26.1.1", "MAX_SPRINT": "26.1.IP"},
        {"PROGRAM_INCREMENT": "PI 26.2", "MIN_SPRINT": "26.2.1", "MAX_SPRINT": "26.2.1"},
    ]
    assert lookups.current_summary.to_dicts() == [{"PROGRAM_INCREMENT": "PI 26.1", "CURRENT_SPRINT": "26.1.2"}]
    assert lookups.range_history.height == 1
    assert lookups.current_history["CURRENT_SPRINT"].to_list() == ["26.1.2"]
    assert lookups.range_history.schema["SNAPSHOT_DATE"] == pl.Date


def test_sprint_sort_key_orders_ip_last_and_round_trips():
    versions = ["26.1.2", "26.1.IP", "26.1.10", "25.4.1"]
    df = pl.DataFrame({"SPRINT_VERSION": versions}).with_columns(_sprint_sort_key().alias("k"))
    assert df["k"].to_list() == [260102, 260199, 260110, 250401]
    assert df.select(_sort_key_to_version("k", "v"))["v"].to_list() == versions


def test_baseline_planned_end_is_carried_onto_snapshot_rows():
    """The history table has no PLANNED_END (the history SQL selects NULL), so
    every snapshot row takes its feature's current value from the summary:
    one baseline per feature across time. ACRP carries the live value."""
    df, acrp = build()

    e1 = df.filter(pl.col("Epic Key") == "E1")
    assert e1.height == 3                                     # prior snapshot + today x 2 PIs
    assert e1["Baseline Planned End"].unique().to_list() == [datetime(2026, 10, 1)]
    e2 = df.filter(pl.col("Epic Key") == "E2")
    assert e2["Baseline Planned End"].unique().to_list() == [datetime(2026, 11, 15)]
    assert df.filter(pl.col("Epic Key") == "E3")["Baseline Planned End"].item() is None
    assert df.schema["Baseline Planned End"] == pl.Datetime("us")   # stays a timestamp, not text

    assert acrp.filter(pl.col("EPIC_KEY") == "E1")["BASELINE_PLANNED_END"].unique().to_list() == [datetime(2026, 10, 1)]


def test_carry_baseline_from_summary_keeps_values_and_leaves_unknown_features_null():
    summary = pl.DataFrame({
        "FEATURE_KEY": ["F1", "F1", None],                    # F1 appears twice (two epics / PIs)
        "BASELINE_PLANNED_END": pl.Series([datetime(2026, 10, 1)] * 2 + [None], dtype=pl.Datetime("us")),
    })
    df = pl.DataFrame({
        "EPIC_KEY": ["E1", "E9", "E3", "E1"],
        "FEATURE_KEY": ["F1", "F9", None, "F1"],              # F9 is gone from the summary
        "BASELINE_PLANNED_END": pl.Series(
            [None, None, None, datetime(2026, 9, 1)], dtype=pl.Datetime("us")),
    })
    out = carry_baseline_from_summary(df, summary)
    assert out.height == 4 and out.columns == df.columns
    assert out["BASELINE_PLANNED_END"].to_list() == [
        datetime(2026, 10, 1),   # filled from the summary
        None,                    # feature unknown to the summary
        None,                    # no feature at all
        datetime(2026, 9, 1),    # existing value kept
    ]
    assert carry_baseline_from_summary(df.drop("BASELINE_PLANNED_END"), summary).columns == ["EPIC_KEY", "FEATURE_KEY"]


def test_planned_dates_are_per_snapshot_unlike_the_baseline():
    """PLANNED_START / PLANNED_END (the database TARGET_* dates) come from the
    history table too, so each snapshot keeps its own value; only the
    baseline column is carried across from the summary."""
    df, acrp = build()

    e1_prior = df.filter((pl.col("Snapshot Date") == PRIOR) & (pl.col("Epic Key") == "E1"))
    assert e1_prior["Planned Start"].item() == datetime(2026, 3, 1)
    assert e1_prior["Planned End"].item() == datetime(2026, 10, 1)            # as of that snapshot
    assert e1_prior["Baseline Planned End"].item() == datetime(2026, 10, 1)   # carried from summary

    e1_today = df.filter((pl.col("Snapshot Date") == TODAY) & (pl.col("Epic Key") == "E1"))
    assert e1_today["Planned End"].unique().to_list() == [datetime(2026, 10, 15)]   # live value
    assert df.schema["Planned End"] == pl.Datetime("us")

    assert {"PLANNED_START", "PLANNED_END", "BASELINE_PLANNED_END"} <= set(acrp.columns)
