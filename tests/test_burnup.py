"""Feature burn-up transform."""

from datetime import datetime, timezone

import polars as pl

from conversion.burnup_table import build_burnup


def source_frame() -> pl.DataFrame:
    return pl.DataFrame({
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


def test_build_burnup_flags_and_stamps():
    # Fixed run time: 2026-07-01 15:00 UTC -> 10:00 CDT (US Central, DST -5)
    out = build_burnup(source_frame(), run_timestamp=datetime(2026, 7, 1, 15, 0, tzinfo=timezone.utc))

    assert out.height == 5                        # 3 features x 3 date cols, minus 4 null dates
    assert out["SOURCE_TYPE"].unique().to_list() == ["summary"]
    assert "TARGET_END_REF" in out.columns

    done = out.filter(pl.col("DONE_FEATURES").is_not_null())
    assert done.height == 1 and done["ISSUE_KEY"].item() == "F-1" and done["DATE_TYPE"].item() == "RESOLVED"

    projected = out.filter(pl.col("PROJECTED_FEATURES").is_not_null())
    assert projected.height == 1 and projected["ISSUE_KEY"].item() == "F-2"

    planned = out.filter(pl.col("PLANNED_FEATURES").is_not_null())
    assert sorted(planned["ISSUE_KEY"].to_list()) == ["F-2", "F-3"]

    last_updated = out["LAST_UPDATED"][0]
    assert (last_updated.hour, last_updated.minute) == (10, 0)
    imet = out["IMET_SUMMARY_LAST_UPDATED"][0]
    assert (imet.hour, imet.minute) == (12, 30)
    snap = out["SNAPSHOT_DATE"][0]
    assert (snap.year, snap.month, snap.day, snap.hour) == (2026, 7, 1, 0)
    resolved = out.filter(pl.col("DATE_TYPE") == "RESOLVED")["DATE_VALUE"].item()
    assert (resolved.hour, resolved.minute) == (0, 0)
