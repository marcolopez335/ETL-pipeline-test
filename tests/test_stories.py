"""Stories build chain on in-memory frames: union, feature join, transforms."""

from datetime import datetime, timedelta

import polars as pl

from conversion.stories_table import apply_transforms, build_stories, join_features

NOW = datetime(2026, 4, 8, 10, 30)          # run time (a Wednesday)
TODAY = NOW.date()
PRIOR = TODAY - timedelta(days=14)          # an earlier snapshot day
PRIOR_DT = datetime(PRIOR.year, PRIOR.month, PRIOR.day)


def summary_frame() -> pl.DataFrame:
    """Live stories: NULL SNAPSHOT_DATE, S-1 finished since the morning snapshot."""
    return pl.DataFrame({
        "STORY_NUMBER": ["S-1", "S-2"],
        "PROJECT_NAME": ["AMMM", "AMMM"],
        "SPRINT_NAME": ["AMMM 26.1.2", "AMMM 26.1.IP"],
        "FIX_VERSION": ["R26.2", "R26.2"],
        "STATUS": ["Done", "In Progress"],
        "SNAPSHOT_DATE": pl.Series([None, None], dtype=pl.Datetime("us")),
        "TARGET_START": pl.Series([datetime(2026, 3, 1), None], dtype=pl.Datetime("us")),
        "FEATURE_ID": ["F-1", "F-1"],
    })


def history_frame() -> pl.DataFrame:
    """Prior snapshot plus a snapshot taken this morning (must be dropped)."""
    return pl.DataFrame({
        "STORY_NUMBER": ["S-1", "S-2", "S-1", "S-2"],
        "PROJECT_NAME": ["AMMM"] * 4,
        "SPRINT_NAME": ["AMMM 26.1.2"] * 4,
        "FIX_VERSION": ["R26.2"] * 4,
        "STATUS": ["In Progress"] * 4,
        "SNAPSHOT_DATE": pl.Series([PRIOR, PRIOR, TODAY, TODAY]).cast(pl.Datetime("us")),
        "TARGET_START": pl.Series([None] * 4, dtype=pl.Datetime("us")),
        "FEATURE_ID": ["F-1"] * 4,
        "IS_SYNTHETIC": [False] * 4,
    })


def features_frame() -> pl.DataFrame:
    """Feature lookup: live row (NULL snapshot) + the prior snapshot, whose
    database timestamp carries a time component."""
    return pl.DataFrame({
        "FEATURE_ID": ["F-1", "F-1"],
        "SNAPSHOT_DATE": pl.Series(
            [None, PRIOR_DT + timedelta(hours=3, minutes=15)], dtype=pl.Datetime("us")),
        "FEATURE_STATUS": ["Committed", "Planned"],
        "TARGET_START": pl.Series([datetime(2026, 2, 1)] * 2, dtype=pl.Datetime("us")),
        "FEATURE_TOTAL_POINTS": [21.0, 13.0],
    })


def test_build_stories_one_row_per_story_on_the_latest_date():
    df = build_stories(summary_frame(), history_frame(), features_frame(), now=NOW)

    assert df.height == 4
    assert df["Snapshot Date"].null_count() == 0
    assert df.schema["Snapshot Date"] == pl.Datetime("us")   # STORIES.hyper keeps a timestamp

    latest = df.filter(pl.col("Snapshot Date").cast(pl.Date) == TODAY)
    assert latest.height == 2 and latest["Story Number"].n_unique() == 2
    # today's rows come from the live summary, not the morning snapshot
    assert latest.filter(pl.col("Story Number") == "S-1")["Status"].item() == "Done"

    prior = df.filter(pl.col("Snapshot Date").cast(pl.Date) == PRIOR)
    assert prior.height == 2
    assert prior["Status"].unique().to_list() == ["In Progress"]


def test_build_stories_joins_the_feature_as_of_each_snapshot():
    df = build_stories(summary_frame(), history_frame(), features_frame(), now=NOW)

    latest = df.filter(pl.col("Snapshot Date").cast(pl.Date) == TODAY)
    assert latest["Feature Status"].unique().to_list() == ["Committed"]   # NULL matched NULL
    prior = df.filter(pl.col("Snapshot Date").cast(pl.Date) == PRIOR)
    assert prior["Feature Status"].unique().to_list() == ["Planned"]      # 03:15 timestamp still matched
    assert prior["Feature Total Points"].unique().to_list() == [13.0]
    # colliding feature columns keep their historical "_epics" suffix
    assert "Target Start Epics" in df.columns
    assert "Target Start" in df.columns


def test_join_features_aligns_key_dtype_to_the_stories_side():
    stories = pl.DataFrame({
        "FEATURE_ID": ["F-1"],
        "SNAPSHOT_DATE": pl.Series([PRIOR_DT], dtype=pl.Datetime("us")),
    })
    features = pl.DataFrame({
        "FEATURE_ID": ["F-1"],
        "SNAPSHOT_DATE": [PRIOR],          # Date on the lookup side
        "FEATURE_STATUS": ["Planned"],
    })
    out = join_features(stories, features)
    assert out["FEATURE_STATUS"].to_list() == ["Planned"]
    assert out.schema["SNAPSHOT_DATE"] == pl.Datetime("us")


def test_apply_transforms_computed_columns_and_order():
    df = pl.DataFrame({
        "STORY_NUMBER": ["S-1", "S-2"],
        "PROJECT_NAME": ["AMMM", "AMMM"],
        "FIX_VERSION": ["R26.2", "R26.3"],
        "SPRINT_NAME": ["AMMM 26.1.2", "AMMM 26.1.IP"],
        "SNAPSHOT_DATE": pl.Series([None, PRIOR_DT], dtype=pl.Datetime("us")),
    })
    out = apply_transforms(df, now=NOW)

    assert out["LAST_UPDATED"].to_list() == [NOW, NOW]
    assert out["PROJECT_NAME_VERSION"].to_list() == ["AMMM R26.2", "AMMM R26.3"]
    assert out["SPRINT_NAME_ALT"].to_list() == ["26.1.2", "26.1.IP"]
    assert out["PI_FROM_SPRINT"].to_list() == ["26.1", "26.1"]
    # summary row: exact run time in _ALT, midnight of today in SNAPSHOT_DATE
    assert out["SNAPSHOT_DATE_ALT"].to_list() == [NOW, PRIOR_DT]
    assert out["SNAPSHOT_DATE"].to_list() == [datetime(TODAY.year, TODAY.month, TODAY.day), PRIOR_DT]
    assert out.columns[-5:] == [
        "LAST_UPDATED", "PROJECT_NAME_VERSION", "SPRINT_NAME_ALT", "SNAPSHOT_DATE_ALT", "PI_FROM_SPRINT",
    ]
