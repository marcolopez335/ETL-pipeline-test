"""Hand the stories frame to the React dashboard in dashboards/.

export_dashboard_json() runs on the final stories frame (Title Case columns,
today's summary rows plus the history snapshots) and writes one JSON document:

    {"source": "pipeline", "generated": ..., "extract": ..., "load": ...,
     "pis": {"26.3": {"snapshots": [...], "start": ..., "end": ...}, ...},
     "stories": [{...one record per story: its latest row...}]}

Each story record carries the STORIES.hyper columns the dashboard reads plus
two snapshot facts: firstSeen (the first Snapshot Date the story appears in)
and doneOn (the first one where its Status is Done or Accepted) -- all the
burn-up needs. The dashboard falls back to its sample data when the file is
missing, so skipping this export never breaks it.
"""
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import polars as pl

from conversion.shared import get_logger

logger = get_logger(__name__)

DONE_STATUSES = ["Done", "Accepted"]
PI_NUMBER_PATTERN = r"(\d{2}\.\d)"
KEY = "Story Number"

# Title Case column on the stories frame -> field name in the JSON. The
# dashboard documents the same mapping in dashboards/README.md.
FIELDS = {
    "Story Number": "storyNumber",
    "Story Name": "storyName",
    "Team Name": "teamName",
    "Sprint Name": "sprintName",
    "Program Increment": "programIncrement",
    "Fix Version": "fixVersion",
    "Status": "status",
    "Estimate": "estimate",
    "Closed Date": "closedDate",
    "Feature Id": "featureId",
    "Feature": "feature",
    "Feature Team": "featureTeam",
    "Feature Fix Version": "featureFixVersion",
    "Feature Status": "featureStatus",
    "Bv": "bv",
    "Target Start": "targetStart",
    "Target End": "targetEnd",
}


def _pi_number(df: pl.DataFrame, col: str) -> pl.Expr:
    """The PI number ('26.3') inside a column, or null when the frame lacks it."""
    if col not in df.columns:
        return pl.lit(None, dtype=pl.Utf8)
    return pl.col(col).cast(pl.Utf8).str.extract(PI_NUMBER_PATTERN)


def _col(df: pl.DataFrame, name: str) -> pl.Expr:
    return pl.col(name) if name in df.columns else pl.lit(None, dtype=pl.Utf8).alias(name)


def _bound(df: pl.DataFrame, col: str, how: str):
    """min/max of a date-like column as a date, or None."""
    if col not in df.columns or df.height == 0:
        return None
    series = df[col].cast(pl.Datetime, strict=False)
    value = series.min() if how == "min" else series.max()
    return value.date() if isinstance(value, datetime) else value


def _json_value(value):
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _stamp(dt: datetime | None) -> str | None:
    """'10/26/2026 11:02:48 AM CDT', like the sheet's data-load banner."""
    if dt is None:
        return None
    text = dt.strftime("%m/%d/%Y %I:%M:%S %p").lstrip("0")
    tz = dt.astimezone().tzname() or ""
    return f"{text} {tz}".strip()


def build_dashboard_dataset(df: pl.DataFrame, now: datetime | None = None) -> dict:
    """The dashboard's dataset as a plain dict (see the module docstring)."""
    now = now or datetime.now()
    out = {
        "source": "pipeline",
        "generated": now.isoformat(timespec="seconds"),
        "extract": None,
        "load": _stamp(now),
        "pis": {},
        "stories": [],
    }
    if df.height == 0:
        return out

    work = df.with_columns([
        pl.col("Snapshot Date").cast(pl.Date, strict=False).alias("_snap"),
        pl.coalesce([_pi_number(df, "Feature Pi"), _pi_number(df, "Pi From Sprint")]).alias("_pi"),
        _pi_number(df, "Pi From Sprint").alias("_sprint_pi"),
    ])

    # Snapshot facts per story, then the newest row per story carries them
    facts = work.group_by(KEY).agg([
        pl.col("_snap").min().alias("firstSeen"),
        pl.col("_snap").filter(pl.col("Status").is_in(DONE_STATUSES)).min().alias("doneOn"),
    ])
    latest = (
        work.sort("_snap", nulls_last=False)
        .unique(subset=[KEY], keep="last", maintain_order=True)
        .join(facts, on=KEY, how="left")
    )

    # One window per PI: the snapshots its stories appear in, and the PI's
    # own dates from the sprints that belong to it
    for pi in sorted(work["_pi"].drop_nulls().unique().to_list()):
        snaps = work.filter(pl.col("_pi") == pi)["_snap"].drop_nulls().unique().sort().to_list()
        if not snaps:
            continue
        sprints = work.filter(pl.col("_sprint_pi") == pi)
        start = _bound(sprints, "Begin Date", "min") or snaps[0]
        end = _bound(sprints, "End Date", "max") or snaps[-1]
        out["pis"][pi] = {
            "snapshots": [d.isoformat() for d in snaps],
            "start": start.isoformat(),
            "end": end.isoformat(),
        }

    columns = [_col(latest, c).alias(f) for c, f in FIELDS.items()]
    columns += [pl.col("_pi").alias("featurePi"), pl.col("firstSeen"), pl.col("doneOn")]
    out["stories"] = [
        {k: _json_value(v) for k, v in row.items()}
        for row in latest.select(columns).rows(named=True)
    ]

    for col in ("Imet Summary Last Updated", "Updated"):
        if col in df.columns:
            value = df[col].cast(pl.Datetime, strict=False).max()
            if value is not None:
                out["extract"] = _stamp(value)
                break
    return out


def export_dashboard_json(df: pl.DataFrame, path: Path, now: datetime | None = None) -> Path:
    """Write the dashboard dataset; creates the folder on demand."""
    data = build_dashboard_dataset(df, now=now)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    logger.info(f"Dashboard dataset: {len(data['stories'])} stories, {len(data['pis'])} PI(s) -> {path}")
    return path
