"""Guard against drift between schemas/datatypes.py and the SQL select lists.

A schema key that no query returns is silently skipped by clean_dtypes, so
a renamed SQL column would quietly lose its cast. This parses each query's
final SELECT list and checks every declared column is really produced.
"""

import re
from pathlib import Path

import pytest

from schemas import datatypes

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
SUPPORTED_TYPES = {"datetime", "date", "float", "string"}

# One mapping per pipeline, covering every query that pipeline runs.
CASES = {
    "stories": (datatypes.EXPECTED_DTYPES_STORIES,
                ["Asum.sql", "Ahist.sql", "Ahist_recent.sql", "EsumEhist.sql"]),
    "epics": (datatypes.EXPECTED_DTYPES_EPICS,
              ["EpicSummary.sql", "EpicHistory.sql", "EpicHistory_recent.sql",
               "AgileHistory.sql", "AgileSummary.sql",
               "AgileSprintRange.sql", "AgileSprintRange_summary.sql"]),
}


def _split_top_level(text: str) -> list[str]:
    """Split on commas that are not inside parentheses."""
    depth, buf, parts = 0, [], []
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
    parts.append("".join(buf))
    return parts


def sql_output_columns(filename: str) -> set[str]:
    """Column names produced by the query's final SELECT (after any CTEs)."""
    text = (SQL_DIR / filename).read_text(encoding="utf-8", errors="replace")
    text = re.sub(r"--[^\n]*", "", text)
    upper = text.upper()
    start = upper.rfind("SELECT") + len("SELECT")
    end = upper.find("FROM", start)
    select_list = text[start:end].lstrip()
    if select_list.upper().startswith("DISTINCT"):
        select_list = select_list[len("DISTINCT"):]

    columns = set()
    for item in _split_top_level(select_list):
        item = " ".join(item.split())
        if not item:
            continue
        alias = re.search(r"\bAS\s+(\w+)$", item, re.IGNORECASE)
        columns.add((alias.group(1) if alias else item.split(".")[-1]).upper())
    return columns


@pytest.mark.parametrize("name", list(CASES))
def test_every_schema_column_is_returned_by_its_sql(name):
    schema, files = CASES[name]
    available = set().union(*(sql_output_columns(f) for f in files))
    unknown = sorted(set(schema) - available)
    assert not unknown, f"{name}: columns not returned by {files}: {unknown}"


def test_schema_type_names_are_supported():
    for attr in dir(datatypes):
        if attr.startswith("EXPECTED_DTYPES_"):
            assert set(getattr(datatypes, attr).values()) <= SUPPORTED_TYPES, attr


def test_summary_and_history_queries_share_a_select_list():
    stories = sql_output_columns("Asum.sql")
    assert sql_output_columns("Ahist.sql") == stories
    assert sql_output_columns("Ahist_recent.sql") == stories

    epics = sql_output_columns("EpicSummary.sql") | {"SNAPSHOT_DATE"}
    assert sql_output_columns("EpicHistory.sql") == epics
    assert sql_output_columns("EpicHistory_recent.sql") == epics

    assert {"STORY_NUMBER", "SNAPSHOT_DATE", "FEATURE_ID"} <= stories   # cache key + feature join key
    assert {"EPIC_KEY", "FEATURE_KEY"} <= epics                          # cache key + agile join key


def test_config_references_only_existing_sql_files():
    import yaml

    config = yaml.safe_load((SQL_DIR.parent / "config.yaml").read_text(encoding="utf-8"))
    for section in ("stories", "epics"):
        for key, value in config[section].items():
            if key.startswith("sql_"):
                assert (SQL_DIR / value).is_file(), f"{section}.{key} -> {value} missing"
