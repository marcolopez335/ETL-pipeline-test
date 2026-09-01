"""Expected column dtypes consumed by ``conversion.shared.clean_dtypes``.

Each mapping is ``{COLUMN_NAME: "datetime" | "date" | "float" | "string"}``
and describes the columns a SQL query returns (see ``sql/README.md``). Keys
must match the SQL output names exactly -- Tibco returns SCREAMING_SNAKE_CASE.
Columns absent from a DataFrame are skipped, so one mapping can cover every
query a pipeline runs: by design the summary, full-history and recent-history
queries of a pipeline share a select list.

Casts are non-strict (unconvertible values become null). Columns whose
database type is not known for certain (BV, SWAG, SPRINT_COUNT) are left out
on purpose so their native type passes through to the .hyper output as-is.

``tests/test_schemas.py`` checks every key below against the SQL files, so a
renamed SQL column fails CI instead of silently skipping its cast.
"""

# Story rows -- Asum.sql (summary), Ahist.sql / Ahist_recent.sql (history).
EXPECTED_DTYPES_STORIES: dict[str, str] = {
    # Identifiers & names
    "STORY_NUMBER": "string",
    "STORY_NAME": "string",
    "STORY_TYPE": "string",
    "PROJECT_KEY": "string",
    "PROJECT_NAME": "string",
    "TEAM_NAME": "string",
    "SPRINT_NAME": "string",
    "PROGRAM_INCREMENT": "string",
    "FIX_VERSION": "string",

    # Status
    "STATUS": "string",
    "RESOLUTION": "string",

    # Hierarchy above the story (the *_ID columns are Jira issue keys)
    "EPIC_ID": "string",
    "EPIC": "string",
    "FEATURE_ID": "string",
    "FEATURE": "string",
    "SUB_CAPABILITY_ID": "string",
    "SUB_CAPABILITY": "string",
    "CUSTOMER_CAPABILITY_ID": "string",
    "CUSTOMER_CAPABILITY": "string",
    "CUSTOMER_EPIC_ID": "string",
    "CUSTOMER_EPIC": "string",

    # Dates -- SNAPSHOT_DATE stays a timestamp in STORIES.hyper (join key with
    # the feature lookup, and what the workbooks were built on)
    "SNAPSHOT_DATE": "datetime",
    "LAST_UPDATED": "datetime",
    "CREATE_DATE": "datetime",
    "UPDATED": "datetime",
    "CLOSED_DATE": "datetime",
    "BEGIN_DATE": "datetime",
    "END_DATE": "datetime",
    "SPRINT_ACTIVATE_DATE": "datetime",
    "SPRINT_COMPLETE_DATE": "datetime",
    "TARGET_START": "datetime",
    "TARGET_END": "datetime",
    "INITIAL_DATE_TO_IN_PROGRESS": "datetime",

    # Numeric
    "ESTIMATE": "float",
}

# Feature attributes joined onto stories -- EsumEhist.sql (feature summary
# UNION ALL feature history, keyed by FEATURE_ID + SNAPSHOT_DATE).
EXPECTED_DTYPES_FEATURES: dict[str, str] = {
    "FEATURE_ID": "string",
    "FEATURE_TEAM": "string",
    "FEATURE_PI": "string",
    "FEATURE_FIX_VERSION": "string",
    "FEATURE_STATUS": "string",

    # Must match the stories' SNAPSHOT_DATE dtype -- it is the join key
    "SNAPSHOT_DATE": "datetime",
    "TARGET_START": "datetime",
    "TARGET_END": "datetime",
    "ACTUAL_END_DATE": "datetime",
    "IMET_SUMMARY_LAST_UPDATED": "datetime",

    "FEATURE_OPEN_POINTS": "float",
    "FEATURE_TOTAL_POINTS": "float",
}

# Epic hierarchy rows -- EpicSummary.sql (summary), EpicHistory.sql /
# EpicHistory_recent.sql (history). One row per Epic with its Feature,
# Sub-Capability, Customer Capability and Customer Epic flattened on.
EXPECTED_DTYPES_EPICS: dict[str, str] = {
    "EPIC_KEY": "string",
    "EPIC_SUMMARY": "string",
    "FEATURE_KEY": "string",
    "FEATURE_SUMMARY": "string",
    "FEATURE_FIX_VERSION": "string",
    "SUBCAPABILITY_KEY": "string",
    "CUSTCAP_KEY": "string",
    "CUSTEPIC_KEY": "string",
    "PROGRAM": "string",

    # Daily grain -- a Date, matching the agile rollups it is joined with
    # and the Date column EPICS.hyper has always carried
    "SNAPSHOT_DATE": "date",

    "EPIC_ESTIMATE": "float",
    "EPIC_OPEN_ESTIMATE": "float",
    "FEATURE_ESTIMATE": "float",
    "FEATURE_OPEN_ESTIMATE": "float",
    "CUSTCAP_ESTIMATE": "float",
}

# Agile rollups and sprint ranges -- AgileHistory.sql, AgileSummary.sql,
# AgileSprintRange.sql, AgileSprintRange_summary.sql.
EXPECTED_DTYPES_AGILE: dict[str, str] = {
    "FEATURE_ID": "string",
    "PROGRAM_INCREMENT": "string",
    "SPRINT_NAME": "string",

    "SNAPSHOT_DATE": "date",
    "BEGIN_DATE": "datetime",
    "END_DATE": "datetime",

    "TOTAL_ESTIMATE": "float",
    "DONE_ESTIMATE": "float",
    "OPEN_ESTIMATE": "float",
}
