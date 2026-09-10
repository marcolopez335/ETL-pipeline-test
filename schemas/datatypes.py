"""Expected column dtypes consumed by ``conversion.shared.clean_dtypes``.

One mapping per pipeline, ``{COLUMN_NAME: "datetime" | "date" | "float" | "string"}``,
covering every query that pipeline runs (see ``sql/README.md``). Keys must
match the SQL output names exactly -- Tibco returns SCREAMING_SNAKE_CASE.
Columns absent from a DataFrame are skipped, so it is safe to list every
column the pipeline is aware of and apply the same mapping to the summary,
the history and the lookup queries.

Casts are non-strict (unconvertible values become null). Columns whose
database type is not known for certain (BV, SWAG, SPRINT_COUNT) are left out
on purpose so their native type passes through to the .hyper output as-is.

``tests/test_schemas.py`` checks every key below against the SQL files, so a
renamed SQL column fails CI instead of silently skipping its cast.
"""

# Stories pipeline -- Asum.sql (summary), Ahist.sql / Ahist_recent.sql
# (history) and EsumEhist.sql (the feature lookup joined onto stories).
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

    # Dates -- SNAPSHOT_DATE stays a timestamp in STORIES.hyper (the join key
    # with the feature lookup, and what the workbooks were built on)
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

    # Feature lookup (EsumEhist.sql) -- FEATURE_ID, SNAPSHOT_DATE, TARGET_START
    # and TARGET_END above apply to it as well
    "FEATURE_TEAM": "string",
    "FEATURE_PI": "string",
    "FEATURE_FIX_VERSION": "string",
    "FEATURE_STATUS": "string",
    "ACTUAL_END_DATE": "datetime",
    "IMET_SUMMARY_LAST_UPDATED": "datetime",
    "FEATURE_OPEN_POINTS": "float",
    "FEATURE_TOTAL_POINTS": "float",
}

# Epics pipeline -- EpicSummary.sql (summary), EpicHistory.sql /
# EpicHistory_recent.sql (history), AgileHistory.sql / AgileSummary.sql
# (story-point rollups per feature and PI) and AgileSprintRange*.sql
# (sprint names and dates per PI).
EXPECTED_DTYPES_EPICS: dict[str, str] = {
    # Epic hierarchy: Epic -> Feature -> Sub-Capability -> Customer Capability -> Customer Epic
    "EPIC_KEY": "string",
    "EPIC_SUMMARY": "string",
    "FEATURE_KEY": "string",
    "FEATURE_SUMMARY": "string",
    "FEATURE_FIX_VERSION": "string",
    "SUBCAPABILITY_KEY": "string",
    "CUSTCAP_KEY": "string",
    "CUSTEPIC_KEY": "string",
    "PROGRAM": "string",
    # Feature dates. Workbook naming, kept on purpose: PLANNED_* are the
    # database TARGET_START / TARGET_END; BASELINE_PLANNED_END is the database
    # PLANNED_END (NULL in the history SQL, filled per feature in the build).
    "PLANNED_START": "datetime",
    "PLANNED_END": "datetime",
    "BASELINE_PLANNED_END": "datetime",

    "EPIC_ESTIMATE": "float",
    "EPIC_OPEN_ESTIMATE": "float",
    "FEATURE_ESTIMATE": "float",
    "FEATURE_OPEN_ESTIMATE": "float",
    "CUSTCAP_ESTIMATE": "float",

    # Daily grain -- a Date on every side of the epic / agile / sprint joins,
    # and the Date column EPICS.hyper has always carried
    "SNAPSHOT_DATE": "date",

    # Agile rollups and sprint ranges
    "FEATURE_ID": "string",
    "PROGRAM_INCREMENT": "string",
    "SPRINT_NAME": "string",
    "BEGIN_DATE": "datetime",
    "END_DATE": "datetime",
    "TOTAL_ESTIMATE": "float",
    "DONE_ESTIMATE": "float",
    "OPEN_ESTIMATE": "float",
}
