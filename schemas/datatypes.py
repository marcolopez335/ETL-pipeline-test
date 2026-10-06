"""Expected column dtypes consumed by ``conversion.shared.clean_dtypes``.

One mapping per pipeline, ``{COLUMN_NAME: "datetime" | "float" | "string"}``,
covering the queries that pipeline casts (see ``sql/README.md``). Keys must
match the SQL output names exactly -- Tibco returns SCREAMING_SNAKE_CASE.
Columns absent from a DataFrame are skipped, so one mapping can describe the
summary, the history and the lookup queries together.

Casts are non-strict (unconvertible values become null). Columns whose
database type is not known for certain (BV, SWAG) are left out on purpose so
their native type passes through to the .hyper output as-is.

When a SQL query gains a column that needs a cast, add it here in the same
commit.
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

    # Dates -- SNAPSHOT_DATE is also the join key with the feature lookup,
    # so it must be the same type on both sides
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

# Epics pipeline -- EpicSummary.sql (summary) and EpicHistory.sql /
# EpicHistory_recent.sql (history). One row per feature: the Epic level is
# commented out in the SQL, so there are no EPIC_* columns.
EXPECTED_DTYPES_EPICS: dict[str, str] = {
    # Hierarchy: Feature -> Sub-Capability -> Customer Capability -> Customer Epic
    "FEATURE_KEY": "string",
    "FEATURE_SUMMARY": "string",
    "FEATURE_FIX_VERSION": "string",
    "FEATURE_TEAM": "string",
    "FEATURE_PI": "string",        # the feature's own PI; PROGRAM_INCREMENT on the row is story-derived
    "SUBCAPABILITY_KEY": "string",
    "CUSTCAP_KEY": "string",
    "CUSTEPIC_KEY": "string",
    "PROGRAM": "string",

    # Feature status / classification
    "STATUS": "string",
    "TYPE": "string",
    "JIRA_PROJECT_NAME": "string",

    # Feature dates. Workbook naming: PLANNED_START / PLANNED_END are the
    # database TARGET_START / TARGET_END; BASELINE_PLANNED_END is the database
    # PLANNED_END_DATE.
    "PLANNED_START": "datetime",
    "PLANNED_END": "datetime",
    "RESOLVED": "datetime",
    "BASELINE_PLANNED_END": "datetime",
    "SNAPSHOT_DATE": "datetime",   # cast to Date in epics_table before the agile / sprint joins

    # Numeric
    "FEATURE_ESTIMATE": "float",
    "FEATURE_OPEN_ESTIMATE": "float",
    "CUSTCAP_ESTIMATE": "float",
}
