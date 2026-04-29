"""Expected column dtypes consumed by ``conversion.shared.clean_dtypes``.

Each mapping is ``{COLUMN_NAME: "datetime" | "float" | "string"}``.
Columns absent from a DataFrame are silently skipped, so it is safe to
list every column the pipeline is aware of.

Add new mappings here as SQL queries gain columns. Keys must match the
exact column names returned by the SQL (Tibco returns SCREAMING_SNAKE_CASE).
"""

EXPECTED_DTYPES_STORIES: dict[str, str] = {
    # Identifiers
    "STORY_NUMBER": "string",
    "FEATURE_ID": "string",
    "FEATURE_KEY": "string",
    "EPIC_KEY": "string",
    "PROJECT_NAME": "string",
    "PROJECT_KEY": "string",

    # Status / classification
    "STATUS": "string",
    "RESOLUTION": "string",
    "ISSUE_TYPE": "string",
    "PRIORITY": "string",
    "ASSIGNEE": "string",
    "REPORTER": "string",
    "LABELS": "string",
    "COMPONENTS": "string",

    # Sprint / release
    "SPRINT_NAME": "string",
    "FIX_VERSION": "string",
    "PROGRAM_INCREMENT": "string",

    # Dates
    "SNAPSHOT_DATE": "datetime",
    "CREATED_DATE": "datetime",
    "UPDATED_DATE": "datetime",
    "RESOLVED_DATE": "datetime",
    "DUE_DATE": "datetime",
    "BEGIN_DATE": "datetime",
    "END_DATE": "datetime",

    # Numeric
    "STORY_POINTS": "float",
    "ORIGINAL_ESTIMATE": "float",
    "REMAINING_ESTIMATE": "float",
    "TIME_SPENT": "float",
}

EXPECTED_DTYPES_EPICS: dict[str, str] = {
    # Identifiers
    "EPIC_KEY": "string",
    "FEATURE_KEY": "string",
    "FEATURE_ID": "string",
    "SUBCAPABILITY_KEY": "string",
    "CUSTOMERCAPABILITY_KEY": "string",
    "CUSTOMEREPIC_KEY": "string",
    "PROJECT_NAME": "string",
    "PROJECT_KEY": "string",

    # Status / classification
    "STATUS": "string",
    "RESOLUTION": "string",
    "ISSUE_TYPE": "string",
    "PRIORITY": "string",
    "ASSIGNEE": "string",
    "REPORTER": "string",
    "LABELS": "string",

    # Sprint / release
    "SPRINT_NAME": "string",
    "FIX_VERSION": "string",
    "FEATURE_FIX_VERSION": "string",
    "PROGRAM_INCREMENT": "string",

    # Dates
    "SNAPSHOT_DATE": "datetime",
    "CREATED_DATE": "datetime",
    "UPDATED_DATE": "datetime",
    "RESOLVED_DATE": "datetime",
    "BEGIN_DATE": "datetime",
    "END_DATE": "datetime",

    # Numeric
    "STORY_POINTS": "float",
    "ORIGINAL_ESTIMATE": "float",
    "REMAINING_ESTIMATE": "float",
    "TIME_SPENT": "float",
}
