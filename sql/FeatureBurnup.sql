-- Feature burn-up source: one row per Feature with its raw date fields.
-- Column names are intentionally NOT renamed (unlike EpicSummary.sql) —
-- the burn-up transform consumes the raw AMMM_JIRA_EPIC_SUMMARY names.
-- SUMMARY comes from the Customer Epic level (program name), reached via
-- Feature -> Sub-Capability -> Customer Capability -> Customer Epic.
WITH
Features AS (
    SELECT
        LAST_UPDATED,
        TEAM_NAME,
        ISSUE_KEY,
        ISSUE_TYPE,
        STATUS,
        TARGET_END,
        RESOLVED,
        PLANNED_END,
        PARENT_KEY
    FROM AMMM_JIRA_EPIC_SUMMARY
    WHERE ISSUE_TYPE = 'Feature'
),
SubCapabilities AS (
    SELECT
        ISSUE_KEY AS SUBCAPABILITY_KEY,
        PARENT_KEY AS SUBCAPABILITY_PARENT
    FROM AMMM_JIRA_EPIC_SUMMARY
    WHERE ISSUE_TYPE = 'Sub-Capability'
),
CustomerCapabilities AS (
    SELECT
        ISSUE_KEY AS CUSTCAP_KEY,
        PARENT_KEY AS CUSTCAP_PARENT
    FROM AMMM_JIRA_EPIC_SUMMARY
    WHERE ISSUE_TYPE = 'Customer Capability'
),
CustomerEpics AS (
    SELECT
        ISSUE_KEY AS CUSTEPIC_KEY,
        SUMMARY
    FROM AMMM_JIRA_EPIC_SUMMARY
    WHERE ISSUE_TYPE = 'Customer Epic'
)
SELECT
    FT.LAST_UPDATED,
    FT.TEAM_NAME,
    FT.ISSUE_KEY,
    CE.SUMMARY,
    FT.ISSUE_TYPE,
    FT.STATUS,
    FT.TARGET_END,
    FT.RESOLVED,
    FT.PLANNED_END,
    FT.PARENT_KEY
FROM Features FT
    LEFT JOIN SubCapabilities SCT
        ON SCT.SUBCAPABILITY_KEY = FT.PARENT_KEY
    LEFT JOIN CustomerCapabilities CC
        ON CC.CUSTCAP_KEY = SCT.SUBCAPABILITY_PARENT
    LEFT JOIN CustomerEpics CE
        ON CE.CUSTEPIC_KEY = CC.CUSTCAP_PARENT;
