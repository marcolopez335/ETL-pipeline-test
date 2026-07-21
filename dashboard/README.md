# Dashboard Mockup — Feature Completion by PI / Team / Program

A self-contained, Tableau-style dashboard mockup that sits where Tableau would
sit in this repo's architecture: **on top of the `STORIES.hyper` extract**. The
pipeline's job ends at the hyper file; this mockup explores what the workbook
on top of it could look like — filter shelf, KPI band, burn-up, and a
spreadsheet-like feature completion grid — using fabricated data shaped exactly
like the real extract.

```
Tibco ──► ETL (this repo) ──► STORIES.hyper ──► Tableau workbook
                                   │
                                   └──► (mocked here)  dashboard/index.html
```

## Try it

Open **`dashboard/index.html`** in any browser — no server, no build, no
network. The mock data is embedded in the page.

- **Filters** (scope every tile, chart, and row): PI · Program ·
  Capability · Sub-capability · Owner · Contributor · Release · Health,
  plus feature search. Multi-select with checkbox popovers, Tableau-style
  "(All)" semantics; Program → Capability → Sub-capability cascade.
- **Hierarchy breakdown**: the grid's default view groups rows
  Capability → Sub-capability → Feature with collapsible rollup rows
  (stories, points, PI % / Total % per level); a toggle switches to the
  flat table.
- **KPI tiles**: % points complete (with week-over-week delta), features,
  stories done, needs-attention count.
- **Burn-up**: done vs planned-ideal vs scope story points across the 10
  weekly Monday snapshots, with a value label on every snapshot point
  (matching the prd workbook). Hover or focus + arrow keys for the
  crosshair readout.
- **Completion by team**: click a bar to cross-filter, like Tableau's
  "use as filter".
- **Feature grid**: sortable columns (BV, planned start–end dates,
  stories, points), split **PI %** vs **Total %** meters for features that
  carry work across PIs, health chips (incl. Accepted), and expandable
  rows showing the underlying stories with their owning/contributing team.
- **Analysis / corrective action** notes panel (persisted in the browser).
- Light/dark theme via the toggle (or follows the OS in Auto).

## Files

| File | Purpose |
|------|---------|
| `index.html` | The dashboard. Single file, zero dependencies, data embedded. |
| `generate_mock_data.py` | Deterministic mock data generator (stdlib only). |
| `data/stories_mock.json` | Compact model the dashboard consumes. |
| `data/stories_mock.csv` | Flat extract mirroring `STORIES.hyper` — history snapshots unioned with a current summary row per story, Title Case columns. |

## Regenerating the data

```bash
python dashboard/generate_mock_data.py            # rewrites json + csv, re-embeds into index.html
python dashboard/generate_mock_data.py --seed 7   # different, equally deterministic dataset
python dashboard/generate_mock_data.py --hyper    # also writes output/STORIES_MOCK.hyper
                                                  # (requires polars + pantab, like the real pipeline)
```

The generator is seeded and the "now" timestamp is pinned
(`GENERATED_AT = 2026-07-21 06:30`), so re-running produces byte-identical
files and clean git diffs.

## How the mock maps to the real extract

The dashboard only uses concepts that exist in the pipeline's output, so
rebuilding it as a real Tableau workbook is a straight translation:

| Dashboard concept | Real `STORIES.hyper` source |
|---|---|
| Program filter | `Project Name` |
| Capability / Sub-capability | `Customercapability Key` / `Subcapability Key` from the epics join (the ACRP hierarchy); display names would come from the capability lookup |
| Owner filter | Derived from `Sprint Name` (`"Team Falcon 26.1.5"` → `Team Falcon`) — a Tableau calculated field, same idea as the pipeline's `SPRINT_NAME_PATTERN` |
| Contributor filter | Any team appearing in a feature's story `Sprint Name`s (owner or otherwise) |
| PI filter | `Program Increment` (with `Pi From Sprint` as fallback) |
| Release filter | `Fix Version` (the ACRP target-release concept) |
| Planned start–end | Sprint calendar span of the feature's stories — the epics pipeline's `MIN_SPRINT`/`MAX_SPRINT` |
| Total % | `SUM(Story Points if Status = Done) / SUM(Story Points)` per `Feature Key` |
| PI % | Same ratio over only the stories whose `Pi From Sprint` matches the feature's PI (carryover from prior PIs is excluded) |
| Burn-up | Weekly Monday `Snapshot Date` history (the pipeline's synthetic-snapshot fill guarantees the axis has no gaps) |
| Ideal line | Cumulative points of stories whose planned sprint has ended by each snapshot |
| Week-over-week delta | Current snapshot vs prior Monday |

Fabricated for the mockup (not in the stories schema): feature display names,
business value, the Accepted flag, and story-level health thresholds. In a
real workbook those would come from the epics join, and health rules would be
a team decision (here: `gap = PI-elapsed-% − PI-slice-done-%`; < 15 pts on
track, < 32 pts at risk, otherwise behind; ≥ 2 blocked stories → blocked;
fully done + sign-off → Accepted).

## Notes

- Mock scenario: 3 programs, 8 teams, 3 PIs (25.4 closed, 26.1 in flight,
  26.2 in planning), 45 features, ~390 stories, 10 Monday snapshots.
- Possible next steps if this direction sticks: an "as of" snapshot selector
  (time travel), story-status breakdown per sprint, and pointing the same
  layout at the real extract by swapping the embedded JSON for a small
  export step in the pipeline.
