# dashboards/ — Program Metrics (PI) mockup

A React + CSS mockup of the production **Program Metrics | PI** Tableau sheet
in a glass (frosted, translucent) theme. No charting library, no CSS
framework: plain CSS custom properties, `backdrop-filter` glass panels and a
hand-drawn SVG burn-up chart.

## Run it

```bash
cd dashboards
npm install
npm run dev        # http://localhost:5173
npm run build      # static site in dist/ (relative asset paths, host anywhere)
```

Node 20.19+ or 22.12+ (Vite 7).

## What is on the page

Same sections as production, top to bottom:

| Section | Component | Notes |
|---|---|---|
| Header | `Header.jsx` | Data extract / load stamps, title with the selected PI, dark/light toggle |
| Filter row | `FilterBar.jsx` | Select PI, Target Release, Group, Program, Owner, Contributor, Sort By, Predictability, Ideal Burnup, BV |
| KPI tiles | `KpiRow.jsx` | Features in PI, features done, story points done / scope, performance to plan |
| Feature Progress (PI) | `FeatureTable.jsx` | Feature, Owner, BV, Start, End, Size, Status pill, PI % and Total % meters; click a header name to sort, its funnel to filter (text search on Feature, pick-list on Owner, BV and Status, from/to range on the dates, Size and the percentages) |
| Program Burnup (PI) | `BurnupChart.jsx` | Ideal / Done / Scope lines, crosshair tooltip, keyboard navigation, legend toggles, table view |
| Analysis / Corrective Action | `AnalysisPanel.jsx` | Notes list plus an add-note form (local state only) |
| Sheet tabs | `TabStrip.jsx` | The production sheet names; only the first sheet is built out |

All ten filters sit on one row with no scrolling (they wrap into rows only on
narrow screens). Wired filters: **Select PI**, **Target Release**, **Owner**,
**Contributor**, **BV**, **Sort By** and **Ideal Burnup** change the table, the
tiles and the chart together, and so do the column header filters: the tiles
and the burn-up are rebuilt from the stories of the features left in the table.
Group, Program, Contributor and Predictability are present for layout fidelity
and do nothing yet.

## Theme

- `src/styles/tokens.css` — every color as a custom property. Dark is the
  default; the header toggle stamps `data-theme="light"` on `<html>` and the
  choice is remembered in `localStorage` when available.
- `src/styles/glass.css` — the gradient plane, the three blurred color blobs,
  and the `.glass` panel recipe (translucent fill, `backdrop-filter` blur,
  hairline border, 1px top highlight). Motion respects `prefers-reduced-motion`.
- `src/styles/app.css` — layout and components. One column on phones, table
  and filter row scroll inside their cards, two columns from 1100px.

Chart colors are the first three slots of a colorblind-validated categorical
palette (Ideal = blue, Scope = orange, Done = aqua), stepped per theme; status
pills use a fixed status color with an icon and a label, never color alone.

## Data

The production sheet is built from the **stories** datasource, and so is the
mockup: `src/data/mock.js` holds sample rows in the shape of `STORIES.hyper`
(one row per story, carrying the feature lookup columns the stories pipeline
joins on), and `src/lib/aggregate.js` turns them into the two views:

| View | How it is aggregated from story rows |
|---|---|
| Feature Progress (PI) | one row per `Feature Id`; Owner, BV, Start, End, Status from `Feature Team`, `Bv`, `Target Start`, `Target End`, `Feature Status`; Size = story count; PI % = done points / total points for stories tagged with the feature's own PI; Total % = done points / total points over every story |
| Program Burnup (PI) | per weekly Snapshot Date: Scope = sum of `Estimate` over the stories present in that snapshot, Done = the same sum over stories whose Status is Done or Accepted, Ideal = straight line from the PI's first snapshot to the final scope |

| Mock field | STORIES.hyper column |
|---|---|
| `storyNumber`, `storyName` | `Story Number`, `Story Name` |
| `teamName`, `sprintName` | `Team Name`, `Sprint Name` |
| `programIncrement`, `fixVersion` | `Program Increment`, `Fix Version` |
| `status`, `estimate`, `closedDate` | `Status`, `Estimate`, `Closed Date` |
| `featureId`, `feature` | `Feature Id`, `Feature` |
| `featureTeam`, `featurePi`, `featureFixVersion`, `featureStatus` | `Feature Team`, `Feature Pi`, `Feature Fix Version`, `Feature Status` |
| `bv`, `targetStart`, `targetEnd` | `Bv`, `Target Start`, `Target End` |
| `addedWeek`, `doneWeek` | stand-ins for the history snapshots: the first weekly `Snapshot Date` the story appears in, and the first one where it is Done |

The sample stories are generated deterministically from a hand-written
feature list (`features` in `mock.js`: story count, progress, dates). To
drive the mockup with real data, export the pipeline with
`python main.py --stories --csv`, convert `output/csv/STORIES.csv` to JSON
with the fields above (one `addedWeek` / `doneWeek` per story from its
snapshot history), and import it in place of `stories`.
