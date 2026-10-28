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
| Feature Progress (PI) | `FeatureTable.jsx` | Feature, Owner, BV, Start, End, Size, Status pill, PI % and Total % meters; sortable headers |
| Program Burnup (PI) | `BurnupChart.jsx` | Ideal / Done / Scope lines, crosshair tooltip, keyboard navigation, legend toggles, table view |
| Analysis / Corrective Action | `AnalysisPanel.jsx` | Notes list plus an add-note form (local state only) |
| Sheet tabs | `TabStrip.jsx` | The production sheet names; only the first sheet is built out |

Wired filters: **Select PI**, **Target Release**, **Owner**, **BV**, **Sort By**
and **Ideal Burnup** change the table, the tiles and the chart together.
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

`src/data/mock.js` holds sample rows in the shape of the pipeline outputs:

| Mock field | EPICS.hyper column |
|---|---|
| `key`, `summary` | `Feature Key`, `Summary` |
| `owner` | `Team` |
| `bv` | `Bv` |
| `start`, `end` | `Begin Date`, `End Date` |
| `size` | `Total Estimate` |
| `status` | `Status` |
| `piPct` | `Done In Feature Pi` / `Total In Feature Pi` |
| `totalPct` | `Done Estimate` / `Total Estimate` |
| `pi`, `release` | `Program Increment`, `Feature Fix Version` |

The burn-up series are weekly `{ date, scope, done, ideal }` points per PI.
To drive the mockup with real data, export the pipeline with
`python main.py --epics --csv`, convert `output/csv/EPICS.csv` to JSON with the
fields above, and import it in place of `features`.
