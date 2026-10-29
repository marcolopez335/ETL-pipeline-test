import { useEffect, useMemo, useState } from 'react'
import Header from './components/Header.jsx'
import FilterBar from './components/FilterBar.jsx'
import KpiRow from './components/KpiRow.jsx'
import FeatureTable from './components/FeatureTable.jsx'
import BurnupChart from './components/BurnupChart.jsx'
import AnalysisPanel from './components/AnalysisPanel.jsx'
import TabStrip, { TABS } from './components/TabStrip.jsx'
import { analysisNotes, PI_WINDOWS, stories } from './data/mock.js'
import { burnup, featureRows } from './lib/aggregate.js'
import { COLUMNS } from './lib/columns.js'
import { anyActive, applyColumnFilters } from './lib/filters.js'

const DEFAULT_FILTERS = {
  pi: '26.3', release: '(All)', group: '(All)', program: '(All)', owner: '(All)',
  contributor: '(All)', sortBy: 'END Date', predictability: 'Hide', idealBurnup: 'Planned', bv: 'ALL',
}
const SORT_KEY = { 'END Date': 'end', 'START Date': 'start', BV: 'bv', Feature: 'key', Status: 'status', Owner: 'owner' }
const BV_RULE = {
  ALL: () => true,
  '8 and up': (bv) => bv != null && bv >= 8,
  '5 and up': (bv) => bv != null && bv >= 5,
  '4 and under': (bv) => bv != null && bv <= 4,
}

const readTheme = () => {
  try { return localStorage.getItem('pm-theme') || 'dark' } catch { return 'dark' }
}

/* Nulls and blanks sort last in either direction. */
const compare = (a, b, key, dir) => {
  const av = a[key], bv = b[key]
  const aNull = av == null || av === '', bNull = bv == null || bv === ''
  if (aNull && bNull) return 0
  if (aNull) return 1
  if (bNull) return -1
  const r = typeof av === 'number' ? av - bv : String(av).localeCompare(String(bv))
  return dir === 'asc' ? r : -r
}

export default function App() {
  const [theme, setTheme] = useState(readTheme)
  const [filters, setFilters] = useState(DEFAULT_FILTERS)
  const [sort, setSort] = useState({ key: 'end', dir: 'asc' })
  const [tab, setTab] = useState(TABS[0])
  const [colFilters, setColFilters] = useState({})

  useEffect(() => {
    document.documentElement.dataset.theme = theme
    try { localStorage.setItem('pm-theme', theme) } catch { /* private window: theme just won't persist */ }
  }, [theme])

  const setFilter = (id, value) => {
    setFilters((f) => ({ ...f, [id]: value }))
    if (id === 'sortBy') setSort({ key: SORT_KEY[value] || 'end', dir: 'asc' })
    if (id === 'pi') setColFilters({})     // another PI has other values to pick from
  }
  const setColFilter = (key, value) => setColFilters((f) => {
    const next = { ...f }
    if (value == null) delete next[key]; else next[key] = value
    return next
  })
  const onSort = (key) => setSort((s) => (s.key === key ? { key, dir: s.dir === 'asc' ? 'desc' : 'asc' } : { key, dir: 'asc' }))

  /* The slice every section shares: story rows for the selected PI, cut by
     the feature-level filters (Owner, Target Release, BV) and by the team
     that worked the story (Contributor). */
  const slice = useMemo(() => stories
    .filter((s) => s.featurePi === filters.pi)
    .filter((s) => filters.owner === '(All)' || s.featureTeam === filters.owner)
    .filter((s) => filters.contributor === '(All)' || s.teamName === filters.contributor)
    .filter((s) => filters.release === '(All)' || s.featureFixVersion === filters.release)
    .filter((s) => (BV_RULE[filters.bv] || BV_RULE.ALL)(s.bv)), [filters])

  /* Column header filters narrow the feature rows, and the tiles and the
     chart follow: they are built from the stories of the features left. */
  const allRows = useMemo(() => featureRows(slice), [slice])
  const rows = useMemo(() => applyColumnFilters(allRows, colFilters, COLUMNS)
    .sort((a, b) => compare(a, b, sort.key, sort.dir)), [allRows, colFilters, sort])
  const chartSlice = useMemo(() => {
    if (!anyActive(colFilters)) return slice
    const keep = new Set(rows.map((r) => r.key))
    return slice.filter((s) => keep.has(s.featureId))
  }, [slice, rows, colFilters])
  const points = useMemo(() => burnup(chartSlice, PI_WINDOWS[filters.pi] || { snapshots: [], start: 0 }), [chartSlice, filters.pi])

  return (
    <>
      <div className="bg-blobs" aria-hidden="true"><span className="a" /><span className="b" /><span className="c" /></div>
      <div className="app">
        <Header pi={filters.pi} theme={theme} onToggleTheme={() => setTheme(theme === 'light' ? 'dark' : 'light')} />
        <FilterBar filters={filters} onChange={setFilter} />

        {tab === TABS[0] ? (
          <>
            <KpiRow rows={rows} points={points} />
            <div className="main">
              <FeatureTable rows={rows} allRows={allRows} sort={sort} onSort={onSort} pi={filters.pi}
                            colFilters={colFilters} onColFilter={setColFilter}
                            onClearColFilters={() => setColFilters({})} />
              <div className="stack">
                <BurnupChart points={points} showIdeal={filters.idealBurnup !== 'Hide'} pi={filters.pi} />
                <AnalysisPanel initialNotes={analysisNotes} />
              </div>
            </div>
          </>
        ) : (
          <section className="glass placeholder" role="tabpanel">
            <strong>{tab}</strong>
            <span>This sheet is not part of the mockup yet. The Program Feature Progress tab shows the full treatment.</span>
          </section>
        )}

        <TabStrip active={tab} onSelect={setTab} />
        <footer className="footer">Mockup · sample story rows in the shape of STORIES.hyper, aggregated into the feature table and burn-up · glass theme</footer>
      </div>
    </>
  )
}
