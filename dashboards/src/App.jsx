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

  useEffect(() => {
    document.documentElement.dataset.theme = theme
    try { localStorage.setItem('pm-theme', theme) } catch { /* private window: theme just won't persist */ }
  }, [theme])

  const setFilter = (id, value) => {
    setFilters((f) => ({ ...f, [id]: value }))
    if (id === 'sortBy') setSort({ key: SORT_KEY[value] || 'end', dir: 'asc' })
  }
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

  const rows = useMemo(() => featureRows(slice).sort((a, b) => compare(a, b, sort.key, sort.dir)), [slice, sort])
  const points = useMemo(() => burnup(slice, PI_WINDOWS[filters.pi] || { snapshots: [], start: 0 }), [slice, filters.pi])

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
              <FeatureTable rows={rows} sort={sort} onSort={onSort} pi={filters.pi} />
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
