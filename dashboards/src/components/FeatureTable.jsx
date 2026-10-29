import { useState } from 'react'
import HeaderFilter from './HeaderFilter.jsx'
import { COLUMNS } from '../lib/columns.js'
import { anyActive, distinctValues, isActive } from '../lib/filters.js'
import { fmtDate, fmtPct } from '../lib/format.js'

/* Status wears a fixed status color plus an icon and the label, never color alone. */
const STATUS = {
  Done: ['done', '✓'],
  Accepted: ['done', '✓'],
  'In Progress': ['progress', '◔'],
  'To Do': ['todo', '○'],
}

function Pill({ status }) {
  const [cls, ico] = STATUS[status] || ['todo', '○']
  return <span className={`pill ${cls}`}><span className="ico" aria-hidden="true">{ico}</span>{status}</span>
}

function Meter({ pct, kind }) {
  if (pct == null) return <span className="pct" style={{ color: 'var(--text-3)' }}>–</span>
  return (
    <div className="meter" role="img" aria-label={`${fmtPct(pct)} ${kind === 'pi' ? 'in this PI' : 'overall'}`}>
      <div className="track"><div className={`fill ${kind}`} style={{ width: `${Math.min(100, pct)}%` }} /></div>
      <span className="pct">{fmtPct(pct)}</span>
    </div>
  )
}

const Funnel = () => (
  <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true">
    <path d="M1 1.5h10L7.2 6.4V10l-2.4 1V6.4Z" fill="currentColor" />
  </svg>
)

export default function FeatureTable({ rows, allRows, sort, onSort, pi, colFilters, onColFilter, onClearColFilters }) {
  const [open, setOpen] = useState(null)      // { key, anchor } of the open header filter
  const filtered = anyActive(colFilters)
  const openCol = open && COLUMNS.find((c) => c.key === open.key)

  return (
    <section className="glass card" aria-labelledby="features-h">
      <div className="card-head">
        <h2 id="features-h">Feature Progress (PI)</h2>
        <div className="card-tools">
          <span className="meta">
            {filtered ? `${rows.length} of ${allRows.length}` : rows.length} features · PI {pi} · sort by the name, filter with the funnel
          </span>
          {filtered && (
            <button type="button" className="btn small" onClick={onClearColFilters}>Clear column filters</button>
          )}
        </div>
      </div>
      <div className="table-wrap">
        <table className="features">
          <thead>
            <tr>
              {COLUMNS.map((c) => {
                const active = sort.key === c.key
                const cls = c.num ? 'num' : c.key === 'key' ? 'feature' : ''
                return (
                  <th key={c.key} className={cls} scope="col"
                      aria-sort={active ? (sort.dir === 'asc' ? 'ascending' : 'descending') : 'none'}>
                    <div className="th-wrap">
                      <button type="button" className="th-sort" onClick={() => onSort(c.key)}
                              title={`Sort by ${c.label}`}>
                        {c.label}<span className="sort" aria-hidden="true">{active ? (sort.dir === 'asc' ? '▲' : '▼') : '↕'}</span>
                      </button>
                      <button type="button" className="th-filter" aria-label={`Filter ${c.label}`}
                              aria-pressed={isActive(colFilters[c.key])} aria-expanded={open?.key === c.key}
                              onClick={(e) => setOpen(open?.key === c.key ? null : { key: c.key, anchor: e.currentTarget })}>
                        <Funnel />
                      </button>
                    </div>
                  </th>
                )
              })}
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 && (
              <tr><td colSpan={COLUMNS.length} className="empty">No features match the current filters.</td></tr>
            )}
            {rows.map((r) => (
              <tr key={r.key}>
                <td className="feature" title={`${r.key} - ${r.summary}`}>
                  <div className="feature-text">
                    <span className="key">{r.key}</span><span className="summary">- {r.summary}</span>
                  </div>
                </td>
                <td className="owner">{r.owner || ''}</td>
                <td className="num">{r.bv ?? ''}</td>
                <td>{fmtDate(r.start)}</td>
                <td>{fmtDate(r.end)}</td>
                <td className="num">{r.size ?? ''}</td>
                <td><Pill status={r.status} /></td>
                <td><Meter pct={r.piPct} kind="pi" /></td>
                <td><Meter pct={r.totalPct} kind="total" /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {openCol && (
        <HeaderFilter column={openCol} anchor={open.anchor} value={colFilters[openCol.key]}
                      options={openCol.filter === 'values' ? distinctValues(allRows, openCol.key) : []}
                      onChange={(v) => onColFilter(openCol.key, v)} onClose={() => setOpen(null)} />
      )}
    </section>
  )
}
