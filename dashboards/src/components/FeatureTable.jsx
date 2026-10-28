import { fmtDate, fmtPct } from '../lib/format.js'

const COLUMNS = [
  { key: 'key', label: 'Feature' },
  { key: 'owner', label: 'Owner' },
  { key: 'bv', label: 'BV', num: true },
  { key: 'start', label: 'Start' },
  { key: 'end', label: 'End' },
  { key: 'size', label: 'Size', num: true },
  { key: 'status', label: 'Status' },
  { key: 'piPct', label: 'PI' },
  { key: 'totalPct', label: 'Total' },
]

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

export default function FeatureTable({ rows, sort, onSort, pi }) {
  return (
    <section className="glass card" aria-labelledby="features-h">
      <div className="card-head">
        <h2 id="features-h">Feature Progress (PI)</h2>
        <span className="meta">{rows.length} features · PI {pi} · click a header to sort</span>
      </div>
      <div className="table-wrap">
        <table className="features">
          <thead>
            <tr>
              {COLUMNS.map((c) => {
                const active = sort.key === c.key
                return (
                  <th key={c.key} className={c.num ? 'num' : c.key === 'key' ? 'feature' : ''} scope="col"
                      aria-sort={active ? (sort.dir === 'asc' ? 'ascending' : 'descending') : 'none'}
                      onClick={() => onSort(c.key)}>
                    {c.label}<span className="sort" aria-hidden="true">{active ? (sort.dir === 'asc' ? '▲' : '▼') : '↕'}</span>
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
                  <span className="key">{r.key}</span><span className="summary">- {r.summary}</span>
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
    </section>
  )
}
