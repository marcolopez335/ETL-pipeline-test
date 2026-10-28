import { fmtNum, fmtPct } from '../lib/format.js'

const isDone = (s) => s === 'Done' || s === 'Accepted'

/* Four stat tiles, all computed from the same filtered slice as the table
   and the chart so the numbers agree. */
export default function KpiRow({ rows, points }) {
  const total = rows.length
  const done = rows.filter((r) => isDone(r.status)).length
  const inProgress = rows.filter((r) => r.status === 'In Progress').length
  const last = points[points.length - 1] || {}
  const toPlan = last.ideal ? (last.done / last.ideal) * 100 : null
  const share = last.scope ? (last.done / last.scope) * 100 : 0

  return (
    <section className="kpis" aria-label="Headline numbers">
      <div className="glass tile">
        <span className="label">Features in PI</span>
        <span className="value">{total}</span>
        <span className="sub">{done} done · {inProgress} in progress</span>
      </div>
      <div className="glass tile">
        <span className="label">Features done</span>
        <span className="value">{total ? fmtPct((done / total) * 100) : '–'}</span>
        <span className="sub">Done or Accepted</span>
      </div>
      <div className="glass tile">
        <span className="label">Story points done</span>
        <span className="value">{fmtNum(last.done)}<small>/ {fmtNum(last.scope)}</small></span>
        <span className="sub">
          <span className="meter" style={{ minWidth: 0, flex: 1 }}>
            <span className="track"><span className="fill pi" style={{ width: `${Math.min(100, share)}%`, display: 'block' }} /></span>
            <span className="pct">{fmtPct(share)}</span>
          </span>
        </span>
      </div>
      <div className="glass tile">
        <span className="label">Performance to plan</span>
        <span className="value">{toPlan == null ? '–' : fmtPct(toPlan)}</span>
        <span className="sub">
          {toPlan == null ? 'No ideal line yet' : (
            <><span className={toPlan >= 100 ? 'delta-up' : 'delta-down'} aria-hidden="true">{toPlan >= 100 ? '▲' : '▼'}</span>
              {toPlan >= 100 ? 'ahead of' : 'behind'} the ideal burn-up</>
          )}
        </span>
      </div>
    </section>
  )
}
