import { useEffect, useMemo, useRef, useState } from 'react'
import { areaPath, linePath, linear, monthTicks, niceTicks } from '../lib/chart.js'
import { fmtCompact, fmtLong, fmtNum, toDate } from '../lib/format.js'

/* Color follows the entity: Ideal is always slot 1, Scope slot 2, Done slot 3,
   whatever is hidden. Legend order matches the production sheet. */
const SERIES = [
  { key: 'ideal', label: 'Ideal', color: 'var(--series-1)' },
  { key: 'done', label: 'Done', color: 'var(--series-3)' },
  { key: 'scope', label: 'Scope', color: 'var(--series-2)' },
]
const M = { top: 16, right: 64, bottom: 30, left: 44 }
const HEIGHT = 300
const LABEL_GAP = 14

export default function BurnupChart({ points, showIdeal, pi }) {
  const wrapRef = useRef(null)
  const [width, setWidth] = useState(640)
  const [hover, setHover] = useState(null)
  const [hidden, setHidden] = useState(() => new Set())
  const [view, setView] = useState('chart')

  useEffect(() => {
    const el = wrapRef.current
    if (!el || typeof ResizeObserver === 'undefined') return undefined
    const ro = new ResizeObserver(([entry]) => setWidth(Math.max(280, Math.floor(entry.contentRect.width))))
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  const series = useMemo(() => SERIES.filter((s) => showIdeal || s.key !== 'ideal'), [showIdeal])
  const visible = series.filter((s) => !hidden.has(s.key))
  const n = points.length

  const { x, y, ticks } = useMemo(() => {
    const max = Math.max(1, ...points.flatMap((p) => visible.map((s) => p[s.key] ?? 0)))
    const t = niceTicks(max)
    return {
      x: linear([0, Math.max(1, n - 1)], [M.left, width - M.right]),
      y: linear([0, t[t.length - 1]], [HEIGHT - M.bottom, M.top]),
      ticks: t,
    }
  }, [points, visible, width, n])

  const lastIdx = (key) => {
    for (let i = n - 1; i >= 0; i -= 1) if (points[i][key] != null) return i
    return -1
  }

  /* Direct labels on Done and Scope only (Ideal ends where Scope does);
     if the two collide, push them apart and draw a leader. */
  const endLabels = useMemo(() => {
    const labels = ['done', 'scope']
      .filter((k) => visible.some((s) => s.key === k))
      .map((k) => ({ key: k, i: lastIdx(k) }))
      .filter((l) => l.i >= 0)
      .map((l) => ({ ...l, value: points[l.i][l.key], yMark: y(points[l.i][l.key]) }))
      .sort((a, b) => a.yMark - b.yMark)
    labels.forEach((l) => { l.yLabel = l.yMark })
    for (let i = 1; i < labels.length; i += 1) {
      if (labels[i].yLabel - labels[i - 1].yLabel < LABEL_GAP) labels[i].yLabel = labels[i - 1].yLabel + LABEL_GAP
    }
    return labels
  }, [points, visible, y, n])

  const idxFromEvent = (e) => {
    const svg = e.currentTarget.ownerSVGElement || e.currentTarget
    const rect = svg.getBoundingClientRect()
    const px = ((e.clientX - rect.left) / rect.width) * width
    const step = (width - M.left - M.right) / Math.max(1, n - 1)
    return Math.max(0, Math.min(n - 1, Math.round((px - M.left) / step)))
  }
  const onKey = (e) => {
    const cur = hover ?? n - 1
    const map = { ArrowLeft: cur - 1, ArrowRight: cur + 1, Home: 0, End: n - 1 }
    if (e.key === 'Escape') { setHover(null); return }
    if (map[e.key] == null) return
    e.preventDefault()
    setHover(Math.max(0, Math.min(n - 1, map[e.key])))
  }
  const toggle = (key) => setHidden((h) => {
    const next = new Set(h)
    if (next.has(key)) next.delete(key); else next.add(key)
    return next
  })

  const months = monthTicks(points.map((p) => p.date))
  const hp = hover == null ? null : points[hover]
  const tipLeft = hover == null ? 0 : (x(hover) > width - 190 ? x(hover) - 178 : x(hover) + 12)

  return (
    <section className="glass card" aria-labelledby="burnup-h">
      <div className="card-head">
        <h2 id="burnup-h">Program Burnup (PI)</h2>
        <div className="card-tools">
          <span className="meta">Story points · PI {pi}</span>
          <button type="button" className="btn small" aria-pressed={view === 'table'}
                  onClick={() => setView(view === 'chart' ? 'table' : 'chart')}>
            {view === 'chart' ? 'Table view' : 'Chart view'}
          </button>
        </div>
      </div>

      {view === 'table' ? (
        <div className="data-wrap">
          <table className="data">
            <thead><tr><th scope="col">Week of</th>{series.map((s) => <th key={s.key} scope="col">{s.label}</th>)}</tr></thead>
            <tbody>
              {points.map((p) => (
                <tr key={p.date}><td>{fmtLong(p.date)}</td>{series.map((s) => <td key={s.key}>{p[s.key] == null ? '–' : fmtNum(p[s.key])}</td>)}</tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="chart-wrap" ref={wrapRef}>
          <svg width={width} height={HEIGHT} viewBox={`0 0 ${width} ${HEIGHT}`} role="img"
               aria-label={`Burn-up of story points for PI ${pi}: ${visible.map((s) => s.label).join(', ')}`}
               tabIndex={0} onKeyDown={onKey} onBlur={() => setHover(null)}>
            {ticks.map((t) => (
              <g key={t}>
                <line className="gridline" x1={M.left} x2={width - M.right} y1={y(t)} y2={y(t)} />
                <text className="tick" x={M.left - 8} y={y(t) + 4} textAnchor="end">{fmtCompact(t)}</text>
              </g>
            ))}
            <line className="axis-line" x1={M.left} x2={width - M.right} y1={y(0)} y2={y(0)} />
            {months.map((i) => (
              <text key={i} className="tick" x={x(i)} y={HEIGHT - 10} textAnchor="middle">
                {toDate(points[i].date).toLocaleDateString('en-US', { month: 'short' })}
              </text>
            ))}

            {visible.some((s) => s.key === 'done') && (
              <path className="series-area" d={areaPath(points, x, y, 'done', y(0))} fill="var(--series-3)" />
            )}
            {visible.map((s) => (
              <path key={s.key} className="series-line" d={linePath(points, x, y, s.key)} stroke={s.color} />
            ))}
            {visible.map((s) => {
              const i = lastIdx(s.key)
              return i < 0 ? null : <circle key={s.key} className="marker" cx={x(i)} cy={y(points[i][s.key])} r={4} fill={s.color} />
            })}
            {endLabels.map((l) => (
              <g key={l.key}>
                {Math.abs(l.yLabel - l.yMark) > 1 && (
                  <line className="axis-line" x1={x(l.i) + 5} y1={l.yMark} x2={x(l.i) + 9} y2={l.yLabel} />
                )}
                <text className="end-label" x={x(l.i) + 10} y={l.yLabel + 4}>{fmtNum(l.value)}</text>
              </g>
            ))}

            {hp && (
              <g>
                <line className="crosshair" x1={x(hover)} x2={x(hover)} y1={M.top} y2={y(0)} />
                {visible.map((s) => hp[s.key] == null ? null : (
                  <circle key={s.key} className="marker" cx={x(hover)} cy={y(hp[s.key])} r={4} fill={s.color} />
                ))}
              </g>
            )}
            <rect x={M.left} y={M.top} width={Math.max(0, width - M.left - M.right)} height={Math.max(0, HEIGHT - M.top - M.bottom)}
                  fill="transparent" onPointerMove={(e) => setHover(idxFromEvent(e))} onPointerLeave={() => setHover(null)} />
          </svg>
          {hp && (
            <div className="tooltip" style={{ left: tipLeft, top: M.top }} role="status">
              <div className="date">{fmtLong(hp.date)}</div>
              {visible.map((s) => (
                <div className="row" key={s.key}>
                  <span className="key" style={{ background: s.color }} />
                  <span>{s.label}</span>
                  <span className="val">{hp[s.key] == null ? '–' : fmtNum(hp[s.key])}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      <div className="legend" aria-label="Series">
        {series.map((s) => (
          <button type="button" key={s.key} aria-pressed={!hidden.has(s.key)} onClick={() => toggle(s.key)}
                  title={`Show or hide ${s.label}`}>
            <span className="key" style={{ background: s.color }} aria-hidden="true" />{s.label}
          </button>
        ))}
      </div>
    </section>
  )
}
