import { useEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'

/* On-screen report behind the header's menu button: enough to tell, from a
   photo of the screen, whether the color tokens, stylesheets, chart and
   dataset loaded. */
const collect = ({ status, dataset, pi }) => {
  const root = document.documentElement
  const cs = getComputedStyle(root)
  const sheets = [...document.styleSheets].map((s) => {
    const node = s.ownerNode
    const id = node?.getAttribute?.('data-vite-dev-id') || node?.id || s.href || ''
    return id.split(/[\\/]/).pop() || 'inline'
  })
  const glass = document.querySelector('.glass')
  const gcs = glass ? getComputedStyle(glass) : null
  const svg = document.querySelector('.chart-wrap svg')
  const tick = svg?.querySelector('.tick')
  let stored = 'n/a'
  try { stored = localStorage.getItem('pm-theme') ?? '(none)' } catch { stored = 'blocked' }
  return [
    `browser: ${navigator.userAgent.replace(/^.*\) /, '')}`,
    `tokens on <html>: --glass-bg = "${cs.getPropertyValue('--glass-bg').trim() || 'MISSING'}", --series-3 = "${cs.getPropertyValue('--series-3').trim() || 'MISSING'}"`,
    `token fallback injected: ${document.getElementById('tokens-fallback') ? 'yes' : 'no'}`,
    `theme: data-theme=${root.dataset.theme || '(unset)'}, stored=${stored}, color-scheme=${cs.colorScheme}`,
    `stylesheets (${sheets.length}): ${sheets.join(', ')}`,
    `glass panel: background=${gcs?.backgroundColor || 'n/a'}, backdrop=${gcs?.backdropFilter || gcs?.webkitBackdropFilter || 'none'}`,
    `chart svg: ${svg ? `${svg.getAttribute('width')}x${svg.getAttribute('height')}, ${svg.querySelectorAll('.series-line').length} lines, tick fill=${tick ? getComputedStyle(tick).fill : 'none'}` : 'not rendered'}`,
    `backdrop-filter supported: ${CSS.supports('backdrop-filter', 'blur(2px)')}, dark reader: ${root.getAttribute('data-darkreader-mode') || 'none'}, forced colors: ${matchMedia('(forced-colors: active)').matches}`,
    `dataset: ${status}, ${dataset.stories.length} stories, ${Object.keys(dataset.pis).length} PI(s)${dataset.generated ? `, generated ${dataset.generated}` : ''}`,
    ...dataLines(dataset, pi),
  ]
}

/* The selected PI's window and its stories, enough to see why a burn-up
   or a KPI would come out empty with real data. */
const dataLines = (dataset, pi) => {
  const w = dataset.pis[pi]
  const rows = dataset.stories.filter((s) => s.featurePi === pi)
  const n = rows.length
  const withEst = rows.filter((s) => Number(s.estimate) > 0).length
  const seen = rows.map((s) => s.firstSeen).filter(Boolean).sort()
  const done = rows.filter((s) => s.doneOn).length
  const sample = rows[0] ? JSON.stringify(rows[0]).slice(0, 260) : 'none'
  return [
    `PI ${pi} window: ${w ? `${w.snapshots.length} snapshots ${w.snapshots[0]} .. ${w.snapshots[w.snapshots.length - 1]}, start ${w.start}, end ${w.end}` : 'MISSING'}`,
    `PI ${pi} stories: ${n}; with estimate > 0: ${withEst}; with firstSeen: ${seen.length} (${seen[0] || '-'} .. ${seen[seen.length - 1] || '-'}); with doneOn: ${done}`,
    `first story record: ${sample}`,
  ]
}

export default function Diagnostics({ anchor, status, dataset, pi, onClose }) {
  const ref = useRef(null)
  const [lines, setLines] = useState(() => collect({ status, dataset, pi }))

  useEffect(() => { setLines(collect({ status, dataset, pi })) }, [status, dataset, pi])
  useEffect(() => {
    const onDown = (e) => { if (ref.current && !ref.current.contains(e.target) && !anchor.contains(e.target)) onClose() }
    const onKey = (e) => { if (e.key === 'Escape') onClose() }
    document.addEventListener('pointerdown', onDown)
    document.addEventListener('keydown', onKey)
    return () => { document.removeEventListener('pointerdown', onDown); document.removeEventListener('keydown', onKey) }
  }, [anchor, onClose])

  const rect = anchor.getBoundingClientRect()
  return createPortal(
    <div ref={ref} className="glass strong popover diagnostics" role="dialog" aria-label="Diagnostics"
         style={{ top: rect.bottom + 8, right: Math.max(8, window.innerWidth - rect.right), width: 'min(640px, calc(100vw - 16px))' }}>
      <div className="head">Diagnostics</div>
      <ol>{lines.map((l, i) => <li key={i}>{l}</li>)}</ol>
      <div className="foot">
        <button type="button" className="btn small" onClick={() => setLines(collect({ status, dataset, pi }))}>Refresh</button>
        <button type="button" className="btn small" onClick={onClose}>Close</button>
      </div>
    </div>,
    document.body,
  )
}
