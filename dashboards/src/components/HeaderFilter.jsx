import { useEffect, useRef } from 'react'
import { createPortal } from 'react-dom'

const WIDTH = 250

/* The popover behind a column header's funnel button. Rendered into
   document.body with fixed positioning so the table's scroll container and
   the glass card's backdrop filter cannot clip it. */
export default function HeaderFilter({ column, anchor, value, options, onChange, onClose }) {
  const ref = useRef(null)

  useEffect(() => {
    const onDown = (e) => {
      if (ref.current && !ref.current.contains(e.target) && !anchor.contains(e.target)) onClose()
    }
    const onKey = (e) => { if (e.key === 'Escape') onClose() }
    document.addEventListener('pointerdown', onDown)
    document.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('pointerdown', onDown)
      document.removeEventListener('keydown', onKey)
    }
  }, [anchor, onClose])

  const rect = anchor.getBoundingClientRect()
  const left = Math.max(8, Math.min(rect.left, window.innerWidth - WIDTH - 8))
  const top = rect.bottom + 6

  let body = null
  if (column.filter === 'text') {
    body = (
      <input type="search" autoFocus value={value || ''} placeholder="Contains..."
             aria-label={`${column.label} contains`}
             onChange={(e) => onChange(e.target.value || null)} />
    )
  } else if (column.filter === 'values') {
    const selected = value == null ? null : new Set(value)
    const toggle = (v) => {
      const next = selected == null ? new Set(options) : new Set(selected)
      if (next.has(v)) next.delete(v); else next.add(v)
      onChange(next.size === options.length ? null : [...next])
    }
    body = (
      <>
        <div className="quick">
          <button type="button" onClick={() => onChange(null)}>All</button>
          <button type="button" onClick={() => onChange([])}>None</button>
        </div>
        <div className="list">
          {options.map((v) => (
            <label key={v || '(blank)'}>
              <input type="checkbox" checked={selected == null || selected.has(v)} onChange={() => toggle(v)} />
              <span>{v === '' ? '(blank)' : v}</span>
            </label>
          ))}
        </div>
      </>
    )
  } else {
    const inputType = column.type === 'date' ? 'date' : 'number'
    const extra = column.type === 'pct' ? { min: 0, max: 100 } : {}
    const set = (side, raw) => {
      const next = { ...(value || {}), [side]: raw === '' ? null : raw }
      onChange(next.min == null && next.max == null ? null : next)
    }
    body = (
      <div className="range">
        <label><span>From</span><input type={inputType} {...extra} value={value?.min ?? ''} onChange={(e) => set('min', e.target.value)} /></label>
        <label><span>To</span><input type={inputType} {...extra} value={value?.max ?? ''} onChange={(e) => set('max', e.target.value)} /></label>
      </div>
    )
  }

  return createPortal(
    <div ref={ref} className="glass strong popover" role="dialog" aria-label={`Filter ${column.label}`}
         style={{ left, top, width: WIDTH }}>
      <div className="head">{column.label}</div>
      {body}
      <div className="foot">
        <button type="button" className="btn small" onClick={() => onChange(null)}>Clear</button>
        <button type="button" className="btn small" onClick={onClose}>Done</button>
      </div>
    </div>,
    document.body,
  )
}
