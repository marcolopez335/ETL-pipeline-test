import { useEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'

const WIDTH = 270
const show = (v) => (v === '' ? '(blank)' : v)

/* A filter-bar dropdown with checkboxes. The choice is staged while the
   list is open and only takes effect on Apply; Cancel, Escape or a click
   outside discards it. `value` is null for "all" or an array of the
   selected values ('' stands for blank). */
export default function MultiSelect({ id, label, options, value, onApply }) {
  const btnRef = useRef(null)
  const popRef = useRef(null)
  const [open, setOpen] = useState(false)
  const [staged, setStaged] = useState(() => new Set())
  const [query, setQuery] = useState('')

  const all = value == null
  const summary = all ? '(All)'
    : value.length === 0 ? '(None)'
      : value.length === 1 ? show(value[0])
        : `${value.length} of ${options.length}`

  const openIt = () => { setStaged(new Set(value ?? options)); setQuery(''); setOpen(true) }
  const close = () => setOpen(false)
  const apply = () => {
    const next = options.filter((o) => staged.has(o))
    onApply(next.length === options.length ? null : next)
    setOpen(false)
  }
  const toggle = (o) => setStaged((s) => {
    const n = new Set(s)
    if (n.has(o)) n.delete(o); else n.add(o)
    return n
  })

  useEffect(() => {
    if (!open) return undefined
    const onDown = (e) => {
      if (popRef.current && !popRef.current.contains(e.target) && !btnRef.current.contains(e.target)) close()
    }
    const onKey = (e) => { if (e.key === 'Escape') close() }
    document.addEventListener('pointerdown', onDown)
    document.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('pointerdown', onDown)
      document.removeEventListener('keydown', onKey)
    }
  }, [open])

  const rect = open && btnRef.current ? btnRef.current.getBoundingClientRect() : null
  const left = rect ? Math.max(8, Math.min(rect.left, window.innerWidth - WIDTH - 8)) : 0
  const q = query.trim().toLowerCase()
  const shown = q ? options.filter((o) => show(o).toLowerCase().includes(q)) : options

  return (
    <div className="filter">
      <label htmlFor={id}>{label}</label>
      <div className="select">
        <button type="button" id={id} ref={btnRef} className={`select-btn${all ? '' : ' active'}`}
                aria-haspopup="dialog" aria-expanded={open} onClick={open ? close : openIt}
                title={all ? 'All' : value.map(show).join(', ')}>
          {summary}
        </button>
      </div>
      {open && rect && createPortal(
        <div ref={popRef} className="glass strong popover" role="dialog" aria-label={`${label} filter`}
             style={{ left, top: rect.bottom + 6, width: WIDTH }}>
          <div className="head">{label}</div>
          {options.length > 8 && (
            <input type="search" placeholder="Find..." value={query} aria-label={`Find in ${label}`}
                   onChange={(e) => setQuery(e.target.value)} autoFocus />
          )}
          <div className="quick">
            <button type="button" onClick={() => setStaged(new Set(options))}>All</button>
            <button type="button" onClick={() => setStaged(new Set())}>None</button>
            <span className="count">{staged.size} of {options.length} selected</span>
          </div>
          <div className="list">
            {shown.length === 0 && <span className="muted">No matches</span>}
            {shown.map((o) => (
              <label key={show(o)}>
                <input type="checkbox" checked={staged.has(o)} onChange={() => toggle(o)} />
                <span>{show(o)}</span>
              </label>
            ))}
          </div>
          <div className="foot">
            <button type="button" className="btn small" onClick={close}>Cancel</button>
            <button type="button" className="btn small primary" onClick={apply}>Apply</button>
          </div>
        </div>,
        document.body,
      )}
    </div>
  )
}
