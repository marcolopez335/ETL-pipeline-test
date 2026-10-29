/* Column header filters. A filter value is one of:
   - text:   a string (contains, case-insensitive)
   - values: an array of the selected values as strings ('' = blank)
   - range:  { min, max } with either side null
   null / undefined means "no filter on this column". */
const blank = (v) => v == null || v === ''
export const asValue = (v) => (blank(v) ? '' : String(v))

export function isActive(f) {
  if (f == null) return false
  if (typeof f === 'string') return f.trim() !== ''
  if (Array.isArray(f)) return true
  return f.min != null || f.max != null
}

export const anyActive = (filters) => Object.values(filters).some(isActive)

/* Distinct values of a column, numbers in numeric order, blank last. */
export function distinctValues(rows, key) {
  const set = new Set(rows.map((r) => asValue(r[key])))
  return [...set].sort((a, b) => {
    if (a === '') return 1
    if (b === '') return -1
    const na = Number(a), nb = Number(b)
    return Number.isNaN(na) || Number.isNaN(nb) ? a.localeCompare(b) : na - nb
  })
}

export function applyColumnFilters(rows, filters, columns) {
  return rows.filter((r) => columns.every((c) => {
    const f = filters[c.key]
    if (!isActive(f)) return true
    const v = r[c.key]
    if (c.filter === 'text') return `${r.key} ${r.summary}`.toLowerCase().includes(f.trim().toLowerCase())
    if (c.filter === 'values') return f.includes(asValue(v))
    if (blank(v)) return false
    if (c.type === 'date') return (f.min == null || v >= f.min) && (f.max == null || v <= f.max)
    const n = Number(v)
    return (f.min == null || n >= Number(f.min)) && (f.max == null || n <= Number(f.max))
  }))
}
