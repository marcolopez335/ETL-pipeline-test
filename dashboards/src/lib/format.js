const num = new Intl.NumberFormat('en-US', { maximumFractionDigits: 1 })
const int = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 })

export const fmtNum = (v) => (v == null ? '' : num.format(v))
export const fmtInt = (v) => (v == null ? '' : int.format(v))
export const fmtPct = (v) => (v == null ? '' : `${Math.round(v)}%`)

/* Axis ticks: compact from 1,000 up (0 / 1K / 2K). */
export const fmtCompact = (v) => (Math.abs(v) >= 1000 ? `${Math.round(v / 100) / 10}K` : int.format(v))

const parse = (iso) => {
  const [y, m, d] = iso.split('-').map(Number)
  return new Date(y, m - 1, d)
}
export const toDate = parse

/* 4/15/26 like the production sheet. */
export const fmtDate = (iso) => {
  if (!iso) return ''
  const d = parse(iso)
  return `${d.getMonth() + 1}/${d.getDate()}/${String(d.getFullYear()).slice(-2)}`
}
export const fmtMonthDay = (iso) => {
  const d = parse(iso)
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
}
export const fmtLong = (iso) => parse(iso).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
