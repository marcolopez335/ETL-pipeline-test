/* Tiny scale and path helpers so the chart needs no charting library. */
export const linear = ([d0, d1], [r0, r1]) => {
  const span = d1 - d0 || 1
  return (v) => r0 + ((v - d0) / span) * (r1 - r0)
}

/* Clean y ticks: 0 .. a rounded max, in steps of 1/2/5 x 10^n. */
export const niceTicks = (max, count = 4) => {
  if (!(max > 0)) return [0, 1]
  const rough = max / count
  const pow = 10 ** Math.floor(Math.log10(rough))
  const step = [1, 2, 5, 10].map((m) => m * pow).find((s) => s >= rough)
  const ticks = []
  for (let v = 0; v <= max + step * 0.999; v += step) ticks.push(v)
  return ticks
}

export const linePath = (pts, x, y, key) => {
  let d = ''
  pts.forEach((p, i) => {
    if (p[key] == null) return
    d += `${d ? 'L' : 'M'}${x(i).toFixed(1)},${y(p[key]).toFixed(1)}`
  })
  return d
}

export const areaPath = (pts, x, y, key, baseline) => {
  const line = linePath(pts, x, y, key)
  if (!line) return ''
  const idx = pts.map((p, i) => (p[key] == null ? null : i)).filter((i) => i != null)
  const first = idx[0]
  const last = idx[idx.length - 1]
  return `${line}L${x(last).toFixed(1)},${baseline.toFixed(1)}L${x(first).toFixed(1)},${baseline.toFixed(1)}Z`
}

/* First-of-month tick positions inside a list of ISO dates. */
export const monthTicks = (dates) => {
  const ticks = []
  let seen = ''
  dates.forEach((iso, i) => {
    const ym = iso.slice(0, 7)
    if (ym !== seen) {
      seen = ym
      if (i > 0 || iso.endsWith('-01')) ticks.push(i)
    }
  })
  return ticks
}
