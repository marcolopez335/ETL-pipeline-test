/* The two views the sheet shows, built from story rows the way the
   production Tableau sheet builds them from the stories datasource. */
const isDone = (status) => status === 'Done' || status === 'Accepted'
const pct = (num, den) => (den ? (num / den) * 100 : null)
const points = (s) => Number(s.estimate) || 0

/* "PI 26.3", "26.3" and "PI_26.3" all mean the same PI. */
export const piNumber = (v) => (String(v ?? '').match(/\d{2}\.\d/) || [null])[0]

/* Feature Progress: one row per Feature Id. Owner, BV, Start, End and Status
   come from the feature lookup columns on the story rows; Size is the
   number of stories; PI % is done points over total points for stories
   tagged with the feature's own PI; Total % is done over total for every
   story under the feature. */
export function featureRows(stories) {
  const byFeature = new Map()
  for (const s of stories) {
    const id = s.featureId || '(no feature)'
    let f = byFeature.get(id)
    if (!f) {
      f = {
        key: id, summary: s.feature || '', owner: s.featureTeam, bv: s.bv,
        start: s.targetStart, end: s.targetEnd, status: s.featureStatus,
        pi: s.featurePi, release: s.featureFixVersion,
        size: 0, total: 0, done: 0, piTotal: 0, piDone: 0,
      }
      byFeature.set(id, f)
    }
    const est = points(s)
    const done = isDone(s.status) ? est : 0
    f.size += 1
    f.total += est
    f.done += done
    if (piNumber(s.programIncrement) === piNumber(s.featurePi)) {
      f.piTotal += est
      f.piDone += done
    }
  }
  return [...byFeature.values()].map((f) => ({
    ...f, piPct: pct(f.piDone, f.piTotal), totalPct: pct(f.done, f.total),
  }))
}

/* Program burn-up over a PI window ({ snapshots, start, end }, ISO dates):
   per snapshot, Scope is the sum of Estimate over the stories first seen by
   then, Done the same sum over the ones done by then, and Ideal a straight
   line from the PI's start date to its end date reaching the final scope. */
export function burnup(stories, window) {
  const { snapshots = [], start, end } = window || {}
  const out = snapshots.map((date) => ({ date, scope: 0, done: 0, ideal: null }))
  for (const s of stories) {
    const est = points(s)
    if (!est) continue
    for (const p of out) {
      if (s.firstSeen && s.firstSeen <= p.date) p.scope += est
      if (s.doneOn && s.doneOn <= p.date) p.done += est
    }
  }
  const finalScope = out.length ? out[out.length - 1].scope : 0
  const t0 = Date.parse(start || snapshots[0])
  const t1 = Date.parse(end || snapshots[snapshots.length - 1])
  const span = Math.max(1, t1 - t0)
  for (const p of out) {
    const t = Date.parse(p.date)
    p.ideal = t < t0 ? null : Math.round(Math.min(1, (t - t0) / span) * finalScope * 10) / 10
  }
  return out
}
