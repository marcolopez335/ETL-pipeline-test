/* The two views the sheet shows, built from story rows the way the
   production Tableau sheet builds them from the stories datasource. */
const isDone = (status) => status === 'Done' || status === 'Accepted'
const pct = (num, den) => (den ? (num / den) * 100 : null)

/* Feature Progress: one row per Feature Id. Owner, BV, Start, End and Status
   come from the feature lookup columns on the story rows; Size is the
   number of stories; PI % is done points over total points for stories
   tagged with the feature's own PI; Total % is done over total for every
   story under the feature. */
export function featureRows(stories) {
  const byFeature = new Map()
  for (const s of stories) {
    let f = byFeature.get(s.featureId)
    if (!f) {
      f = {
        key: s.featureId, summary: s.feature, owner: s.featureTeam, bv: s.bv,
        start: s.targetStart, end: s.targetEnd, status: s.featureStatus,
        pi: s.featurePi, release: s.featureFixVersion,
        size: 0, total: 0, done: 0, piTotal: 0, piDone: 0,
      }
      byFeature.set(s.featureId, f)
    }
    const done = isDone(s.status) ? s.estimate : 0
    f.size += 1
    f.total += s.estimate
    f.done += done
    if (s.programIncrement === `PI ${s.featurePi}`) {
      f.piTotal += s.estimate
      f.piDone += done
    }
  }
  return [...byFeature.values()].map((f) => ({
    ...f, piPct: pct(f.piDone, f.piTotal), totalPct: pct(f.done, f.total),
  }))
}

/* Program burn-up: per weekly Snapshot Date, Scope is the sum of Estimate
   over the stories present in that snapshot, Done the same sum over the
   ones whose Status is Done or Accepted by then, and Ideal a straight line
   from the PI's first snapshot to the final scope. */
export function burnup(stories, window) {
  const { snapshots, start } = window
  const n = snapshots.length
  const points = snapshots.map((date) => ({ date, scope: 0, done: 0, ideal: null }))
  for (const s of stories) {
    for (let i = Math.max(0, s.addedWeek); i < n; i += 1) points[i].scope += s.estimate
    if (s.doneWeek != null) for (let i = Math.max(0, s.doneWeek); i < n; i += 1) points[i].done += s.estimate
  }
  const finalScope = n ? points[n - 1].scope : 0
  const span = Math.max(1, n - 1 - start)
  points.forEach((p, i) => {
    p.ideal = i < start ? null : Math.round((finalScope * (i - start)) / span * 10) / 10
  })
  return points
}
