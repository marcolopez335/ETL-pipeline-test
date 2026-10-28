/* Mock data in the shape of STORIES.hyper: one row per story, carrying the
   feature lookup columns the stories pipeline joins on (EsumEhist.sql).
   The Feature Progress table and the burn-up are NOT stored here -- they are
   aggregated from these rows in src/lib/aggregate.js, the way the production
   sheet is built from the stories datasource.

   Field -> STORIES.hyper column:
     storyNumber -> Story Number      storyName -> Story Name
     teamName -> Team Name            sprintName -> Sprint Name
     programIncrement -> Program Increment   fixVersion -> Fix Version
     status -> Status                 estimate -> Estimate (story points)
     closedDate -> Closed Date        featureId / feature -> Feature Id / Feature
     featureTeam -> Feature Team      featurePi -> Feature Pi
     featureFixVersion -> Feature Fix Version   featureStatus -> Feature Status
     bv -> Bv                         targetStart / targetEnd -> Target Start / Target End
   addedWeek / doneWeek stand in for the story's history snapshots: the index
   of the first weekly Snapshot Date the story appears in, and the first one
   where its Status is Done or Accepted. */

export const STAMPS = {
  extract: '10/26/2026 12:43:06 AM CT',
  load: '10/26/2026 11:02:48 AM CT',
}

export const PI_OPTIONS = ['26.2', '26.3', '26.4']
export const RELEASE_OPTIONS = ['(All)', 'CR3.1', 'CR3.2', 'CR3.3', '8.1.1', '8.1.2']
export const TEAMS = [
  '3H3T', 'Cyber Defenders', 'Guardians', 'Mega Maniacs', 'Navigators',
  'OMS Rangers', 'The Watchmen', 'Thunderbolts', 'Tiger Team',
]

const iso = (d) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
const weeklyDates = (startIso, weeks) => {
  const [y, m, d] = startIso.split('-').map(Number)
  return Array.from({ length: weeks }, (_, i) => {
    const dt = new Date(y, m - 1, d)
    dt.setDate(dt.getDate() + i * 7)
    return iso(dt)
  })
}

/* Weekly Snapshot Dates per PI, and the index where the PI itself starts
   (the ideal line runs from there to the last snapshot). PI 26.3 keeps the
   pre-PI weeks visible like the production chart. */
export const PI_WINDOWS = {
  '26.2': { snapshots: weeklyDates('2026-01-07', 13), start: 0 },
  '26.3': { snapshots: weeklyDates('2026-04-01', 29), start: 13 },
  '26.4': { snapshots: weeklyDates('2026-10-07', 4), start: 0 },
}

/* Feature lookup rows (what EsumEhist.sql returns). `size` is the number of
   stories under the feature, `progress` the share of story points done,
   `spill` how many of its stories were worked in the previous PI. */
const F = (key, summary, team, bv, start, end, size, status, progress, pi = '26.3', release = 'CR3.2', spill = 0) =>
  ({ key, summary, team, bv, start, end, size, status, progress, pi, release, spill })

export const features = [
  F('AC-42888', 'RCCA - 8.1.1 IPR actions - CA1', '', null, '', '', 3, 'Done', 0, '26.3', '8.1.1'),
  F('AC-50770', 'ICC 8.1.1.2 - EGI installation drawings review', 'Navigators', null, '', '', 1, 'In Progress', 100, '26.3', '8.1.1'),
  F('AC-59865', 'ADG STR documentation', 'Tiger Team', 7, '', '', 3, 'Done', 100, '26.3', '8.1.2'),
  F('AC-60241', 'SIMS XML schema update', 'Tiger Team', 8, '', '', 5, 'Done', 100, '26.3', '8.1.2'),
  F('AC-60244', 'P-748 metrics compare to unclassified repository', 'Tiger Team', 6, '', '', 2, 'Done', 100, '26.3', '8.1.2'),
  F('AC-61411', 'C-130J DMS studies - standby instruments SDRL review', 'Guardians', null, '', '', 3, 'Done', 100, '26.3', '8.1.2'),
  F('AC-61513', 'SDL to Silout feature (RAAF)', 'Tiger Team', 7, '', '', 5, 'Done', 100, '26.3', '8.1.2'),
  F('AC-61659', 'EnD 8.1.4.2 - pop-up features PI26.3', 'Thunderbolts', null, '', '', 1, 'To Do', 0, '26.3', '8.1.2'),
  F('AMMMC5-4015', 'WxR investigation and support PI 26.3', '', null, '', '', 2, 'To Do', 0),
  F('CY-1748', 'Support for ICR/CSWG #8', 'Cyber Defenders', 7, '2026-03-02', '2026-04-14', 3, 'Done', 100),
  F('AMMMSM-8114', 'MISC - CFMS demo (April 23rd)', '3H3T', 4, '2026-04-15', '2026-04-23', 3, 'Done', 100),
  F('CY-2705', 'Update ITMS CR 2.0.5 cybersecurity test report - Rev B', 'Cyber Defenders', 7, '2026-03-16', '2026-04-23', 6, 'Done', 100),
  F('AMMMSM-8121', 'J2 - Garmin demo (April 29-30)', '3H3T', 4, '2026-04-15', '2026-04-30', 3, 'Done', 100),
  F('AMMMSM-6255', 'CR3.2 - IMA - MC/SP init miscompare', '3H3T', 2, '2026-04-15', '2026-05-12', 1, 'Done', 100),
  F('AMMMSM-7100', 'CR3.2 - FPP - mission support RV', '3H3T', 7, '2026-01-07', '2026-05-12', 3, 'Done', 100),
  F('AMMMSM-7101', 'CR3.2 - FPP - mission support HP', '3H3T', 8, '2026-01-07', '2026-05-12', 10, 'Done', 100),
  F('AMMMSM-7414', 'MISC - system test automation framework investigation', '3H3T', 2, '2026-01-07', '2026-05-12', 9, 'Done', 100),
  F('AMMMSM-7903', 'NGSMP - TF conversion phase 2', '3H3T', 10, '2026-01-07', '2026-05-12', 5, 'Done', 100),
  F('AMMMSM-7907', 'MISC - CR2.0.7 SITR', '3H3T', 10, '2026-01-07', '2026-05-12', 5, 'Done', 100),
  F('AMMMSM-8064', 'CR3.2 - PIDS/SSDD for inactive/modified route', '3H3T', 1, '2026-04-15', '2026-05-12', 4, 'Done', 100),
  F('AMMMSM-8109', 'CRDR - DR analysis contract', '3H3T', 4, '2026-04-15', '2026-05-12', 9, 'Done', 100),
  F('AMMMSM-8165', 'CR3.2 - Block 8.1 demo (May 14th)', '3H3T', 5, '2026-04-15', '2026-05-14', 3, 'Done', 100),
  F('USSOCOM-3890', 'PI 2026.3 CR 3.0.2 DT/OT findings resolution', 'OMS Rangers', 10, '2026-02-18', '2026-05-18', 19, 'Accepted', 100),
  F('CY-2817', 'ITMS CR3.2 static code analysis finding review - DTB #8', 'Cyber Defenders', null, '2026-04-22', '2026-06-11', 3, 'Done', 100),
  F('AC-52620', 'Perform formal verification and validation (26.3)', 'The Watchmen', 7, '2026-04-01', '2026-06-23', 5, 'Done', 100),
  F('AC-52622', 'Perform SWQE process evaluations (26.3)', 'The Watchmen', 10, '2026-04-01', '2026-06-23', 2, 'In Progress', 0),
  F('AC-55379', 'SWQE continuous improvement (26.3)', 'The Watchmen', 8, '2026-04-01', '2026-06-23', 2, 'In Progress', 0),
  F('AC-55384', 'Perform product evaluations (26.3)', 'The Watchmen', 7, '2026-04-01', '2026-06-23', 4, 'In Progress', 0),
  F('AMMMSM-6736', 'CR3.2 - system integration test event support (PI 2026.3)', '3H3T', 2, '2026-04-01', '2026-06-23', 7, 'Done', 100),
  F('AMMMSM-7019', 'CR3.2 - IMA - rendezvous (RV) display delay', '3H3T', 3, '2026-04-29', '2026-06-23', 11, 'Done', 100),
  F('AMMMSM-7600', 'CR3.2 - RRRT burndown', 'Mega Maniacs', 6, '2026-05-01', '2026-10-14', 53, 'In Progress', 70, '26.3', 'CR3.2', 6),
  F('AMMMSM-7811', 'CR3.2 - datalink message validation', 'Mega Maniacs', 9, '2026-06-02', '2026-09-20', 21, 'In Progress', 62, '26.3', 'CR3.2', 3),
  F('CY-2901', 'Cyber test plan update - CR3.2', 'Cyber Defenders', 5, '2026-07-07', '2026-10-14', 8, 'In Progress', 25),
  F('AMMMSM-7950', 'CR3.2 - mission data loader hardening', '3H3T', 6, '2026-07-01', '2026-10-14', 34, 'In Progress', 80, '26.3', 'CR3.2', 4),
  F('AMMMSM-8010', 'CR3.2 - flight test support (Aug-Sep)', 'Mega Maniacs', 8, '2026-07-15', '2026-10-14', 40, 'In Progress', 90),
  F('AMMMSM-8042', 'CR3.2 - regression suite closeout', '3H3T', 7, '2026-08-01', '2026-10-14', 46, 'In Progress', 85),
  F('USSOCOM-3950', 'CR 3.0.2 DT/OT re-test support', 'OMS Rangers', 9, '2026-07-20', '2026-10-14', 30, 'In Progress', 75),
  // Other PIs, so the Select PI filter changes the page
  F('AMMMSM-6900', 'CR3.1 - FPP regression closeout', '3H3T', 5, '2026-01-07', '2026-03-24', 8, 'Done', 100, '26.2', 'CR3.1'),
  F('AC-49001', 'ADG STR documentation - phase 1', 'Tiger Team', 6, '2026-01-12', '2026-03-20', 3, 'Done', 100, '26.2', '8.1.1'),
  F('CY-1500', 'Support for ICR/CSWG #7', 'Cyber Defenders', 7, '2025-12-01', '2026-03-10', 3, 'Accepted', 100, '26.2', 'CR3.1'),
  F('AMMMSM-7050', 'CR3.1 - datalink regression', 'Mega Maniacs', 8, '2026-01-07', '2026-03-31', 24, 'Done', 100, '26.2', 'CR3.1'),
  F('AMMMSM-8300', 'CR3.3 - IMA - route planning updates', '3H3T', 8, '2026-10-07', '2026-12-15', 13, 'To Do', 0, '26.4', 'CR3.3'),
  F('AC-62010', 'EnD 8.1.5 - pop-up features PI26.4', 'Thunderbolts', 4, '2026-10-07', '2026-12-15', 5, 'To Do', 0, '26.4', '8.1.2'),
  F('USSOCOM-4102', 'PI 2026.4 CR 3.0.3 DT/OT prep', 'OMS Rangers', 9, '2026-10-07', '2026-12-20', 21, 'In Progress', 10, '26.4', 'CR3.3'),
]

/* Deterministic story generator (seeded PRNG) so every load shows the same
   rows. Each feature gets `size` stories; their estimates, the snapshot
   they first appear in and the snapshot they are done in follow the
   feature's dates and progress. */
const mulberry32 = (seed) => () => {
  seed |= 0; seed = (seed + 0x6D2B79F5) | 0
  let t = Math.imul(seed ^ (seed >>> 15), 1 | seed)
  t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296
}
const ESTIMATES = [1, 2, 3, 5, 8, 13]
const prevPi = (pi) => PI_OPTIONS[Math.max(0, PI_OPTIONS.indexOf(pi) - 1)]
const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v))

const buildStories = () => {
  const rnd = mulberry32(20263)
  const out = []
  features.forEach((f, fi) => {
    const win = PI_WINDOWS[f.pi]
    const last = win.snapshots.length - 1
    const weekOf = (d, fallback) => {
      if (!d) return fallback
      const i = win.snapshots.findIndex((s) => s >= d)
      return i < 0 ? last : i
    }
    const startW = weekOf(f.start, win.start)
    const endW = Math.max(startW, weekOf(f.end, last))
    const doneCount = Math.round((f.size * f.progress) / 100)
    const project = f.key.split('-')[0]
    for (let i = 0; i < f.size; i += 1) {
      const estimate = ESTIMATES[Math.floor(rnd() * ESTIMATES.length)]
      const addedWeek = clamp(startW + Math.floor(rnd() * Math.max(1, (endW - startW) * 0.5)), 0, last)
      const isDone = i < doneCount
      const doneWeek = isDone ? clamp(addedWeek + 1 + Math.floor(rnd() * Math.max(1, endW - addedWeek)), addedWeek, last) : null
      const spill = i < f.spill
      const otherTeam = i % 7 === 6 ? TEAMS[Math.floor(rnd() * TEAMS.length)] : null
      out.push({
        storyNumber: `${project}-${90000 + fi * 100 + i}`,
        storyName: `${f.summary} - story ${i + 1}`,
        teamName: otherTeam || f.team || 'Unassigned',
        sprintName: `AMMM ${spill ? prevPi(f.pi) : f.pi}.${1 + (addedWeek % 5)}`,
        programIncrement: `PI ${spill ? prevPi(f.pi) : f.pi}`,
        fixVersion: f.release,
        status: isDone ? 'Done' : (addedWeek < last ? 'In Progress' : 'To Do'),
        estimate,
        closedDate: doneWeek == null ? null : win.snapshots[doneWeek],
        featureId: f.key,
        feature: f.summary,
        featureTeam: f.team,
        featurePi: f.pi,
        featureFixVersion: f.release,
        featureStatus: f.status,
        bv: f.bv,
        targetStart: f.start,
        targetEnd: f.end,
        addedWeek,
        doneWeek,
      })
    }
  })
  return out
}

export const stories = buildStories()

export const analysisNotes = [
  { date: '2026-10-21', who: 'RTE', text: 'Scope grew through the PI 26.3 midpoint when CR3.2 flight test and regression support were pulled in; done points track the ideal from September onward.' },
  { date: '2026-10-14', who: 'Program', text: 'The Watchmen SWQE evaluations (AC-52622 / 55379 / 55384) stay In Progress with 0% done; re-plan into 26.4 or close.' },
  { date: '2026-10-07', who: 'Product', text: 'USSOCOM-3890 accepted; remaining DT/OT findings moved to USSOCOM-4102 in PI 26.4.' },
]
