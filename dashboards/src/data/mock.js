/* Mock data for the mockup. Each feature row mirrors the EPICS.hyper columns:
   key -> Feature Key, summary -> Summary, owner -> Team, bv -> Bv,
   start/end -> Begin Date / End Date, size -> Total Estimate,
   status -> Status, piPct -> Done In Feature Pi / Total In Feature Pi,
   totalPct -> Done Estimate / Total Estimate, pi -> Program Increment,
   release -> Feature Fix Version. Swap this file for a JSON built from
   `python main.py --epics --csv` to drive the mockup with real rows. */

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

const F = (key, summary, owner, bv, start, end, size, status, piPct, totalPct, pi = '26.3', release = 'CR3.2') =>
  ({ key, summary, owner, bv, start, end, size, status, piPct, totalPct, pi, release })

export const features = [
  F('AC-42888', 'RCCA - 8.1.1 IPR actions - CA1', '', null, '', '', 3, 'Done', 0, 0, '26.3', '8.1.1'),
  F('AC-50770', 'ICC 8.1.1.2 - EGI installation drawings review', 'Navigators', null, '', '', 1, 'In Progress', 100, 100, '26.3', '8.1.1'),
  F('AC-59865', 'ADG STR documentation', 'Tiger Team', 7, '', '', 3, 'Done', 100, 100, '26.3', '8.1.2'),
  F('AC-60241', 'SIMS XML schema update', 'Tiger Team', 8, '', '', 5, 'Done', 100, 100, '26.3', '8.1.2'),
  F('AC-60244', 'P-748 metrics compare to unclassified repository', 'Tiger Team', 6, '', '', 2, 'Done', 100, 100, '26.3', '8.1.2'),
  F('AC-61411', 'C-130J DMS studies - standby instruments SDRL review', 'Guardians', null, '', '', 3, 'Done', 100, 100, '26.3', '8.1.2'),
  F('AC-61513', 'SDL to Silout feature (RAAF)', 'Tiger Team', 7, '', '', 5, 'Done', 100, 100, '26.3', '8.1.2'),
  F('AC-61659', 'EnD 8.1.4.2 - pop-up features PI26.3', 'Thunderbolts', null, '', '', 1, 'To Do', 0, 0, '26.3', '8.1.2'),
  F('AMMMC5-4015', 'WxR investigation and support PI 26.3', '', null, '', '', null, 'To Do', null, null),
  F('CY-1748', 'Support for ICR/CSWG #8', 'Cyber Defenders', 7, '2026-03-02', '2026-04-14', 3, 'Done', 100, 100),
  F('AMMMSM-8114', 'MISC - CFMS demo (April 23rd)', '3H3T', 4, '2026-04-15', '2026-04-23', 3, 'Done', 100, 100),
  F('CY-2705', 'Update ITMS CR 2.0.5 cybersecurity test report - Rev B', 'Cyber Defenders', 7, '2026-03-16', '2026-04-23', 6, 'Done', 100, 100),
  F('AMMMSM-8121', 'J2 - Garmin demo (April 29-30)', '3H3T', 4, '2026-04-15', '2026-04-30', 3, 'Done', 100, 100),
  F('AMMMSM-6255', 'CR3.2 - IMA - MC/SP init miscompare', '3H3T', 2, '2026-04-15', '2026-05-12', 1, 'Done', 100, 100),
  F('AMMMSM-7100', 'CR3.2 - FPP - mission support RV', '3H3T', 7, '2026-01-07', '2026-05-12', 3, 'Done', 100, 100),
  F('AMMMSM-7101', 'CR3.2 - FPP - mission support HP', '3H3T', 8, '2026-01-07', '2026-05-12', 10, 'Done', 100, 100),
  F('AMMMSM-7414', 'MISC - system test automation framework investigation', '3H3T', 2, '2026-01-07', '2026-05-12', 9, 'Done', 100, 100),
  F('AMMMSM-7903', 'NGSMP - TF conversion phase 2', '3H3T', 10, '2026-01-07', '2026-05-12', 5, 'Done', 100, 100),
  F('AMMMSM-7907', 'MISC - CR2.0.7 SITR', '3H3T', 10, '2026-01-07', '2026-05-12', 5, 'Done', 100, 100),
  F('AMMMSM-8064', 'CR3.2 - PIDS/SSDD for inactive/modified route', '3H3T', 1, '2026-04-15', '2026-05-12', 4, 'Done', 100, 100),
  F('AMMMSM-8109', 'CRDR - DR analysis contract', '3H3T', 4, '2026-04-15', '2026-05-12', 9, 'Done', 100, 100),
  F('AMMMSM-8165', 'CR3.2 - Block 8.1 demo (May 14th)', '3H3T', 5, '2026-04-15', '2026-05-14', 3, 'Done', 100, 100),
  F('USSOCOM-3890', 'PI 2026.3 CR 3.0.2 DT/OT findings resolution', 'OMS Rangers', 10, '2026-02-18', '2026-05-18', 19, 'Accepted', 100, 100),
  F('CY-2817', 'ITMS CR3.2 static code analysis finding review - DTB #8', 'Cyber Defenders', null, '2026-04-22', '2026-06-11', 3, 'Done', 100, 100),
  F('AC-52620', 'Perform formal verification and validation (26.3)', 'The Watchmen', 7, '2026-04-01', '2026-06-23', 5, 'Done', 100, 100),
  F('AC-52622', 'Perform SWQE process evaluations (26.3)', 'The Watchmen', 10, '2026-04-01', '2026-06-23', 2, 'In Progress', 0, 0),
  F('AC-55379', 'SWQE continuous improvement (26.3)', 'The Watchmen', 8, '2026-04-01', '2026-06-23', 2, 'In Progress', 0, 0),
  F('AC-55384', 'Perform product evaluations (26.3)', 'The Watchmen', 7, '2026-04-01', '2026-06-23', 4, 'In Progress', 0, 0),
  F('AMMMSM-6736', 'CR3.2 - system integration test event support (PI 2026.3)', '3H3T', 2, '2026-04-01', '2026-06-23', 7, 'Done', 100, 100),
  F('AMMMSM-7019', 'CR3.2 - IMA - rendezvous (RV) display delay', '3H3T', 3, '2026-04-29', '2026-06-23', 11, 'Done', 100, 100),
  F('AMMMSM-7600', 'CR3.2 - RRRT burndown', 'Mega Maniacs', 6, '2026-05-01', '2026-07-23', 53, 'In Progress', 45, 45),
  F('AMMMSM-7811', 'CR3.2 - datalink message validation', 'Mega Maniacs', 9, '2026-06-02', '2026-08-20', 21, 'In Progress', 62, 58),
  F('CY-2901', 'Cyber test plan update - CR3.2', 'Cyber Defenders', 5, '2026-07-07', '2026-09-15', 8, 'In Progress', 25, 25),
  // Other PIs, so the Select PI filter changes the page
  F('AMMMSM-6900', 'CR3.1 - FPP regression closeout', '3H3T', 5, '2026-01-07', '2026-03-24', 8, 'Done', 100, 100, '26.2', 'CR3.1'),
  F('AC-49001', 'ADG STR documentation - phase 1', 'Tiger Team', 6, '2026-01-12', '2026-03-20', 3, 'Done', 100, 100, '26.2', '8.1.1'),
  F('CY-1500', 'Support for ICR/CSWG #7', 'Cyber Defenders', 7, '2025-12-01', '2026-03-10', 3, 'Accepted', 100, 100, '26.2', 'CR3.1'),
  F('AMMMSM-8300', 'CR3.3 - IMA - route planning updates', '3H3T', 8, '2026-10-07', '2026-12-15', 13, 'To Do', 0, 0, '26.4', 'CR3.3'),
  F('AC-62010', 'EnD 8.1.5 - pop-up features PI26.4', 'Thunderbolts', 4, '2026-10-07', '2026-12-15', 5, 'To Do', 0, 0, '26.4', '8.1.2'),
  F('USSOCOM-4102', 'PI 2026.4 CR 3.0.3 DT/OT prep', 'OMS Rangers', 9, '2026-10-07', '2026-12-20', 21, 'In Progress', 10, 10, '26.4', 'CR3.3'),
]

/* Weekly burn-up points per PI: scope (story points committed), done, and
   the ideal line from the PI's first day to its last. Values follow the
   shape of the production chart. */
const weekly = (startIso, scope, done, idealFrom) => {
  const [y, m, d] = startIso.split('-').map(Number)
  const start = new Date(y, m - 1, d)
  const n = scope.length
  const points = scope.map((s, i) => {
    const dt = new Date(start)
    dt.setDate(start.getDate() + i * 7)
    const iso = `${dt.getFullYear()}-${String(dt.getMonth() + 1).padStart(2, '0')}-${String(dt.getDate()).padStart(2, '0')}`
    const ideal = i < idealFrom ? null : (s === null ? null : (scope[n - 1] * (i - idealFrom)) / (n - 1 - idealFrom))
    return { date: iso, scope: s, done: done[i], ideal: ideal == null ? null : Math.round(ideal * 10) / 10 }
  })
  return points
}

export const burnupByPi = {
  '26.3': weekly('2026-04-01',
    [198, 410, 536, 585, 640, 695, 700, 702, 697, 697, 700, 760, 900, 1216, 1500, 1776, 2095, 2473.6, 2622.8, 2850.9, 2854, 3060.5, 3210.3, 3334.5, 3360, 3385.7, 3385.7, 3385.7, 3385.7],
    [0, 0, 0, 0, 0, 0, 0, 0, 41, 81.7, 174.6, 200, 234, 364, 416.7, 496, 501.6, 567, 607, 684.5, 848, 943.4, 1034.1, 1216, 1291.6, 1492.1, 1726.4, 1910.5, 2976.9],
    13),
  '26.2': weekly('2026-01-07',
    [320, 540, 760, 820, 880, 910, 940, 960, 980, 980, 1010, 1010, 1010],
    [0, 40, 120, 210, 330, 420, 520, 610, 700, 790, 880, 960, 1010],
    0),
  '26.4': weekly('2026-10-07',
    [640, 980, 1240, 1400],
    [0, 30, 95, 180],
    0),
}

export const analysisNotes = [
  { date: '2026-10-21', who: 'RTE', text: 'Scope grew 1,200 points at the PI 26.3 midpoint when CR3.2 test support was pulled in; burn-up tracks the ideal from Sep onward.' },
  { date: '2026-10-14', who: 'Program', text: 'The Watchmen SWQE evaluations (AC-52622 / 55379 / 55384) stay In Progress with 0% done; re-plan into 26.4 or close.' },
  { date: '2026-10-07', who: 'Product', text: 'USSOCOM-3890 accepted; remaining DT/OT findings moved to USSOCOM-4102 in PI 26.4.' },
]
