/* The Group filter: each group is a set of feature teams (the Owner /
   Feature Team values). Add the real team names to the lists below. A team
   that appears in no list counts as General, so the filter works before
   the lists are complete. Names must match the Feature Team values in the
   data exactly. The teams listed now are the sample data's. */
export const GROUP_NAMES = [
  'AMMM ART (C130J)',
  'Special Missions ART',
  'Platform ART',
  'AMMM ART (C5)',
  'General',
]

export const GROUPS = {
  'AMMM ART (C130J)': ['Tiger Team', 'Guardians'],
  'Special Missions ART': ['OMS Rangers', 'The Watchmen'],
  'Platform ART': ['3H3T', 'Mega Maniacs'],
  'AMMM ART (C5)': ['Thunderbolts', 'Navigators'],
}

const lookup = new Map()
for (const [group, teams] of Object.entries(GROUPS)) for (const t of teams) lookup.set(t, group)

export const groupOf = (team) => lookup.get(team) || 'General'
