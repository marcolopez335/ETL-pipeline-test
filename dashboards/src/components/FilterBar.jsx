import Select from './Select.jsx'
import { PI_OPTIONS, RELEASE_OPTIONS, TEAMS } from '../data/mock.js'

export const SORT_OPTIONS = ['END Date', 'START Date', 'BV', 'Feature', 'Status', 'Owner']
export const BV_OPTIONS = ['ALL', '8 and up', '5 and up', '4 and under']

/* One row, above everything it scopes. Every select is wired to App state;
   the ones the mockup does not model yet (Group, Program, Contributor,
   Predictability) still hold a value so the row reads like production. */
const FILTERS = [
  { id: 'pi', label: 'Select PI', options: PI_OPTIONS },
  { id: 'release', label: 'Target Release', options: RELEASE_OPTIONS },
  { id: 'group', label: 'Group', options: ['(All)', 'Avionics', 'Mission Systems', 'Cyber'] },
  { id: 'program', label: 'Program', options: ['(All)', 'AMMM', 'USSOCOM', 'C-130J'] },
  { id: 'owner', label: 'Owner', options: ['(All)', ...TEAMS] },
  { id: 'contributor', label: 'Contributor', options: ['(All)', ...TEAMS] },
  { id: 'sortBy', label: 'Sort By', options: SORT_OPTIONS },
  { id: 'predictability', label: 'Predictability', options: ['Hide', 'Show'] },
  { id: 'idealBurnup', label: 'Ideal Burnup', options: ['Planned', 'Hide'] },
  { id: 'bv', label: 'BV', options: BV_OPTIONS },
]

export default function FilterBar({ filters, onChange }) {
  return (
    <section className="glass filters" aria-label="Filters">
      {FILTERS.map((f) => (
        <Select key={f.id} id={`filter-${f.id}`} label={f.label} options={f.options}
                value={filters[f.id]} onChange={(v) => onChange(f.id, v)} />
      ))}
    </section>
  )
}
