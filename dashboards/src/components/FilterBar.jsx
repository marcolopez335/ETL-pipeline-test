import Select from './Select.jsx'
import MultiSelect from './MultiSelect.jsx'
import { GROUP_NAMES } from '../data/groups.js'

export const SORT_OPTIONS = ['END Date', 'START Date', 'BV', 'Feature', 'Status', 'Owner']

/* One row, above everything it scopes. Single-choice filters are native
   selects; the categorical ones are checkbox lists with Apply. Their
   choices come from the loaded dataset: Target Release = Feature Fix
   Version, Program = Customer Epic, Owner = Feature Team, Contributor =
   the story's Team Name, BV = the feature's BV; Group maps Owner teams
   through data/groups.js. */
const filterDefs = (o) => [
  { id: 'pi', label: 'Select PI', kind: 'single', options: o.pis },
  { id: 'release', label: 'Target Release', kind: 'multi', options: o.releases },
  { id: 'group', label: 'Group', kind: 'multi', options: GROUP_NAMES },
  { id: 'program', label: 'Program', kind: 'multi', options: o.programs },
  { id: 'owner', label: 'Owner', kind: 'multi', options: o.owners },
  { id: 'contributor', label: 'Contributor', kind: 'multi', options: o.contributors },
  { id: 'sortBy', label: 'Sort By', kind: 'single', options: SORT_OPTIONS },
  { id: 'predictability', label: 'Predictability', kind: 'single', options: ['Hide', 'Show'] },
  { id: 'idealBurnup', label: 'Ideal Burnup', kind: 'single', options: ['Planned', 'Hide'] },
  { id: 'bv', label: 'BV', kind: 'multi', options: o.bvs },
]

export default function FilterBar({ filters, onChange, options }) {
  return (
    <section className="glass filters" aria-label="Filters">
      {filterDefs(options).map((f) => (f.kind === 'multi' ? (
        <MultiSelect key={f.id} id={`filter-${f.id}`} label={f.label} options={f.options}
                     value={filters[f.id]} onApply={(v) => onChange(f.id, v)} />
      ) : (
        <Select key={f.id} id={`filter-${f.id}`} label={f.label} options={f.options}
                value={filters[f.id]} onChange={(v) => onChange(f.id, v)} />
      )))}
    </section>
  )
}
