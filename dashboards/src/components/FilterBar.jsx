import Select from './Select.jsx'

export const SORT_OPTIONS = ['END Date', 'START Date', 'BV', 'Feature', 'Status', 'Owner']
export const BV_OPTIONS = ['ALL', '8 and up', '5 and up', '4 and under']

/* One row, above everything it scopes. PI, release, owner and contributor
   choices come from the loaded dataset; the ones the mockup does not model
   yet (Group, Program, Predictability) still hold a value so the row reads
   like production. */
const filterDefs = (o) => [
  { id: 'pi', label: 'Select PI', options: o.pis },
  { id: 'release', label: 'Target Release', options: ['(All)', ...o.releases] },
  { id: 'group', label: 'Group', options: ['(All)', 'Avionics', 'Mission Systems', 'Cyber'] },
  { id: 'program', label: 'Program', options: ['(All)', 'AMMM', 'USSOCOM', 'C-130J'] },
  { id: 'owner', label: 'Owner', options: ['(All)', ...o.owners] },
  { id: 'contributor', label: 'Contributor', options: ['(All)', ...o.contributors] },
  { id: 'sortBy', label: 'Sort By', options: SORT_OPTIONS },
  { id: 'predictability', label: 'Predictability', options: ['Hide', 'Show'] },
  { id: 'idealBurnup', label: 'Ideal Burnup', options: ['Planned', 'Hide'] },
  { id: 'bv', label: 'BV', options: BV_OPTIONS },
]

export default function FilterBar({ filters, onChange, options }) {
  return (
    <section className="glass filters" aria-label="Filters">
      {filterDefs(options).map((f) => (
        <Select key={f.id} id={`filter-${f.id}`} label={f.label} options={f.options}
                value={filters[f.id]} onChange={(v) => onChange(f.id, v)} />
      ))}
    </section>
  )
}
