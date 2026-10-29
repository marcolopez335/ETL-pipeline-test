/* Feature Progress columns. `filter` is the header filter kind:
   text (contains), values (pick from the distinct values), range (from/to). */
export const COLUMNS = [
  { key: 'key', label: 'Feature', filter: 'text' },
  { key: 'owner', label: 'Owner', filter: 'values' },
  { key: 'bv', label: 'BV', num: true, filter: 'values' },
  { key: 'start', label: 'Start', filter: 'range', type: 'date' },
  { key: 'end', label: 'End', filter: 'range', type: 'date' },
  { key: 'size', label: 'Size', num: true, filter: 'range', type: 'number' },
  { key: 'status', label: 'Status', filter: 'values' },
  { key: 'piPct', label: 'PI', filter: 'range', type: 'pct' },
  { key: 'totalPct', label: 'Total', filter: 'range', type: 'pct' },
]
