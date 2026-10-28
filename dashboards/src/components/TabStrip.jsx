export const TABS = [
  'Program Feature Progress', 'Feature Burnup by Release', 'Program Roadmap', 'Program Planning',
  'Performance To Plan', 'Performance To Plan (SOCOM)', 'Feature Burnup by PI', 'Team Metrics',
  'Flow Metrics', 'Team Predictability',
]

export default function TabStrip({ active, onSelect }) {
  return (
    <nav className="glass tabs" role="tablist" aria-label="Dashboard sheets">
      {TABS.map((t) => (
        <button key={t} type="button" role="tab" className="tab" aria-selected={t === active}
                onClick={() => onSelect(t)}>
          <span className="grid-ico" aria-hidden="true" />{t}
        </button>
      ))}
    </nav>
  )
}
