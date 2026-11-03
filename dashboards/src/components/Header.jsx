import { useState } from 'react'
import Diagnostics from './Diagnostics.jsx'

const STATUS_LABEL = { live: 'Live data', sample: 'Sample data', loading: 'Loading data' }

export default function Header({ pi, stamps, status, dataset, theme, onToggleTheme }) {
  const dark = theme !== 'light'
  const [diag, setDiag] = useState(null)   // the menu button while the diagnostics panel is open
  return (
    <header className="glass header">
      <div className="brand">
        <span className="stamp">DATA EXTRACT: {stamps.extract || 'n/a'}</span>
        <span className="stamp">DATA LOAD: {stamps.load || 'n/a'}</span>
        <span className="program">
          LMPI | AMMM Program Information
          <span className={`chip ${status}`} title="Where the rows on this page come from">{STATUS_LABEL[status] || status}</span>
        </span>
      </div>
      <h1 className="title">
        Program Metrics<span className="sep">|</span><span className="pi">PI {pi}</span>
      </h1>
      <div className="header-actions">
        <button type="button" className="btn small" onClick={onToggleTheme} aria-pressed={!dark}
                title="Switch between the dark and light glass themes">
          <span aria-hidden="true">{dark ? '☾' : '☀'}</span> {dark ? 'Dark' : 'Light'}
        </button>
        <button type="button" className="btn icon" aria-label="Menu and diagnostics" aria-expanded={diag != null}
                title="Diagnostics" onClick={(e) => setDiag(diag ? null : e.currentTarget)}>
          <span className="menu-lines" aria-hidden="true"><i /><i /><i /></span>
        </button>
      </div>
      {diag && <Diagnostics anchor={diag} status={status} dataset={dataset} onClose={() => setDiag(null)} />}
    </header>
  )
}
