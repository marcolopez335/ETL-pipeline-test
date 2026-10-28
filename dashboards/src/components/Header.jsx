import { STAMPS } from '../data/mock.js'

export default function Header({ pi, theme, onToggleTheme }) {
  const dark = theme !== 'light'
  return (
    <header className="glass header">
      <div className="brand">
        <span className="stamp">DATA EXTRACT: {STAMPS.extract}</span>
        <span className="stamp">DATA LOAD: {STAMPS.load}</span>
        <span className="program">LMPI | AMMM Program Information</span>
      </div>
      <h1 className="title">
        Program Metrics<span className="sep">|</span><span className="pi">PI {pi}</span>
      </h1>
      <div className="header-actions">
        <button type="button" className="btn small" onClick={onToggleTheme} aria-pressed={!dark}
                title="Switch between the dark and light glass themes">
          <span aria-hidden="true">{dark ? '☾' : '☀'}</span> {dark ? 'Dark' : 'Light'}
        </button>
        <button type="button" className="btn icon" aria-label="Menu">
          <span className="menu-lines" aria-hidden="true"><i /><i /><i /></span>
        </button>
      </div>
    </header>
  )
}
