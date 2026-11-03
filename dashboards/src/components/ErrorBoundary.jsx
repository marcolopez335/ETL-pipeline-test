import { Component } from 'react'

/* A render error used to unmount the whole page and leave it black, with
   the reason only in the browser console. Show it on screen instead, with
   two ways out: reload, or reload on the sample data to tell a data
   problem from a code problem. */
export default class ErrorBoundary extends Component {
  constructor(props) {
    super(props)
    this.state = { error: null }
  }

  static getDerivedStateFromError(error) {
    return { error }
  }

  componentDidCatch(error, info) {
    console.error('[dashboard] render failed', error, info?.componentStack)
  }

  render() {
    const { error } = this.state
    if (!error) return this.props.children
    const stack = String(error?.stack || error).split('\n').slice(0, 12).join('\n')
    const where = (this.state.info || '').toString()
    return (
      <div className="app">
        <section className="glass card" role="alert" style={{ maxWidth: 900, margin: '40px auto' }}>
          <div className="card-head"><h2>The dashboard hit an error while rendering</h2></div>
          <p style={{ margin: 0, color: 'var(--text-2)' }}>
            Send a photo of this panel. The first line is the error, the rest is where it happened.
          </p>
          <pre style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word', font: '12px/1.5 Consolas, monospace', margin: 0 }}>{stack}{where}</pre>
          <div className="card-tools">
            <button type="button" className="btn small" onClick={() => window.location.reload()}>Reload</button>
            <button type="button" className="btn small" onClick={() => { window.location.search = '?data=sample' }}>
              Reload with sample data
            </button>
          </div>
        </section>
      </div>
    )
  }
}
