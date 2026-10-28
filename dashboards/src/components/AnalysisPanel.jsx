import { useState } from 'react'
import { fmtLong } from '../lib/format.js'

export default function AnalysisPanel({ initialNotes }) {
  const [notes, setNotes] = useState(initialNotes)
  const [draft, setDraft] = useState('')

  const add = (e) => {
    e.preventDefault()
    const text = draft.trim()
    if (!text) return
    const d = new Date()
    const iso = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
    setNotes([{ date: iso, who: 'You', text }, ...notes])
    setDraft('')
  }

  return (
    <section className="glass card" aria-labelledby="analysis-h">
      <div className="card-head">
        <h2 id="analysis-h">Analysis / Corrective Action</h2>
        <span className="meta">{notes.length} note{notes.length === 1 ? '' : 's'}</span>
      </div>
      <form className="note-form" onSubmit={add}>
        <textarea value={draft} onChange={(e) => setDraft(e.target.value)}
                  placeholder="What changed, why, and what we are doing about it" aria-label="New note" />
        <div className="row"><button type="submit" className="btn small">Add note</button></div>
      </form>
      <div className="notes">
        {notes.map((n, i) => (
          <article key={`${n.date}-${i}`} className="note">
            <div className="who">{fmtLong(n.date)} · {n.who}</div>
            <p>{n.text}</p>
          </article>
        ))}
      </div>
    </section>
  )
}
