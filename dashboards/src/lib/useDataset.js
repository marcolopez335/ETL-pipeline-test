import { useEffect, useState } from 'react'
import { sampleDataset } from '../data/mock.js'

/* Where the stories pipeline drops the real rows (config.yaml:
   stories.dashboard_json -> dashboards/public/data/stories.json, served as
   ./data/stories.json next to the page). */
export const DATA_URL = './data/stories.json'

/* Load the real dataset when the file is there; otherwise keep the sample.
   status: 'loading' | 'live' | 'sample'. */
export function useDataset() {
  const [state, setState] = useState({ dataset: sampleDataset, status: 'loading' })

  useEffect(() => {
    let alive = true
    fetch(DATA_URL, { cache: 'no-store' })
      .then(async (res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const data = await res.json()
        if (!Array.isArray(data.stories) || typeof data.pis !== 'object') throw new Error('unexpected shape')
        if (alive) setState({ dataset: { ...data, source: 'live' }, status: 'live' })
      })
      .catch(() => { if (alive) setState({ dataset: sampleDataset, status: 'sample' }) })
    return () => { alive = false }
  }, [])

  return state
}
