import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import tokensCss from './styles/tokens.css?raw'
import './styles/tokens.css'
import './styles/glass.css'
import './styles/app.css'
import App from './App.jsx'

/* Every color on the page is a CSS variable from tokens.css. If they come up
   missing (a stylesheet that did not apply, an extension that rewrote the
   variables), re-inject them once and say so, so the symptom is not a page
   with no panels, meters or chart. */
const tokensPresent = () => getComputedStyle(document.documentElement).getPropertyValue('--glass-bg').trim() !== ''
if (!tokensPresent()) {
  const style = document.createElement('style')
  style.id = 'tokens-fallback'
  style.textContent = tokensCss
  document.head.appendChild(style)
  console.warn(`[dashboard] color tokens were missing and have been re-injected (now ${tokensPresent() ? 'present' : 'STILL missing: something is stripping CSS variables'})`)
}

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
