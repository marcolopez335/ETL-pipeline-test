import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// base './' keeps asset URLs relative so the built dist/ opens from any path
// (a static file server, a Tableau web object, or an artifact preview).
export default defineConfig({
  plugins: [react()],
  base: './',
})
