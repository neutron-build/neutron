import { render } from 'preact'
import { App } from './core/App'
import './style/global.css'

// Apply saved theme before first render
const saved = localStorage.getItem('studio-theme')
document.documentElement.setAttribute('data-theme', saved ?? 'dark')

render(<App />, document.getElementById('app')!)
