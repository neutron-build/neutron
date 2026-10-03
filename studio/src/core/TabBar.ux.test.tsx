import { it, expect, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup } from '@testing-library/preact'
import { tabs, activeTabId, openTab } from '../lib/store'
import { TabBar } from './TabBar'
afterEach(() => { cleanup(); tabs.value = []; activeTabId.value = null })
it('exposes independent keyboard-native close controls without nested buttons', () => {
  openTab({ id: 'a', kind: 'sql-editor', label: 'First query', initialSql: 'SELECT 1' })
  openTab({ id: 'b', kind: 'sql-editor', label: 'Second query', initialSql: 'SELECT 2' })
  render(<TabBar />)
  const close = screen.getByRole('button', { name: 'Close Second query' })
  expect(close.parentElement?.tagName).toBe('DIV')
  close.focus(); expect(document.activeElement).toBe(close)
  fireEvent.click(close)
  expect(activeTabId.value).toBe('a')
  expect(tabs.value.map(t => t.id)).toEqual(['a'])
  expect(screen.getByRole('button', { name: 'First query' }).getAttribute('aria-pressed')).toBe('true')
})
