import { it, expect, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup, act } from '@testing-library/preact'
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
  expect(document.activeElement).toBe(screen.getByRole('button', { name: 'First query' }))
})


it('closing an inactive focused tab returns focus to the surviving active tab', () => {
  openTab({ id: 'a', kind: 'sql-editor', label: 'Inactive query', initialSql: 'SELECT 1' })
  openTab({ id: 'b', kind: 'sql-editor', label: 'Active query', initialSql: 'SELECT 2' })
  render(<TabBar />)
  const close = screen.getByRole('button', { name: 'Close Inactive query' })
  close.focus()
  fireEvent.click(close)
  expect(activeTabId.value).toBe('b')
  expect(document.activeElement).toBe(screen.getByRole('button', { name: 'Active query' }))
})

it('closing the last focused tab returns to the workspace, but unfocused closes do not steal focus', () => {
  openTab({ id: 'a', kind: 'sql-editor', label: 'Only query', initialSql: 'SELECT 1' })
  render(<><main id="studio-content" tabIndex={-1}><input aria-label="Workspace input" /></main><TabBar /></>)
  const close = screen.getByRole('button', { name: 'Close Only query' })
  const input = screen.getByRole('textbox', { name: 'Workspace input' })
  input.focus()
  fireEvent.click(close)
  expect(document.activeElement).toBe(input)
  act(() => { openTab({ id: 'b', kind: 'sql-editor', label: 'Final query', initialSql: 'SELECT 2' }) })
  const finalClose = screen.getByRole('button', { name: 'Close Final query' })
  finalClose.focus()
  fireEvent.click(finalClose)
  expect(document.activeElement).toBe(screen.getByRole('main'))
})
