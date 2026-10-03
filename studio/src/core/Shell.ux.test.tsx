import { it, expect, vi, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import { activeConnection, activeTabId } from '../lib/store'
import { Shell } from './Shell'
vi.mock('./Sidebar', () => ({ Sidebar: () => <aside><button data-workspace-navigation="true">First navigation action</button><input aria-label="Search"/><button>Last navigation action</button></aside> }))
vi.mock('./ContentArea', () => ({ ContentArea: () => <div>Workspace content</div> }))
vi.mock('./CommitBar', () => ({ CommitBar: () => null }))
afterEach(() => { cleanup(); activeConnection.value = null; activeTabId.value = null })
it('keeps keyboard focus inside open narrow navigation and returns it on Escape', async () => {
  activeConnection.value = { id: 'c', name: 'local', url: 'masked', isNucleus: false }
  render(<Shell />)
  const toggle = screen.getByRole('button', { name: 'Toggle database navigation' })
  fireEvent.click(toggle)
  await waitFor(() => expect(document.activeElement).toBe(screen.getByRole('button', { name: 'First navigation action' })))
  expect(screen.getByRole('dialog').getAttribute('aria-modal')).toBe('true')
  expect(screen.getByRole('main').hasAttribute('inert')).toBe(true)
  const last = screen.getByRole('button', { name: 'Last navigation action' }); last.focus()
  fireEvent.keyDown(document.activeElement!, { key: 'Tab' })
  expect(document.activeElement).toBe(screen.getByRole('button', { name: 'First navigation action' }))
  fireEvent.keyDown(document.activeElement!, { key: 'Tab', shiftKey: true })
  expect(document.activeElement).toBe(last)
  fireEvent.keyDown(document.activeElement!, { key: 'Escape' })
  await waitFor(() => expect(toggle.getAttribute('aria-expanded')).toBe('false'))
  expect(document.activeElement).toBe(toggle)
  expect(screen.getByRole('main').hasAttribute('inert')).toBe(false)
})

it('closes navigation after selecting an object and focuses the workspace without replacing its deep link', async () => {
  activeConnection.value = { id: 'c', name: 'local', url: 'masked', isNucleus: false }
  render(<Shell />)
  fireEvent.click(screen.getByRole('button', { name: 'Toggle database navigation' }))
  await waitFor(() => expect(screen.getByRole('dialog')).toBeTruthy())
  fireEvent.click(screen.getByRole('button', { name: 'First navigation action' }))
  await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull())
  expect(document.activeElement).toBe(screen.getByRole('main'))
  history.replaceState(null, '', '#/c/c/sql')
  fireEvent.click(screen.getByRole('link', { name: 'Skip to workspace' }))
  expect(location.hash).toBe('#/c/c/sql')
  expect(document.activeElement).toBe(screen.getByRole('main'))
})
