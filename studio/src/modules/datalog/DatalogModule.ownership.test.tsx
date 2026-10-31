import { beforeEach, afterEach, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import { activeConnection } from '../../lib/store'
import { DatalogModule } from './DatalogModule'
vi.mock('../../lib/api', async original => { const m = await original<typeof import('../../lib/api')>(); return { ...m, api: { ...m.api, query: vi.fn() } } })
import { api } from '../../lib/api'
const ok = { columns: ['v'], rows: [], rowCount: 0, duration: 0 }
beforeEach(() => { activeConnection.value = { id: 'c1', name: 'one', url: 'pg://one', isNucleus: true }; vi.mocked(api.query).mockReset() })
afterEach(cleanup)
it('reports earlier successful writes after later failure and blocks blind repeat', async () => {
  vi.mocked(api.query).mockResolvedValueOnce(ok).mockResolvedValueOnce({ ...ok, error: 'second assertion denied' })
  render(<DatalogModule />)
  const textarea = document.querySelector('textarea')!
  fireEvent.input(textarea, { target: { value: 'a(1).\nb(2).\n?- a(X)' } })
  fireEvent.keyDown(textarea, { key: 'Enter', ctrlKey: true }); fireEvent.keyDown(textarea, { key: 'Enter', ctrlKey: true, repeat: true })
  await screen.findByText(/c1: 1 earlier fact\/rule write/)
  expect(api.query).toHaveBeenCalledTimes(2)
  fireEvent.keyDown(textarea, { key: 'Enter', ctrlKey: true })
  expect(api.query).toHaveBeenCalledTimes(2)
  expect(screen.getByText('▶ Evaluate').hasAttribute('disabled')).toBe(true)
  expect(screen.getByText('I verified database state; allow another evaluation')).toBeTruthy()
})
it('connection change during an acknowledged assertion stops subsequent statements', async () => {
  let finish!: (r: typeof ok) => void
  vi.mocked(api.query).mockImplementationOnce(() => new Promise(resolve => { finish = resolve }))
  render(<DatalogModule />)
  fireEvent.click(screen.getByText('▶ Evaluate'))
  await waitFor(() => expect(finish).toBeTypeOf('function'))
  activeConnection.value = { id: 'c2', name: 'two', url: 'pg://two', isNucleus: true }
  finish(ok)
  await screen.findByText(/c1: 1 earlier fact\/rule write/)
  expect(api.query).toHaveBeenCalledTimes(1)
})
