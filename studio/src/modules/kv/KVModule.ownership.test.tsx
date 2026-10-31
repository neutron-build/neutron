import { beforeEach, afterEach, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import { activeConnection } from '../../lib/store'
import { KVModule } from './KVModule'
vi.mock('../../lib/api', async original => { const m = await original<typeof import('../../lib/api')>(); return { ...m, api: { ...m.api, query: vi.fn() } } })
import { api } from '../../lib/api'
const ok = (cell: unknown) => ({ columns: ['v'], rows: [[cell]], rowCount: 1, duration: 0 })
const connect = (id: string) => { activeConnection.value = { id, name: id, url: 'pg://test', isNucleus: true } }
beforeEach(() => {
  connect('c1'); vi.mocked(api.query).mockReset()
  vi.mocked(api.query).mockImplementation(async (sql, id) => sql.includes('KV_KEYS') ? ok(['same-key']) : sql.includes('KV_GET') ? { ...ok(''), rows: [[`value-${id}`, -1]] } : ok('deleted'))
})
afterEach(cleanup)
it('c1 confirmation cannot confirm c2; a separately confirmed c2 delete targets c2', async () => {
  render(<KVModule name="kv" />)
  await screen.findByText('value-c1')
  fireEvent.click(screen.getByTitle('Delete key'))
  connect('c2')
  await screen.findByText('value-c2')
  expect(screen.queryByTitle('Click again to confirm')).toBeNull()
  fireEvent.click(screen.getByTitle('Delete key'))
  expect(vi.mocked(api.query).mock.calls.filter(([sql]) => sql.includes('KV_DEL'))).toHaveLength(0)
  fireEvent.click(screen.getByTitle('Click again to confirm'))
  await waitFor(() => expect(vi.mocked(api.query).mock.calls.filter(([sql]) => sql.includes('KV_DEL'))).toEqual([["SELECT KV_DEL('same-key')", 'c2', undefined]]))
})
it('late c1 enumeration cannot publish or fetch values through c2', async () => {
  let finish!: (r: ReturnType<typeof ok>) => void
  vi.mocked(api.query).mockImplementationOnce(() => new Promise(resolve => { finish = resolve }))
  render(<KVModule name="kv" />)
  await waitFor(() => expect(finish).toBeTypeOf('function'))
  connect('c2'); await screen.findByText('value-c2')
  finish(ok(['stale-key']))
  await new Promise(resolve => setTimeout(resolve, 0))
  await waitFor(() => expect(screen.queryByText('stale-key')).toBeNull())
  expect(vi.mocked(api.query).mock.calls.some(([sql]) => sql.includes("KV_GET('stale-key')"))).toBe(false)
})
it.each(['c2 selection', 'same key reopened', 'same selection edited'])('deferred delete preserves newer %s', async next => {
  let finish!: (r: ReturnType<typeof ok>) => void
  const ordinary = vi.mocked(api.query).getMockImplementation()!
  vi.mocked(api.query).mockImplementation((sql, id, params, requestId) => sql.includes('KV_DEL') ? new Promise(resolve => { finish = resolve }) : ordinary(sql, id, params, requestId))
  render(<KVModule name="kv" />)
  fireEvent.click(await screen.findByText('same-key'))
  fireEvent.click(screen.getByTitle('Delete key')); fireEvent.click(screen.getByTitle('Click again to confirm'))
  await waitFor(() => expect(finish).toBeTypeOf('function'))
  if (next === 'c2 selection') { connect('c2'); await screen.findByText('value-c2'); fireEvent.click(screen.getByText('same-key')) }
  else if (next === 'same key reopened') fireEvent.click(document.querySelector('span[class*="keyName"]')!)
  const textarea = document.querySelector('textarea')!
  fireEvent.input(textarea, { target: { value: 'newer-draft' } })
  finish(ok('deleted'))
  await new Promise(resolve => setTimeout(resolve, 0))
  await waitFor(() => expect(screen.getByDisplayValue('newer-draft')).toBeTruthy())
  expect(vi.mocked(api.query).mock.calls.filter(([sql]) => sql.includes('KV_DEL'))[0][1]).toBe('c1')
})
