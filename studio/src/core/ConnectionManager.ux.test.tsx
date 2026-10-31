import { it, expect, vi, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import { connections, connectionError } from '../lib/store'
import { ConnectionManager } from './ConnectionManager'
vi.mock('../lib/api', async importOriginal => {
  const original = await importOriginal<typeof import('../lib/api')>()
  return { ...original, api: { ...original.api, connections: { ...original.api.connections, list: vi.fn(), add: vi.fn() } } }
})
import { api } from '../lib/api'
afterEach(() => { cleanup(); connections.value = []; connectionError.value = null; vi.clearAllMocks() })
it('recovers a failed saved-connection load without hiding the failure as an empty list', async () => {
  vi.mocked(api.connections.list).mockRejectedValueOnce(new Error('service unavailable')).mockResolvedValueOnce([{ id: 'c1', name: 'recovered', url: 'postgres://u@h/db', isNucleus: false }])
  render(<ConnectionManager />)
  await waitFor(() => expect(screen.getByRole('alert').textContent).toContain('service unavailable'))
  expect(screen.queryByText('A workspace starts with a connection')).toBeNull()
  fireEvent.click(screen.getByRole('button', { name: 'Try again' }))
  await waitFor(() => expect(screen.getByText('recovered')).toBeTruthy())
  expect(api.connections.list).toHaveBeenCalledTimes(2)
})
it('keeps a failed save draft and prevents repeated submissions while saving', async () => {
  vi.mocked(api.connections.list).mockResolvedValue([])
  let reject!: (error: Error) => void
  vi.mocked(api.connections.add).mockImplementation(() => new Promise((_, no) => { reject = no }))
  render(<ConnectionManager />)
  fireEvent.click(screen.getByRole('button', { name: '+ Add' }))
  fireEvent.input(screen.getByLabelText('Name'), { target: { value: 'local' } })
  fireEvent.input(screen.getByLabelText('Connection URL'), { target: { value: 'postgres://u@h/db' } })
  const form = screen.getByLabelText('Name').closest('form')!
  fireEvent.submit(form); fireEvent.submit(form)
  await waitFor(() => expect((screen.getByRole('button', { name: 'Saving…' }) as HTMLButtonElement).disabled).toBe(true))
  expect(api.connections.add).toHaveBeenCalledTimes(1)
  reject(new Error('save refused'))
  await waitFor(() => expect(screen.getByRole('alert').textContent).toContain('save refused'))
  expect((screen.getByLabelText('Name') as HTMLInputElement).value).toBe('local')
  expect((screen.getByLabelText('Connection URL') as HTMLInputElement).value).toBe('postgres://u@h/db')
  expect((screen.getByRole('button', { name: 'Save' }) as HTMLButtonElement).disabled).toBe(false)
})
