import { describe, it, expect, vi, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import { connections } from '../lib/store'
import { ConnectionManager } from './ConnectionManager'

// R02 onboarding: a fresh user's first screen. The add-connection fields
// are reachable by their visible labels (screen readers, getByLabel);
// before, the <label>s named nothing.

vi.mock('../lib/api', async (importOriginal) => {
  const orig = await importOriginal<typeof import('../lib/api')>()
  return {
    ...orig,
    api: {
      ...orig.api,
      connections: { ...orig.api.connections, list: vi.fn(async () => []), add: vi.fn() },
    },
  }
})

import { api } from '../lib/api'

afterEach(() => {
  cleanup()
  connections.value = []
})

describe('ConnectionManager add form', () => {
  it('labels the name and URL fields and saves what was typed', async () => {
    const add = vi.mocked(api.connections.add)
    add.mockResolvedValue({ id: 'c1', name: 'local', url: 'postgres://u@h/db', isNucleus: false })
    render(<ConnectionManager />)
    fireEvent.click(screen.getByRole('button', { name: '+ Add' }))

    const name = screen.getByLabelText('Name') as HTMLInputElement
    const url = screen.getByLabelText('Connection URL') as HTMLInputElement
    expect(url.type).toBe('password')
    fireEvent.input(name, { target: { value: 'local' } })
    fireEvent.input(url, { target: { value: 'postgres://u@h/db' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))

    await waitFor(() => expect(add).toHaveBeenCalledWith({ name: 'local', url: 'postgres://u@h/db' }))
    await waitFor(() => expect(screen.getByText('local')).toBeTruthy())
  })
})
