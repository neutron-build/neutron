import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/preact'
import { activeConnection, closePalette, openPalette, paletteOpen, paletteQuery, schema } from '../lib/store'
import type { Schema } from '../lib/types'
import { CommandPalette } from './CommandPalette'

beforeEach(() => {
  closePalette()
  activeConnection.value = { id: 'palette-connection', name: 'Local', url: 'masked', isNucleus: false }
  schema.value = {
    sql: [{ schema: 'public', name: 'notes', columns: [] }],
    kv: [], vector: [], timeseries: [], document: [], graph: [], fts: [], geo: [], blob: [], pubsub: [], streams: [], columnar: [], datalog: null, cdc: false,
  } as Schema
})
afterEach(() => { cleanup(); closePalette(); paletteQuery.value = ''; activeConnection.value = null; schema.value = null })
function mountedPalette() {
  return render(<><button onClick={openPalette}>Find objects</button><CommandPalette /></>)
}

describe('command palette retained-component modal focus lifecycle', () => {
  it('names its modal and search field, traps both Tab directions and restores focus on Escape', async () => {
    mountedPalette()
    const opener = screen.getByRole('button', { name: 'Find objects' })
    opener.focus()
    fireEvent.click(opener)
    const input = await screen.findByRole('textbox', { name: 'Search database objects and commands' })
    await waitFor(() => expect(document.activeElement).toBe(input))
    const dialog = screen.getByRole('dialog', { name: 'Find database objects and commands' })
    expect(dialog.getAttribute('aria-modal')).toBe('true')
    fireEvent.keyDown(input, { key: 'Tab', shiftKey: true })
    const last = screen.getByRole('button', { name: /New SQL Query/ })
    expect(document.activeElement).toBe(last)
    fireEvent.keyDown(last, { key: 'Tab' })
    expect(document.activeElement).toBe(input)
    fireEvent.keyDown(input, { key: 'Escape' })
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull())
    await waitFor(() => expect(document.activeElement).toBe(opener))
    expect(paletteOpen.value).toBe(false)
  })

  it('focuses on every reopen while still mounted and traps an empty result set', async () => {
    mountedPalette()
    const opener = screen.getByRole('button', { name: 'Find objects' })
    opener.focus()
    fireEvent.click(opener)
    let input = await screen.findByRole('textbox', { name: 'Search database objects and commands' })
    await waitFor(() => expect(document.activeElement).toBe(input))
    fireEvent.keyDown(input, { key: 'Escape' })
    await waitFor(() => expect(document.activeElement).toBe(opener))
    fireEvent.click(opener)
    input = await screen.findByRole('textbox', { name: 'Search database objects and commands' })
    await waitFor(() => expect(document.activeElement).toBe(input))
    fireEvent.input(input, { target: { value: 'no-such-object' } })
    await screen.findByText('No results')
    fireEvent.keyDown(input, { key: 'Tab' })
    expect(document.activeElement).toBe(input)
    fireEvent.keyDown(input, { key: 'Tab', shiftKey: true })
    expect(document.activeElement).toBe(input)
  })
})


describe('command palette bounded visible matches', () => {
  it('announces truncation and narrower-search guidance while keeping at most twenty actions', async () => {
    schema.value = { ...schema.value!, sql: Array.from({ length: 30 }, (_, index) => ({ schema: 'public', name: `notes-${index}`, columns: [] })) }
    mountedPalette()
    fireEvent.click(screen.getByRole('button', { name: 'Find objects' }))
    const dialog = await screen.findByRole('dialog', { name: 'Find database objects and commands' })
    await screen.findByText('Showing first 20 of 31 matches. Narrow your search to see more.')
    expect(within(dialog).getAllByRole('button')).toHaveLength(20)
    const search = screen.getByRole('textbox', { name: 'Search database objects and commands' })
    fireEvent.input(search, { target: { value: 'notes-29' } })
    await screen.findByText('1 match')
    expect(within(dialog).getAllByRole('button')).toHaveLength(1)
    expect(within(dialog).getByRole('button', { name: /notes-29/ })).toBeTruthy()
    expect(screen.getByRole('status').getAttribute('aria-live')).toBe('polite')
  })
})
