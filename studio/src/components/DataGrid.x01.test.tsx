import { describe, it, expect, vi, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup } from '@testing-library/preact'
import { DataGrid } from './DataGrid'
import { TableSearchPanel } from '../modules/sql/TableSearchPanel'
import { api } from '../lib/api'
import { activeConnection } from '../lib/store'
import type { QueryResult, TableMeta, TableMetaColumn } from '../lib/types'

// X01: vector/tsvector cells render read-only with a copy affordance (they
// are not row-editable values), and the table search panel drives the
// server's parameter-bound vector/FTS search endpoint.

afterEach(cleanup)

function vecMeta(): TableMetaColumn[] {
  return [
    { name: 'id', type: 'int4', tag: null, nullable: false, isKey: true, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: false },
    { name: 'embedding', type: 'vector', tag: 'vector', nullable: true, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: false, readOnlyReason: 'vector columns are not row-editable values (read-only render; use the table\'s vector search or the SQL editor)' },
    { name: 'keywords', type: 'tsvector', tag: 'tsvector', nullable: true, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: false, readOnlyReason: 'tsvector columns are read-only in the editor' },
    { name: 'label', type: 'text', tag: null, nullable: true, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: true },
  ]
}

describe('X01 vector/tsvector cells: read-only render with copy', () => {
  it('renders the exact text form with a copy button and never opens an editor', () => {
    const onStageUpdate = vi.fn()
    const result: QueryResult = {
      columns: ['id', 'embedding', 'keywords', 'label'],
      rows: [[1, '[0.1,0.2,0.3]', "'cat' 'sat'", 'plain']],
      rowCount: 1,
      duration: 0,
    }
    render(<DataGrid result={result} columns={vecMeta()} onStageUpdate={onStageUpdate} />)

    expect(screen.getByText('[0.1,0.2,0.3]')).toBeTruthy()
    expect(screen.getByText("'cat' 'sat'")).toBeTruthy()
    const copyVec = screen.getByTitle('Copy vector value') as HTMLButtonElement
    const copyTsv = screen.getByTitle('Copy tsvector value') as HTMLButtonElement
    expect(copyVec).toBeTruthy()
    expect(copyTsv).toBeTruthy()

    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    fireEvent.click(copyVec)
    expect(writeText).toHaveBeenCalledWith('[0.1,0.2,0.3]')
    fireEvent.click(copyTsv)
    expect(writeText).toHaveBeenCalledWith("'cat' 'sat'")

    // Double-click on the vector cell never opens an editor (it is not
    // row-editable); double-click on the text column does.
    const headerCells = Array.from(document.querySelectorAll('th'))
    const vecIdx = headerCells.findIndex(th => th.textContent?.includes('embedding'))
    const labelIdx = headerCells.findIndex(th => th.textContent?.includes('label'))
    const row = document.querySelector('tbody tr') as HTMLElement
    fireEvent.dblClick(row.children[vecIdx] as HTMLElement)
    expect(document.querySelector('input[aria-label$=" value"]')).toBeNull()
    fireEvent.dblClick(row.children[labelIdx] as HTMLElement)
    expect(document.querySelector('input[aria-label$=" value"]')).not.toBeNull()
  })
})

describe('X01 table search panel', () => {
  const meta = (): TableMeta => ({
    exists: true,
    keyColumns: ['id'],
    versioned: true,
    readOnly: false,
    canDelete: true,
    columns: vecMeta(),
  })

  it('renders vector controls when a vector column exists and runs the search', async () => {
    activeConnection.value = { id: 'c1' } as never
    const search = vi.fn().mockResolvedValue({ columns: ['id', 'embedding'], rows: [[1, '[1,2,3]']], rowCount: 1, duration: 2 })
    vi.spyOn(api, 'tableSearch').mockImplementation(search)

    render(<TableSearchPanel schema="public" table="x01_items" meta={meta()} />)

    const input = screen.getByPlaceholderText('query vector, e.g. [0.1, 0.2, 0.3]') as HTMLInputElement
    expect(input).toBeTruthy()
    fireEvent.input(input, { target: { value: '[0.95,0.05,0]' } })
    fireEvent.click(screen.getByText('Search'))

    await vi.waitFor(() => {
      expect(search).toHaveBeenCalledWith(expect.objectContaining({
        kind: 'vector',
        column: 'embedding',
        operator: 'cosine',
        query: '[0.95,0.05,0]',
        limit: 20,
      }))
    })
    expect(await screen.findByText('1 result')).toBeTruthy()
  })

  it('surfaces the server error honestly (no extension, wrong dimension)', async () => {
    activeConnection.value = { id: 'c1' } as never
    vi.spyOn(api, 'tableSearch').mockRejectedValue(new Error('type "vector" does not exist'))
    render(<TableSearchPanel schema="public" table="x01_items" meta={meta()} />)

    const input = screen.getByPlaceholderText('query vector, e.g. [0.1, 0.2, 0.3]') as HTMLInputElement
    fireEvent.input(input, { target: { value: '[1,2,3]' } })
    fireEvent.click(screen.getByText('Search'))

    expect(await screen.findByText(/does not exist/)).toBeTruthy()
  })

  it('hides entirely when no searchable column exists', () => {
    const plain = meta()
    plain.columns = plain.columns.filter(c => c.name === 'id')
    const { container } = render(<TableSearchPanel schema="public" table="t" meta={plain} />)
    expect(container.querySelector('select')).toBeNull()
  })
})
