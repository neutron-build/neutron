import { describe, it, expect, vi, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup } from '@testing-library/preact'
import { DataGrid } from './DataGrid'
import type { QueryResult } from '../lib/types'

// Rendered-component tests for the B04 edit containment: empty string and
// SQL NULL are distinct controls, the key column is read-only, and grids
// without a single-column PK offer no edit affordances at all.

function gridResult(): QueryResult {
  return {
    columns: ['id', 'name'],
    rows: [[1, 'Alice'], [2, null]],
    rowCount: 2,
    duration: 0,
  }
}

function cellAt(row: number, col: string): HTMLElement {
  const headerCells = Array.from(document.querySelectorAll('th'))
  const colIdx = headerCells.findIndex(th => th.textContent?.includes(col))
  const rows = Array.from(document.querySelectorAll('tbody tr'))
  return rows[row].children[colIdx] as HTMLElement
}

afterEach(cleanup)

describe('DataGrid editing: empty string vs NULL stay distinct', () => {
  it('committing cleared text sends an empty string, not null', () => {
    const onCommitEdit = vi.fn()
    render(<DataGrid result={gridResult()} pkColumn="id" onCommitEdit={onCommitEdit} />)

    fireEvent.dblClick(cellAt(0, 'name'))
    const input = screen.getByRole('textbox') as HTMLInputElement
    fireEvent.input(input, { target: { value: '' } })
    fireEvent.keyDown(input, { key: 'Enter' })

    expect(onCommitEdit).toHaveBeenCalledWith(0, 'name', '')
    expect(onCommitEdit.mock.calls[0][2]).not.toBe(null)
  })

  it('committing text sends the text', () => {
    const onCommitEdit = vi.fn()
    render(<DataGrid result={gridResult()} pkColumn="id" onCommitEdit={onCommitEdit} />)

    fireEvent.dblClick(cellAt(0, 'name'))
    const input = screen.getByRole('textbox') as HTMLInputElement
    fireEvent.input(input, { target: { value: 'Bob' } })
    fireEvent.keyDown(input, { key: 'Enter' })

    expect(onCommitEdit).toHaveBeenCalledWith(0, 'name', 'Bob')
  })

  it('unchanged text is a no-op (no commit)', () => {
    const onCommitEdit = vi.fn()
    render(<DataGrid result={gridResult()} pkColumn="id" onCommitEdit={onCommitEdit} />)

    fireEvent.dblClick(cellAt(0, 'name'))
    fireEvent.keyDown(screen.getByRole('textbox'), { key: 'Enter' })

    expect(onCommitEdit).not.toHaveBeenCalled()
  })

  it('a NULL cell opens with the NULL control checked and Enter is a no-op', () => {
    const onCommitEdit = vi.fn()
    render(<DataGrid result={gridResult()} pkColumn="id" onCommitEdit={onCommitEdit} />)

    fireEvent.click(cellAt(1, 'name').querySelector('button')!)
    const nullToggle = screen.getByRole('checkbox') as HTMLInputElement
    expect(nullToggle.checked).toBe(true)
    const input = screen.getByRole('textbox') as HTMLInputElement
    expect(input.disabled).toBe(true)

    fireEvent.keyDown(input, { key: 'Enter' })
    expect(onCommitEdit).not.toHaveBeenCalled()
  })

  it('unchecking NULL on a NULL cell commits an explicit empty string', () => {
    const onCommitEdit = vi.fn()
    render(<DataGrid result={gridResult()} pkColumn="id" onCommitEdit={onCommitEdit} />)

    fireEvent.click(cellAt(1, 'name').querySelector('button')!)
    const nullToggle = screen.getByRole('checkbox')
    fireEvent.click(nullToggle)
    fireEvent.keyDown(screen.getByRole('textbox'), { key: 'Enter' })

    expect(onCommitEdit).toHaveBeenCalledWith(1, 'name', '')
  })

  it('checking the NULL control commits SQL null', () => {
    const onCommitEdit = vi.fn()
    render(<DataGrid result={gridResult()} pkColumn="id" onCommitEdit={onCommitEdit} />)

    fireEvent.dblClick(cellAt(0, 'name'))
    const nullToggle = screen.getByRole('checkbox')
    fireEvent.click(nullToggle)
    fireEvent.keyDown(screen.getByRole('textbox'), { key: 'Enter' })

    expect(onCommitEdit).toHaveBeenCalledWith(0, 'name', null)
  })

  it('Escape cancels without committing', () => {
    const onCommitEdit = vi.fn()
    render(<DataGrid result={gridResult()} pkColumn="id" onCommitEdit={onCommitEdit} />)

    fireEvent.dblClick(cellAt(0, 'name'))
    const input = screen.getByRole('textbox') as HTMLInputElement
    fireEvent.input(input, { target: { value: 'changed' } })
    fireEvent.keyDown(input, { key: 'Escape' })

    expect(onCommitEdit).not.toHaveBeenCalled()
    expect(screen.queryByRole('textbox')).toBeNull()
  })
})

describe('DataGrid edit affordances are contained', () => {
  it('offers no editing without a pkColumn (composite/no-key tables)', () => {
    const onCommitEdit = vi.fn()
    render(<DataGrid result={gridResult()} onCommitEdit={onCommitEdit} />)

    expect(screen.queryByText(/double-click a cell to edit/)).toBeNull()
    fireEvent.dblClick(cellAt(0, 'name'))
    expect(screen.queryByRole('textbox')).toBeNull()
    expect(onCommitEdit).not.toHaveBeenCalled()

    // NULL cells render as plain text, not as a set-value button
    const nullCell = cellAt(1, 'name')
    expect(nullCell.querySelector('button')).toBeNull()
    expect(nullCell.textContent).toBe('NULL')
  })

  it('the primary key column is read-only even when editing is enabled', () => {
    const onCommitEdit = vi.fn()
    render(<DataGrid result={gridResult()} pkColumn="id" onCommitEdit={onCommitEdit} />)

    const idCell = cellAt(0, 'id')
    expect(idCell.getAttribute('title')).toBe('Primary key column is read-only')
    fireEvent.dblClick(idCell)
    expect(screen.queryByRole('textbox')).toBeNull()

    // non-key cells still open the editor
    fireEvent.dblClick(cellAt(0, 'name'))
    expect(screen.getByRole('textbox')).toBeDefined()
  })
})
