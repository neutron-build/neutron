import { describe, it, expect, vi, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup } from '@testing-library/preact'
import { DataGrid, visibleWindow, DEFAULT_ROW_HEIGHT, OVERSCAN_ROWS, FALLBACK_VIEWPORT_HEIGHT } from './DataGrid'
import type { CellEdit, QueryResult, TableMetaColumn, TableSort } from '../lib/types'

// S06 grid tests: row virtualization (V16: visible rows + overscan in the
// DOM, whatever the result size), the WAI-ARIA keyboard model (arrows,
// PageUp/Down, Home/End, Enter/F2 editing, focus management) and the ARIA
// structure a screen reader needs (roles, names, row/col indexing, sort
// state) — asserted on the rendered component, not its inputs.

function metaColumns(): TableMetaColumn[] {
  return [
    { name: 'id', type: 'int4', tag: null, nullable: false, isKey: true, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: false, readOnlyReason: 'key column is read-only (it addresses the row)' },
    { name: 'name', type: 'text', tag: null, nullable: true, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: true },
    { name: 'total', type: 'int4', tag: null, nullable: true, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: true },
  ]
}

function result(rows: unknown[][], over: Partial<QueryResult> = {}): QueryResult {
  return { columns: ['id', 'name', 'total'], rows, rowCount: rows.length, duration: 0, ...over }
}

function bigResult(n: number): QueryResult {
  const rows: unknown[][] = []
  for (let i = 0; i < n; i++) rows.push([i, `row ${i}`, i * 2])
  return result(rows, { rowCount: n })
}

function renderedRowIndexes(): number[] {
  return Array.from(document.querySelectorAll('tr[data-row-index]'))
    .map(tr => Number((tr as HTMLElement).dataset.rowIndex))
}

function gridElement(): HTMLElement {
  const el = document.querySelector('table[role="grid"]')
  if (!el) throw new Error('grid not found')
  return el as HTMLElement
}

function cell(row: number, col: number): HTMLElement {
  const td = document.querySelector(`tr[data-row-index="${row}"] td[data-col-index="${col}"]`)
  if (!td) throw new Error(`cell ${row},${col} not rendered`)
  return td as HTMLElement
}

function scrollArea(): HTMLElement {
  const el = document.querySelector(`.${(gridElement().parentElement as HTMLElement).className.split(' ')[0]}`)
  return (gridElement().parentElement as HTMLElement) ?? el
}

afterEach(cleanup)

describe('visibleWindow math', () => {
  it('returns viewport rows plus overscan, clamped to the data', () => {
    const h = DEFAULT_ROW_HEIGHT
    expect(visibleWindow(10000, 0, FALLBACK_VIEWPORT_HEIGHT, h)).toEqual({
      start: 0,
      end: Math.ceil(FALLBACK_VIEWPORT_HEIGHT / h) + OVERSCAN_ROWS,
    })
    const mid = visibleWindow(10000, 28 * 500, 600, h)
    expect(mid.start).toBe(500 - OVERSCAN_ROWS)
    expect(mid.end).toBe(500 + Math.ceil(600 / h) + OVERSCAN_ROWS)
    // Out-of-range scroll positions (a caller bug; the spacer rows bound
    // real scrolling) yield an EMPTY window, never an inverted one.
    expect(visibleWindow(5, 10000, 600, h)).toEqual({ start: 347, end: 347 })
    expect(visibleWindow(0, 0, 600, h)).toEqual({ start: 0, end: 0 })
  })
  it('degenerate inputs fall back to the default row height and non-negative windows', () => {
    expect(visibleWindow(100, 0, 600, 0)).toEqual(visibleWindow(100, 0, 600, DEFAULT_ROW_HEIGHT))
    expect(visibleWindow(100, -50, -1, DEFAULT_ROW_HEIGHT)).toEqual({ start: 0, end: OVERSCAN_ROWS })
  })
})

describe('virtualized rendering: DOM size is bounded by the viewport (V16)', () => {
  it('a 10k-row result renders only the visible window plus overscan', () => {
    render(<DataGrid result={bigResult(10000)} label="Query result" />)
    const rendered = renderedRowIndexes()
    const expected = Math.ceil(FALLBACK_VIEWPORT_HEIGHT / DEFAULT_ROW_HEIGHT) + OVERSCAN_ROWS
    expect(rendered).toHaveLength(expected)
    expect(rendered[0]).toBe(0)
    expect(rendered[rendered.length - 1]).toBe(expected - 1)
    // the grid announces the true size
    expect(gridElement().getAttribute('aria-rowcount')).toBe('10001')
  })

  it('scrolling moves the rendered window; spacers keep rows addressable', () => {
    render(<DataGrid result={bigResult(10000)} />)
    const area = scrollArea()
    area.scrollTop = DEFAULT_ROW_HEIGHT * 2000
    fireEvent.scroll(area)
    const rendered = renderedRowIndexes()
    expect(rendered[0]).toBe(2000 - OVERSCAN_ROWS)
    expect(rendered).toHaveLength(Math.ceil(FALLBACK_VIEWPORT_HEIGHT / DEFAULT_ROW_HEIGHT) + 2 * OVERSCAN_ROWS)
    // spacer rows exist above (and below), marked presentation-only
    const spacers = Array.from(document.querySelectorAll('tbody tr[aria-hidden="true"]'))
    expect(spacers.length).toBeGreaterThan(0)
    // absolute row indexes stay correct for the visible slice
    expect(cell(2000, 0).textContent).toContain('2000')
  })

  it('aria-rowindex is absolute (header 1, data from 2) for the rendered slice', () => {
    render(<DataGrid result={bigResult(10000)} />)
    const area = scrollArea()
    area.scrollTop = DEFAULT_ROW_HEIGHT * 100
    fireEvent.scroll(area)
    const first = renderedRowIndexes()[0]
    const tr = document.querySelector(`tr[data-row-index="${first}"]`)
    expect(tr!.getAttribute('aria-rowindex')).toBe(String(first + 2))
    expect(document.querySelector('thead tr')!.getAttribute('aria-rowindex')).toBe('1')
  })
})

describe('grid ARIA structure', () => {
  it('roles and accessible names: grid, columnheader scope, gridcell, labelled', () => {
    render(<DataGrid result={result([[1, 'a', null]])} label="public.users rows" />)
    expect(gridElement().getAttribute('aria-label')).toBe('public.users rows')
    expect(gridElement().getAttribute('aria-colcount')).toBe('3')
    const cells = document.querySelectorAll('td[role="gridcell"]')
    expect(cells).toHaveLength(3)
    expect(document.querySelectorAll('th[scope="col"]')).toHaveLength(3)
  })

  it('aria-sort reflects the primary sort column, both directions, absent otherwise', () => {
    const onSort = vi.fn()
    const { rerender } = render(<DataGrid result={result([[1, 'a', 2]])} sorts={[{ column: 'name', dir: 'desc' } as TableSort]} onSort={onSort} />)
    const nameHeader = Array.from(document.querySelectorAll('th')).find(th => th.textContent!.includes('name'))!
    expect(nameHeader.getAttribute('aria-sort')).toBe('descending')
    rerender(<DataGrid result={result([[1, 'a', 2]])} sorts={[{ column: 'name', dir: 'asc' } as TableSort]} onSort={onSort} />)
    expect(Array.from(document.querySelectorAll('th')).find(th => th.textContent!.includes('name'))!.getAttribute('aria-sort')).toBe('ascending')
    rerender(<DataGrid result={result([[1, 'a', 2]])} onSort={onSort} />)
    expect(Array.from(document.querySelectorAll('th')).find(th => th.textContent!.includes('name'))!.getAttribute('aria-sort')).toBeNull()
  })

  it('sort headers are named buttons announcing current state; click and shift-click sort', () => {
    const onSort = vi.fn()
    render(<DataGrid result={result([[1, 'a', 2]])} onSort={onSort} />)
    const btn = screen.getByRole('button', { name: /^Sort by name/ }) as HTMLElement
    btn.focus()
    fireEvent.click(btn)
    expect(onSort).toHaveBeenCalledWith('name', false)
    fireEvent.click(btn, { shiftKey: true })
    expect(onSort).toHaveBeenLastCalledWith('name', true)
  })

  it('gridcells on editable grids expose aria-readonly for key columns', () => {
    render(<DataGrid result={result([[1, 'a', 2]])} columns={metaColumns()} onStageUpdate={vi.fn()} />)
    expect(cell(0, 0).getAttribute('aria-readonly')).toBe('true')
    expect(cell(0, 1).getAttribute('aria-readonly')).toBe('false')
  })
})

describe('roving tabindex keyboard model', () => {
  it('exactly one grid cell is tabbable; it moves with the active cell', () => {
    render(<DataGrid result={result([[1, 'a', 2], [2, 'b', 4]])} />)
    const tabbable = () => Array.from(document.querySelectorAll('td[tabindex="0"]'))
    expect(tabbable()).toHaveLength(1)
    expect(tabbable()[0]).toBe(cell(0, 0))
    fireEvent.keyDown(gridElement(), { key: 'ArrowDown' })
    expect(tabbable()[0]).toBe(cell(1, 0))
    fireEvent.keyDown(gridElement(), { key: 'ArrowRight' })
    expect(tabbable()[0]).toBe(cell(1, 1))
  })

  it('arrow keys move focus and clamp at the edges', () => {
    render(<DataGrid result={result([[1, 'a', 2], [2, 'b', 4]])} />)
    fireEvent.focus(cell(0, 0))
    fireEvent.keyDown(gridElement(), { key: 'ArrowUp' })
    fireEvent.keyDown(gridElement(), { key: 'ArrowLeft' })
    expect(document.activeElement).toBe(cell(0, 0))
    fireEvent.keyDown(gridElement(), { key: 'ArrowDown' })
    expect(document.activeElement).toBe(cell(1, 0))
    fireEvent.keyDown(gridElement(), { key: 'ArrowRight' })
    fireEvent.keyDown(gridElement(), { key: 'ArrowRight' })
    fireEvent.keyDown(gridElement(), { key: 'ArrowRight' })
    expect(document.activeElement).toBe(cell(1, 2))
  })

  it('PageDown/PageEnd/Home navigate by viewport and to the ends', () => {
    render(<DataGrid result={bigResult(200)} />)
    fireEvent.focus(cell(0, 0))
    fireEvent.keyDown(gridElement(), { key: 'PageDown' })
    // viewport rows at default height = floor(600/28)-1 = 20
    expect(document.activeElement).toBe(cell(20, 0))
    fireEvent.keyDown(gridElement(), { key: 'End', ctrlKey: true })
    expect(document.activeElement).toBe(cell(199, 2))
    fireEvent.keyDown(gridElement(), { key: 'Home' })
    expect(document.activeElement).toBe(cell(199, 0))
    fireEvent.keyDown(gridElement(), { key: 'Home', ctrlKey: true })
    expect(document.activeElement).toBe(cell(0, 0))
  })

  it('Enter on an editable cell opens the editor; Escape returns focus to the cell', () => {
    render(<DataGrid result={result([[1, 'a', 2]])} columns={metaColumns()} onStageUpdate={vi.fn()} />)
    fireEvent.focus(cell(0, 1))
    fireEvent.keyDown(gridElement(), { key: 'Enter' })
    const input = document.querySelector('input[aria-label$=" value"]') as HTMLInputElement
    expect(input).not.toBeNull()
    expect(document.activeElement).toBe(input)
    fireEvent.keyDown(input, { key: 'Escape' })
    expect(document.querySelector('input[aria-label$=" value"]')).toBeNull()
    expect(document.activeElement).toBe(cell(0, 1))
  })

  it('F2 edits, Enter commits and keeps focus on the cell', () => {
    const onStageUpdate = vi.fn()
    render(<DataGrid result={result([[1, 'a', 2]])} columns={metaColumns()} onStageUpdate={onStageUpdate} />)
    fireEvent.focus(cell(0, 2))
    fireEvent.keyDown(gridElement(), { key: 'F2' })
    const input = document.querySelector('input[aria-label$=" value"]') as HTMLInputElement
    fireEvent.input(input, { target: { value: '42' } })
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(onStageUpdate).toHaveBeenCalledWith(0, 'total', { kind: 'value', text: '42' } as CellEdit)
    expect(document.activeElement).toBe(cell(0, 2))
  })

  it('Enter on a read-only key cell does nothing; Delete stages a row delete on editable grids', () => {
    const onStageDelete = vi.fn()
    render(<DataGrid result={result([[1, 'a', 2]])} columns={metaColumns()} onStageUpdate={vi.fn()} onStageDelete={onStageDelete} canDelete />)
    fireEvent.focus(cell(0, 0))
    fireEvent.keyDown(gridElement(), { key: 'Enter' })
    expect(document.querySelector('input[aria-label$=" value"]')).toBeNull()
    fireEvent.keyDown(gridElement(), { key: 'Delete' })
    expect(onStageDelete).toHaveBeenCalledWith(0)
  })

  it('keyboard movement scrolls far rows into the rendered window before focusing them', () => {
    render(<DataGrid result={bigResult(5000)} />)
    fireEvent.focus(cell(0, 0))
    fireEvent.keyDown(gridElement(), { key: 'End', ctrlKey: true })
    // the last row now exists in the DOM and holds focus
    expect(document.activeElement).toBe(cell(4999, 2))
    expect(renderedRowIndexes()).toContain(4999)
  })
})

describe('truncation notice and in-grid export', () => {
  it('a truncated result says so and names the limit', () => {
    render(<DataGrid result={result([[1, 'a', 2]], { truncated: true, rowLimit: 10000 })} exportName="query-result" />)
    const note = screen.getByRole('status')
    expect(note.textContent).toContain('truncated')
    expect(note.textContent).toContain('10,000')
  })

  it('the editor grid exports its fetched rows; table grids without exportName do not', () => {
    render(<DataGrid result={result([[1, 'a', 2]])} exportName="query-result" />)
    expect(screen.getByTitle(/^Download these rows as CSV/)).toBeTruthy()
    expect(screen.getByTitle(/^Download these rows as JSON/)).toBeTruthy()
    cleanup()
    render(<DataGrid result={result([[1, 'a', 2]])} />)
    expect(screen.queryByTitle(/^Download these rows as CSV/)).toBeNull()
  })
})
