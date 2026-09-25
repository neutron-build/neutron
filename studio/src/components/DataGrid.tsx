import { useEffect, useLayoutEffect, useRef, useState } from 'preact/hooks'
import type { CellEdit, QueryResult, TableMetaColumn, TableSort } from '../lib/types'
import { formatCell } from '../lib/wire'
import { toast, limitsFor } from '../lib/store'
import { exportResultCSV, exportResultJSON } from '../lib/export'
import { TypedEditor } from './TypedEditor'
import s from './DataGrid.module.css'

// Row virtualization (S06): only the rows in the scroll viewport plus a
// fixed overscan are in the DOM, whatever the result size; spacer rows keep
// the scroll height exact. Rows have a fixed height (DataGrid.module.css),
// measured from the first rendered row when layout exists.
export const DEFAULT_ROW_HEIGHT = 28
export const OVERSCAN_ROWS = 10
/** Viewport height assumed before layout exists (tests, hidden tabs). */
export const FALLBACK_VIEWPORT_HEIGHT = 600

/** The rendered row window for a scroll position: [start, end). */
export function visibleWindow(rowCount: number, scrollTop: number, viewportHeight: number, rowHeight: number, overscan = OVERSCAN_ROWS): { start: number; end: number } {
  const h = rowHeight > 0 ? rowHeight : DEFAULT_ROW_HEIGHT
  const first = Math.floor(Math.max(0, scrollTop) / h)
  const count = Math.ceil(Math.max(0, viewportHeight) / h)
  const start = Math.max(0, first - overscan)
  const end = Math.min(rowCount, first + count + overscan)
  return { start, end: Math.max(start, end) }
}

export interface FKTarget {
  refSchema: string
  refTable: string
  refColumn?: string
  refColumns?: string[]
  /** Local columns of the constraint, in constraint order (the whole tuple). */
  columns?: string[]
}

/** Local tuple columns of an FK (single-column FKs may only name `column`). */
function fkLocalColumns(fk: FKTarget, fallback: string): string[] {
  return fk.columns && fk.columns.length > 0 ? fk.columns : [fallback]
}

/** One staged cell displayed over the committed value. */
export interface StagedCell {
  editId: string
  edit: CellEdit
}

/** A row's staged state: cell updates keyed by column, plus a staged delete. */
export interface StagedRowState {
  updates: Record<string, StagedCell>
  deleteId?: string
}

export function stagedCellText(edit: CellEdit): string {
  if (edit.kind === 'null') return 'NULL'
  if (edit.kind === 'default') return 'DEFAULT'
  return edit.text
}

interface DataGridProps {
  result: QueryResult
  /** Authoritative column metadata; columns that are not editable are rendered read-only. */
  columns?: TableMetaColumn[]
  /** Stage a cell edit (S03): edits join the atomic draft, never auto-commit.
   *  The parent addresses the row by its full-key identity at stage time. */
  onStageUpdate?: (rowIndex: number, column: string, edit: CellEdit) => void
  /** Stage a row delete. */
  onStageDelete?: (rowIndex: number) => void
  /** Whether the server reports DELETE privilege for this table. */
  canDelete?: boolean
  /** Staged state per row index: overlay + disabled re-delete. */
  stagedRows?: Map<number, StagedRowState>
  /** FK columns by name; enables follow links. */
  fkColumns?: Record<string, FKTarget>
  /** Follow a reference; the whole FK tuple is read from the given row. */
  onFollowFK?: (fk: FKTarget, rowIndex: number) => void
  /** Ordered multi-sort keys; header clicks rebuild this list. */
  sorts?: TableSort[]
  /** plain click: replace the sort with this column (asc -> desc -> off);
   *  shift-click: toggle the column inside the multi-sort list. */
  onSort?: (column: string, additive: boolean) => void
  /** Stable row key strings (data attributes + focus targets). */
  rowKeys?: string[]
  /** Row/cell the parent wants focused after a failed commit. The parent
   *  clears it (the highlight lingers for the user); the grid only focuses. */
  focusCell?: { rowIndex: number; column?: string } | null
  /** Accessible name of the grid. */
  label?: string
  /** Offer CSV/JSON export of the fetched rows under this file name stem
   *  (the SQL editor's results; tables use the streamed server export). */
  exportName?: string
}

interface EditState {
  row: number
  col: string
  edit: CellEdit
  initial: CellEdit
}

function sameEdit(a: CellEdit, b: CellEdit): boolean {
  if (a.kind !== b.kind) return false
  if (a.kind === 'value' && b.kind === 'value') return a.text === b.text
  return true
}

export function DataGrid({
  result,
  columns,
  onStageUpdate,
  onStageDelete,
  canDelete,
  stagedRows,
  fkColumns,
  onFollowFK,
  sorts,
  onSort,
  rowKeys,
  focusCell,
  label,
  exportName,
}: DataGridProps) {
  const [edit, setEdit] = useState<EditState | null>(null)
  const rowRefs = useRef<Map<number, HTMLTableRowElement>>(new Map())
  const scrollRef = useRef<HTMLDivElement | null>(null)
  const headRef = useRef<HTMLTableSectionElement | null>(null)
  const [scrollTop, setScrollTop] = useState(0)
  const [viewportH, setViewportH] = useState(FALLBACK_VIEWPORT_HEIGHT)
  const [rowH, setRowH] = useState(DEFAULT_ROW_HEIGHT)
  // Keyboard grid (S06): one active cell (roving tabindex). focusSeq bumps
  // when the active cell must take DOM focus (keyboard moves, editor close).
  const [active, setActive] = useState<{ row: number; col: number }>({ row: 0, col: 0 })
  const [focusSeq, setFocusSeq] = useState(0)
  const editCellRef = useRef<HTMLTableCellElement | null>(null)
  const rowCount = result.rows.length
  const colCount = result.columns.length

  // Viewport measurement: the scroll area's height when laid out (fallback
  // otherwise), kept current on resize.
  useLayoutEffect(() => {
    const el = scrollRef.current
    if (!el) return
    const measure = () => { if (el.clientHeight > 0) setViewportH(el.clientHeight) }
    measure()
    if (typeof ResizeObserver === 'undefined') return
    const ro = new ResizeObserver(measure)
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  // Row height from the first rendered data row (fixed by CSS).
  useLayoutEffect(() => {
    const first = rowRefs.current.values().next().value as HTMLTableRowElement | undefined
    const h = first?.offsetHeight ?? 0
    if (h > 0 && Math.abs(h - rowH) > 0.5 && !edit) setRowH(h)
  })

  // A different result resets the active cell into range.
  useEffect(() => {
    setActive(a => ({ row: Math.min(a.row, Math.max(0, rowCount - 1)), col: Math.min(a.col, Math.max(0, colCount - 1)) }))
  }, [rowCount, colCount])

  /** Scroll so the given row is inside the viewport (below the sticky header). */
  function revealRow(row: number) {
    const el = scrollRef.current
    if (!el) return
    const headerH = headRef.current?.offsetHeight || rowH
    const top = row * rowH
    const bottom = top + rowH
    const view = el.clientHeight > 0 ? el.clientHeight : viewportH
    let next = el.scrollTop
    if (top < next) next = top
    else if (bottom + headerH > next + view) next = bottom + headerH - view
    if (next !== el.scrollTop) {
      el.scrollTop = next
      setScrollTop(next)
    }
  }

  // Error focus (S03): land on the first offending row/cell after a failed
  // commit so the cause is on screen. The parent clears focusCell (the
  // highlight lingers); each new focus object re-triggers the focus. With
  // virtualization the row may be outside the rendered window: scroll it in
  // first, then focus it once rendered.
  const pendingRowFocus = useRef<number | null>(null)
  useEffect(() => {
    if (!focusCell) return
    const colIdx = focusCell.column ? result.columns.indexOf(focusCell.column) : -1
    setActive({ row: focusCell.rowIndex, col: colIdx >= 0 ? colIdx : 0 })
    revealRow(focusCell.rowIndex)
    pendingRowFocus.current = focusCell.rowIndex
    const tr = rowRefs.current.get(focusCell.rowIndex)
    if (tr) {
      tr.focus()
      tr.scrollIntoView?.({ block: 'nearest' })
      pendingRowFocus.current = null
    }
  }, [focusCell])
  useEffect(() => {
    if (pendingRowFocus.current === null) return
    const tr = rowRefs.current.get(pendingRowFocus.current)
    if (tr) {
      tr.focus()
      pendingRowFocus.current = null
    }
  })

  // Move DOM focus to the active cell when a keyboard action asked for it.
  useEffect(() => {
    if (focusSeq === 0) return
    const td = scrollRef.current?.querySelector<HTMLElement>(
      `tr[data-row-index="${active.row}"] td[data-col-index="${active.col}"]`)
    td?.focus()
  }, [focusSeq])

  // An editor opened from the keyboard on a NULL cell has no text input to
  // autofocus: put focus on its value-state control so keys reach it.
  useEffect(() => {
    if (!edit) return
    const cell = editCellRef.current
    if (cell && !cell.contains(document.activeElement)) {
      cell.querySelector<HTMLElement>('select, input, textarea')?.focus()
    }
  }, [edit?.row, edit?.col])

  if (result.error) {
    return <div class={s.error} role="alert">{result.error}</div>
  }

  const editable = columns !== undefined && typeof onStageUpdate === 'function'
  const deletable = typeof onStageDelete === 'function' && canDelete !== false

  const metaByCol = new Map((columns ?? []).map(c => [c.name, c]))

  function cellEditable(col: string): boolean {
    // Authoritative: the server's catalog metadata decides. Key columns,
    // generated and identity columns are read-only; everything else edits.
    if (!editable) return false
    const meta = metaByCol.get(col)
    if (!meta) return false
    return meta.editable
  }

  function nextEditableColumn(fromCol: string): string | null {
    if (!columns) return null
    const order = result.columns
    const start = order.indexOf(fromCol)
    for (let i = start + 1; i < order.length; i++) {
      if (cellEditable(order[i])) return order[i]
    }
    return null
  }

  /** Whether focus is inside the open editor (Enter/Escape), as opposed to
   *  leaving it (blur commit): only the former hands focus back to the cell. */
  function focusInEditor(): boolean {
    const cell = editCellRef.current
    return !!cell && cell.contains(document.activeElement)
  }

  function closeEditor(returnFocus: boolean) {
    setEdit(null)
    if (returnFocus) setFocusSeq(n => n + 1)
  }

  function commitEdit() {
    if (!edit || !onStageUpdate) return
    const returnFocus = focusInEditor()
    if (!sameEdit(edit.edit, edit.initial)) {
      onStageUpdate(edit.row, edit.col, edit.edit)
    }
    closeEditor(returnFocus)
  }

  function cancelEdit() {
    closeEditor(focusInEditor())
  }

  function commitEditAndTab() {
    if (!edit || !onStageUpdate) return
    if (!sameEdit(edit.edit, edit.initial)) {
      onStageUpdate(edit.row, edit.col, edit.edit)
    }
    const next = nextEditableColumn(edit.col)
    if (next === null) {
      setEdit(null)
      return
    }
    const idx = result.columns.indexOf(next)
    const cell = (result.rows[edit.row] as unknown[] | undefined)?.[idx]
    const isNull = cell === null || cell === undefined
    const staged = stagedRows?.get(edit.row)?.updates[next]
    const initial: CellEdit = staged
      ? staged.edit
      : isNull ? { kind: 'null' } : { kind: 'value', text: formatCell(cell) }
    setActive({ row: edit.row, col: idx })
    setEdit({ row: edit.row, col: next, edit: initial, initial })
  }

  function openEditor(rowIdx: number, col: string) {
    if (!cellEditable(col)) return
    const idx = result.columns.indexOf(col)
    setActive({ row: rowIdx, col: idx })
    const cell = (result.rows[rowIdx] as unknown[] | undefined)?.[idx]
    const isNull = cell === null || cell === undefined
    const staged = stagedRows?.get(rowIdx)?.updates[col]
    const initial: CellEdit = staged
      ? staged.edit
      : isNull ? { kind: 'null' } : { kind: 'value', text: formatCell(cell) }
    setEdit({ row: rowIdx, col, edit: initial, initial })
  }

  function sortMark(col: string): string | null {
    if (!sorts) return null
    const i = sorts.findIndex(k => k.column === col)
    if (i < 0) return null
    return `${sorts[i].dir === 'desc' ? '↓' : '↑'}${sorts.length > 1 ? i + 1 : ''}`
  }

  function renderCell(rowIdx: number, colIdx: number) {
    const col = result.columns[colIdx]
    const val = (result.rows[rowIdx] as unknown[] | undefined)?.[colIdx]

    if (edit && edit.row === rowIdx && edit.col === col) {
      const meta = metaByCol.get(col)
      return (
        <TypedEditor
          column={meta ?? { name: col, type: 'text', tag: null, nullable: true, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: true }}
          mode="update"
          edit={edit.edit}
          autoFocus
          commitOnBlur
          onChange={next => { if (edit) setEdit({ ...edit, edit: next }) }}
          onCommit={commitEdit}
          onCancel={cancelEdit}
          onTab={commitEditAndTab}
        />
      )
    }

    const stagedCell = stagedRows?.get(rowIdx)?.updates[col]
    if (stagedCell) {
      return (
        <span
          class={s.stagedCell}
          title={`staged (not committed): ${stagedCellText(stagedCell.edit)} — was ${val === null || val === undefined ? 'NULL' : formatCell(val)}`}
        >
          {stagedCellText(stagedCell.edit)}
        </span>
      )
    }

    const fk = fkColumns?.[col]
    // A reference is followable only when every component of its tuple is
    // non-NULL in this row (MATCH SIMPLE: a NULL component references nothing).
    const fkTupleComplete = fk !== undefined && fkLocalColumns(fk, col).every(c => {
      const i = result.columns.indexOf(c)
      const v = i < 0 ? undefined : (result.rows[rowIdx] as unknown[] | undefined)?.[i]
      return v !== null && v !== undefined
    })
    if (fk && onFollowFK && fkTupleComplete) {
      const refs = (fk.refColumns ?? [fk.refColumn]).filter(Boolean).join(', ')
      return (
        <button
          class={s.fkLink}
          tabIndex={-1}
          title={`Follow ${fk.refSchema}.${fk.refTable} (${refs})`}
          onClick={() => onFollowFK(fk, rowIdx)}
        >
          {formatCell(val)}
        </button>
      )
    }

    if (val === null) {
      return cellEditable(col) ? (
        <button class={s.nullBtn} tabIndex={-1} title="Set value" onClick={() => openEditor(rowIdx, col)}>
          <span class={s.null}>NULL</span>
        </button>
      ) : (
        <span class={s.null}>NULL</span>
      )
    }
    if (val === undefined) return <span class={s.null}>—</span>
    // X01: vectors and tsvectors are not row-editable values — read-only
    // render of the exact text form with a copy affordance.
    const tag = metaByCol.get(col)?.tag
    if (tag === 'vector' || tag === 'tsvector') {
      const text = formatCell(val)
      return (
        <span class={s.vectorCell}>
          <span class={s.vectorText} title={text}>{text}</span>
          <button
            class={s.copyBtn}
            tabIndex={-1}
            title={`Copy ${tag} value`}
            aria-label={`Copy ${tag} value`}
            onClick={() => {
              void navigator.clipboard?.writeText(text).then(
                () => toast('info', `${tag} value copied`),
                () => toast('error', 'copy failed'),
              )
            }}
          >⧉</button>
        </span>
      )
    }
    return formatCell(val)
  }

  /** Follow the FK link of a cell, when its whole tuple is present. */
  function followFrom(rowIdx: number, col: string): boolean {
    const fk = fkColumns?.[col]
    if (!fk || !onFollowFK) return false
    const complete = fkLocalColumns(fk, col).every(c => {
      const i = result.columns.indexOf(c)
      const v = i < 0 ? undefined : (result.rows[rowIdx] as unknown[] | undefined)?.[i]
      return v !== null && v !== undefined
    })
    if (!complete) return false
    onFollowFK(fk, rowIdx)
    return true
  }

  const pageRows = Math.max(1, Math.floor(viewportH / rowH) - 1)

  function moveTo(row: number, col: number) {
    const r = Math.max(0, Math.min(rowCount - 1, row))
    const c = Math.max(0, Math.min(colCount - 1, col))
    setActive({ row: r, col: c })
    revealRow(r)
    setFocusSeq(n => n + 1)
  }

  // Grid keyboard model (WAI-ARIA grid): arrows move the active cell,
  // PageUp/PageDown by a viewport, Home/End within the row (Ctrl: first/
  // last row), Enter/F2 edit an editable cell (Enter on a read-only FK cell
  // follows the link; Shift+Enter always follows), Delete stages the row
  // delete. Keys typed inside an open editor belong to the editor.
  function handleGridKey(e: KeyboardEvent) {
    const target = e.target as HTMLElement | null
    if (!target || target.closest('[data-editor-cell]')) return
    if (rowCount === 0 || colCount === 0) return
    const { row, col } = active
    const colName = result.columns[col]
    switch (e.key) {
      case 'ArrowDown': moveTo(row + 1, col); break
      case 'ArrowUp': moveTo(row - 1, col); break
      case 'ArrowRight': moveTo(row, col + 1); break
      case 'ArrowLeft': moveTo(row, col - 1); break
      case 'PageDown': moveTo(row + pageRows, col); break
      case 'PageUp': moveTo(row - pageRows, col); break
      case 'Home': moveTo(e.ctrlKey || e.metaKey ? 0 : row, 0); break
      case 'End': moveTo(e.ctrlKey || e.metaKey ? rowCount - 1 : row, colCount - 1); break
      case 'F2':
        if (!cellEditable(colName)) return
        openEditor(row, colName)
        break
      case 'Enter':
        if (e.shiftKey) {
          if (!followFrom(row, colName)) return
        } else if (cellEditable(colName)) {
          openEditor(row, colName)
        } else if (!followFrom(row, colName)) {
          return
        }
        break
      case 'Delete':
        if (!deletable || stagedRows?.get(row)?.deleteId !== undefined) return
        onStageDelete?.(row)
        break
      default:
        return
    }
    e.preventDefault()
  }

  const win = visibleWindow(rowCount, scrollTop, viewportH, rowH)
  const spanCols = colCount + (deletable ? 1 : 0)
  const windowRows: number[] = []
  for (let i = win.start; i < win.end; i++) windowRows.push(i)
  // The grid's single tab stop: the active cell when it is rendered, else
  // the first rendered row's cell in the active column (after wheel
  // scrolling the active row may have left the window).
  const clampedRow = Math.min(active.row, Math.max(0, rowCount - 1))
  const tabRow = clampedRow >= win.start && clampedRow < win.end ? clampedRow : win.start

  return (
    <div class={s.wrap}>
      <div
        class={s.scrollArea}
        ref={scrollRef}
        onScroll={e => setScrollTop((e.currentTarget as HTMLDivElement).scrollTop)}
      >
        <table
          class={s.table}
          role="grid"
          aria-label={label ?? 'Result rows'}
          aria-rowcount={rowCount + 1}
          aria-colcount={colCount}
          onKeyDown={handleGridKey}
        >
          <thead ref={headRef}>
            <tr aria-rowindex={1}>
              {deletable && <th class={`${s.th} ${s.thAction}`} scope="col"> </th>}
              {result.columns.map((col, colIdx) => {
                const meta = metaByCol.get(col)
                const isPk = meta?.isKey ?? false
                const mark = sortMark(col)
                const sortIdx = sorts?.findIndex(k => k.column === col) ?? -1
                const ariaSort = sortIdx === 0 ? (sorts![0].dir === 'desc' ? 'descending' : 'ascending') : undefined
                return (
                  <th
                    key={col}
                    class={`${s.th}${isPk ? ` ${s.thPk}` : ''}${onSort ? ` ${s.thSortable}` : ''}`}
                    scope="col"
                    aria-colindex={colIdx + 1}
                    aria-sort={ariaSort}
                    onClick={onSort ? (e => onSort(col, e.shiftKey)) : undefined}
                    title={onSort ? 'Click to sort by this column (asc → desc → off); Shift+click to add it to a multi-column sort' : undefined}
                  >
                    {isPk && <span class={s.pkHeader} title="Primary key">PK </span>}
                    {onSort ? (
                      <button
                        type="button"
                        class={s.sortBtn}
                        aria-label={`Sort by ${col}${mark ? ` (currently ${mark})` : ''}; Shift adds to a multi-column sort`}
                        onClick={e => { e.stopPropagation(); onSort(col, e.shiftKey) }}
                      >{col}</button>
                    ) : col}
                    {mark && <span class={s.sortMark}> {mark}</span>}
                    {fkColumns?.[col] && <span class={s.fkMark} title={`References ${fkColumns[col].refTable}.${(fkColumns[col].refColumns ?? [fkColumns[col].refColumn]).filter(Boolean).join(', ')}`}> FK</span>}
                  </th>
                )
              })}
            </tr>
          </thead>
          <tbody>
            {win.start > 0 && (
              <tr class={s.spacer} aria-hidden="true" style={{ height: `${win.start * rowH}px` }}>
                <td colSpan={spanCols} />
              </tr>
            )}
            {windowRows.map(rowIdx => {
              const stagedRow = stagedRows?.get(rowIdx)
              const rowDeleted = stagedRow?.deleteId !== undefined
              const focused = focusCell?.rowIndex === rowIdx
              return (
                <tr
                  key={rowKeys?.[rowIdx] ?? rowIdx}
                  ref={el => {
                    if (el) rowRefs.current.set(rowIdx, el)
                    else rowRefs.current.delete(rowIdx)
                  }}
                  class={`${s.tr}${rowDeleted ? ` ${s.trStagedDelete}` : ''}${focused ? ` ${s.trFocus}` : ''}`}
                  data-row-index={rowIdx}
                  data-row-key={rowKeys?.[rowIdx]}
                  aria-rowindex={rowIdx + 2}
                  tabIndex={-1}
                >
                  {deletable && (
                    <td class={s.tdAction}>
                      <button
                        class={s.deleteBtn}
                        tabIndex={-1}
                        disabled={rowDeleted}
                        title={rowDeleted ? 'Delete staged — commit or discard it below' : 'Stage row delete'}
                        aria-label={`Stage delete row ${rowIdx + 1}`}
                        onClick={() => onStageDelete?.(rowIdx)}
                      >×</button>
                    </td>
                  )}
                  {result.columns.map((col, colIdx) => {
                    const meta = metaByCol.get(col)
                    const readOnlyTitle = editable && meta && !meta.editable
                      ? (meta.readOnlyReason ?? 'read-only')
                      : undefined
                    const cellFocused = focused && focusCell?.column === col
                    const isActive = tabRow === rowIdx && active.col === colIdx
                    const isEditing = edit !== null && edit.row === rowIdx && edit.col === col
                    return (
                      <td
                        key={col}
                        ref={isEditing ? (el => { editCellRef.current = el }) : undefined}
                        class={`${s.td}${cellFocused ? ` ${s.tdFocus}` : ''}${isActive ? ` ${s.tdActive}` : ''}${isEditing ? ` ${s.tdEditing}` : ''}`}
                        role="gridcell"
                        aria-colindex={colIdx + 1}
                        aria-readonly={editable ? !cellEditable(col) : undefined}
                        data-col-index={colIdx}
                        data-editor-cell={isEditing ? '' : undefined}
                        tabIndex={isActive ? 0 : -1}
                        onFocus={() => { if (!isActive) setActive({ row: rowIdx, col: colIdx }) }}
                        onDblClick={cellEditable(col) ? () => openEditor(rowIdx, col) : undefined}
                        title={readOnlyTitle}
                      >
                        {renderCell(rowIdx, colIdx)}
                      </td>
                    )
                  })}
                </tr>
              )
            })}
            {win.end < rowCount && (
              <tr class={s.spacer} aria-hidden="true" style={{ height: `${(rowCount - win.end) * rowH}px` }}>
                <td colSpan={spanCols} />
              </tr>
            )}
          </tbody>
        </table>
      </div>
      {result.truncated && (
        <div class={s.truncatedNote} role="status">
          Showing the first {rowCount.toLocaleString()} rows — the result has more and was truncated
          {result.rowLimit ? ` at the ${result.rowLimit.toLocaleString()}-row limit` : ''}. Use LIMIT/OFFSET, or export the table for every row.
        </div>
      )}
      {exportName && (
        <div class={s.footer}>
          <span class={s.footerText}>
            {rowCount.toLocaleString()} row{rowCount === 1 ? '' : 's'}
            {result.duration != null ? ` · ${result.duration}ms` : ''}
          </span>
          {rowCount > 0 && (
            <div class={s.exportBtns}>
              <button class={s.exportBtn} onClick={() => exportResultCSV(result, `${exportName}.csv`)} title="Download these rows as CSV (NULL is an empty field, the empty string is &quot;&quot;)">↓ CSV</button>
              <button class={s.exportBtn} onClick={() => exportResultJSON(result, `${exportName}.json`)} title="Download these rows as JSON (bigint digits exact)">↓ JSON</button>
            </div>
          )}
        </div>
      )}
      {editable && (
        <div class={s.editHint}>double-click a cell (or Enter/F2 on the focused cell) to stage an edit — Enter or Tab saves, Esc cancels; arrow keys move between cells, Delete stages a row delete; NULL is explicit (empty text stays an empty string); staged edits commit as one {limitsFor('sql')?.transaction === 'atomic' ? 'atomic batch' : 'transaction'} below</div>
      )}
    </div>
  )
}
