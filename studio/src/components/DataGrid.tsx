import { useEffect, useRef, useState } from 'preact/hooks'
import type { CellEdit, QueryResult, TableMetaColumn, TableSort } from '../lib/types'
import { formatCell } from '../lib/wire'
import { toast } from '../lib/store'
import { TypedEditor } from './TypedEditor'
import s from './DataGrid.module.css'

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
}: DataGridProps) {
  const [edit, setEdit] = useState<EditState | null>(null)
  const rowRefs = useRef<Map<number, HTMLTableRowElement>>(new Map())

  // Error focus (S03): land on the first offending row/cell after a failed
  // commit so the cause is on screen. The parent clears focusCell (the
  // highlight lingers); each new focus object re-triggers the focus.
  useEffect(() => {
    if (!focusCell) return
    const tr = rowRefs.current.get(focusCell.rowIndex)
    if (tr) {
      tr.focus()
      tr.scrollIntoView({ block: 'nearest' })
    }
  }, [focusCell])

  if (result.error) {
    return <div class={s.error}>{result.error}</div>
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

  function commitEdit() {
    if (!edit || !onStageUpdate) return
    if (!sameEdit(edit.edit, edit.initial)) {
      onStageUpdate(edit.row, edit.col, edit.edit)
    }
    setEdit(null)
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
    setEdit({ row: edit.row, col: next, edit: initial, initial })
  }

  function openEditor(rowIdx: number, col: string) {
    if (!cellEditable(col)) return
    const idx = result.columns.indexOf(col)
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
          onCancel={() => setEdit(null)}
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
          title={`Follow ${fk.refSchema}.${fk.refTable} (${refs})`}
          onClick={() => onFollowFK(fk, rowIdx)}
        >
          {formatCell(val)}
        </button>
      )
    }

    if (val === null) {
      return cellEditable(col) ? (
        <button class={s.nullBtn} title="Set value" onClick={() => openEditor(rowIdx, col)}>
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

  return (
    <div class={s.wrap}>
      <div class={s.scrollArea}>
        <table class={s.table}>
          <thead>
            <tr>
              {deletable && <th class={`${s.th} ${s.thAction}`} scope="col"> </th>}
              {result.columns.map((col) => {
                const meta = metaByCol.get(col)
                const isPk = meta?.isKey ?? false
                const mark = sortMark(col)
                return (
                  <th
                    key={col}
                    class={`${s.th}${isPk ? ` ${s.thPk}` : ''}${onSort ? ` ${s.thSortable}` : ''}`}
                    onClick={onSort ? (e => onSort(col, e.shiftKey)) : undefined}
                    title={onSort ? 'Click to sort by this column (asc → desc → off); Shift+click to add it to a multi-column sort' : undefined}
                  >
                    {isPk && <span class={s.pkHeader} title="Primary key">PK </span>}
                    {col}
                    {mark && <span class={s.sortMark}> {mark}</span>}
                    {fkColumns?.[col] && <span class={s.fkMark} title={`References ${fkColumns[col].refTable}.${(fkColumns[col].refColumns ?? [fkColumns[col].refColumn]).filter(Boolean).join(', ')}`}> FK</span>}
                  </th>
                )
              })}
            </tr>
          </thead>
          <tbody>
            {result.rows.map((_row, rowIdx) => {
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
                  tabIndex={-1}
                >
                  {deletable && (
                    <td class={s.tdAction}>
                      <button
                        class={s.deleteBtn}
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
                    return (
                      <td
                        key={col}
                        class={`${s.td}${cellFocused ? ` ${s.tdFocus}` : ''}`}
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
          </tbody>
        </table>
      </div>
      {editable && (
        <div class={s.editHint}>double-click a cell to stage an edit — Enter or Tab saves, Esc cancels; NULL is explicit (empty text stays an empty string); staged edits commit as one atomic batch below</div>
      )}
    </div>
  )
}
