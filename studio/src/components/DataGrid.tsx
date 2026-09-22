import { useEffect, useRef, useState } from 'preact/hooks'
import type { QueryResult } from '../lib/types'
import s from './DataGrid.module.css'

export interface FKTarget {
  refSchema: string
  refTable: string
  refColumn: string
}

interface DataGridProps {
  result: QueryResult
  /** Column name of the single-column primary key (enables editing). */
  pkColumn?: string
  /** Commit an edit; value === null means SQL NULL, '' means empty string. */
  onCommitEdit?: (rowIndex: number, column: string, value: string | null) => void
  /** FK columns by name; enables follow links. */
  fkColumns?: Record<string, FKTarget>
  onFollowFK?: (fk: FKTarget, value: unknown) => void
  sortColumn?: string | null
  sortDir?: 'asc' | 'desc'
  onSort?: (column: string) => void
}

interface EditState {
  row: number
  col: string
  draft: string
  initialText: string
  initialIsNull: boolean
  setNull: boolean
}

export function DataGrid({
  result,
  pkColumn,
  onCommitEdit,
  fkColumns,
  onFollowFK,
  sortColumn,
  sortDir,
  onSort,
}: DataGridProps) {
  const [edit, setEdit] = useState<EditState | null>(null)
  const inputRef = useRef<HTMLInputElement>(null)
  const nullRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    if (edit && !edit.setNull && inputRef.current) {
      inputRef.current.focus()
      inputRef.current.select()
    }
  }, [edit])

  if (result.error) {
    return <div class={s.error}>{result.error}</div>
  }

  const editable = pkColumn !== undefined && typeof onCommitEdit === 'function'

  function cellEditable(col: string): boolean {
    // The key column identifies the row; editing it is not offered (the
    // backend additionally rejects writes to generated/identity keys).
    return editable && col !== pkColumn
  }

  function commitEdit() {
    if (!edit || !onCommitEdit) return
    if (edit.setNull) {
      if (edit.initialIsNull) {
        setEdit(null)
        return
      }
      onCommitEdit(edit.row, edit.col, null)
    } else {
      if (!edit.initialIsNull && edit.draft === edit.initialText) {
        setEdit(null)
        return
      }
      // Empty input is a real empty string — SQL NULL requires the explicit
      // NULL control. The two are never silently converted into each other.
      onCommitEdit(edit.row, edit.col, edit.draft)
    }
    setEdit(null)
  }

  function handleKey(e: KeyboardEvent) {
    if (e.key === 'Enter') {
      e.preventDefault()
      commitEdit()
    } else if (e.key === 'Escape') {
      e.preventDefault()
      setEdit(null)
    }
  }

  function handleBlur(e: FocusEvent) {
    // Clicking/tabbing into the NULL control must not commit the editor.
    if (e.relatedTarget === nullRef.current) return
    commitEdit()
  }

  function renderCell(rowIdx: number, colIdx: number) {
    const col = result.columns[colIdx]
    const val = (result.rows[rowIdx] as unknown[] | undefined)?.[colIdx]

    if (edit && edit.row === rowIdx && edit.col === col) {
      return (
        <span class={s.cellEditor}>
          <input
            ref={inputRef}
            class={s.cellInput}
            value={edit.draft}
            disabled={edit.setNull}
            onInput={e => { if (edit) setEdit({ ...edit, draft: (e.target as HTMLInputElement).value }) }}
            onKeyDown={handleKey}
            onBlur={handleBlur}
            title="Enter to save, Esc to cancel; empty stays an empty string"
          />
          <label
            class={s.nullToggle}
            title="Set SQL NULL (empty text stays an empty string)"
            onMouseDown={e => e.preventDefault()}
          >
            <input
              ref={nullRef}
              type="checkbox"
              checked={edit.setNull}
              onKeyDown={e => { if (e.key === 'Escape') { e.preventDefault(); setEdit(null) } else if (e.key === 'Enter') { e.preventDefault(); commitEdit() } }}
              onChange={e => { if (edit) setEdit({ ...edit, setNull: (e.target as HTMLInputElement).checked }) }}
            />
            NULL
          </label>
        </span>
      )
    }

    const fk = fkColumns?.[col]
    if (fk && onFollowFK && val !== null && val !== undefined) {
      return (
        <button
          class={s.fkLink}
          title={`Follow ${fk.refSchema}.${fk.refTable} (${fk.refColumn})`}
          onClick={() => onFollowFK(fk, val)}
        >
          {String(val)}
        </button>
      )
    }

    if (val === null) {
      return cellEditable(col) ? (
        <button class={s.nullBtn} title="Set value" onClick={() => setEdit({ row: rowIdx, col, draft: '', initialText: '', initialIsNull: true, setNull: true })}>
          <span class={s.null}>NULL</span>
        </button>
      ) : (
        <span class={s.null}>NULL</span>
      )
    }
    if (val === undefined) return <span class={s.null}>—</span>
    return String(val)
  }

  return (
    <div class={s.wrap}>
      <div class={s.scrollArea}>
        <table class={s.table}>
          <thead>
            <tr>
              {result.columns.map((col) => {
                const isPk = col === pkColumn
                const isSorted = sortColumn === col
                return (
                  <th
                    key={col}
                    class={`${s.th}${isPk ? ` ${s.thPk}` : ''}${onSort ? ` ${s.thSortable}` : ''}`}
                    onClick={onSort ? () => onSort(col) : undefined}
                    title={onSort ? 'Click to sort' : undefined}
                  >
                    {isPk && <span class={s.pkHeader} title="Primary key">PK </span>}
                    {col}
                    {isSorted && <span class={s.sortMark}> {sortDir === 'desc' ? '↓' : '↑'}</span>}
                    {fkColumns?.[col] && <span class={s.fkMark} title={`References ${fkColumns[col].refTable}.${fkColumns[col].refColumn}`}> FK</span>}
                                      </th>
                )
              })}
            </tr>
          </thead>
          <tbody>
            {result.rows.map((row, rowIdx) => (
              <tr key={rowIdx} class={s.tr}>
                {result.columns.map((col, colIdx) => (
                  <td
                    key={col}
                    class={s.td}
                    onDblClick={
                      cellEditable(col)
                        ? () => {
                            const cell = (row as unknown[])[colIdx]
                            const isNull = cell === null || cell === undefined
                            setEdit({
                              row: rowIdx,
                              col,
                              draft: isNull ? '' : String(cell),
                              initialText: isNull ? '' : String(cell),
                              initialIsNull: isNull,
                              setNull: isNull,
                            })
                          }
                        : undefined
                    }
                    title={editable && col === pkColumn ? 'Primary key column is read-only' : undefined}
                  >
                    {renderCell(rowIdx, colIdx)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {editable && (
        <div class={s.editHint}>double-click a cell to edit — Enter saves, Esc cancels; check NULL for SQL NULL, empty text stays an empty string</div>
      )}
    </div>
  )
}
