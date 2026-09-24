import { useEffect, useRef } from 'preact/hooks'
import type { CellEdit, TableMetaColumn } from '../lib/types'
import s from './TypedEditor.module.css'

/** Which explicit value states the editor offers. Update edits are
 *  value/NULL (DEFAULT is an insert-only concept: an omitted column). */
export type EditorValueState = 'value' | 'null' | 'default'

export interface TypedEditorProps {
  column: TableMetaColumn
  mode: 'update' | 'insert'
  edit: CellEdit
  onChange: (edit: CellEdit) => void
  /** Enter commits (calls onCommit) only when set; Escape always cancels. */
  onCommit?: () => void
  onCancel?: () => void
  /** Shift-less Tab: commit and move (grid keyboard flow). */
  onTab?: () => void
  /** Clicking/tabbing out of the whole editor commits (grid behavior). */
  commitOnBlur?: boolean
  autoFocus?: boolean
  /** Validation error from the parent (e.g. a required insert column). */
  error?: string | null
}

function isBoolean(col: TableMetaColumn): boolean {
  return col.type === 'boolean' || col.type === 'bool'
}

function isJson(col: TableMetaColumn): boolean {
  return /^json(b)?$/.test(col.type)
}

/** Placeholder showing the canonical wire form for the column's type. */
function placeholderFor(col: TableMetaColumn): string {
  if (col.tag === 'int8') return 'bigint digits, e.g. 9007199254740993'
  if (col.tag === 'numeric') return 'decimal digits with scale, e.g. 12.3450'
  if (col.tag === 'date') return 'YYYY-MM-DD'
  if (col.tag === 'timestamp') return 'YYYY-MM-DDTHH:MM:SS[.ffffff]'
  if (col.tag === 'timestamptz') return 'YYYY-MM-DDTHH:MM:SS[.ffffff]Z (UTC)'
  if (col.tag === 'bytea') return '\\x hex'
  if (isJson(col)) return 'JSON text, e.g. {"a":1}'
  if (isBoolean(col)) return 'true | false'
  return col.type
}

function stateOf(edit: CellEdit): EditorValueState {
  return edit.kind
}

function textOf(edit: CellEdit): string {
  return edit.kind === 'value' ? edit.text : ''
}

/** The typed cell editor (S03): one column, one explicit value state.
 *
 * The value state is a three-way control — a concrete value, SQL NULL, or
 * (insert only) DEFAULT, i.e. omit the column. They never coerce: an empty
 * text input is a real empty string, not NULL and not DEFAULT. Typing is
 * per column kind: booleans edit as true/false, JSON as validated JSON
 * text, tagged numerics/temporals as their canonical text (the wire layer
 * re-tags them so bigint/decimal digits never cross through Number).
 *
 * Keyboard: Enter commits (when onCommit is given), Escape cancels, Tab
 * keeps native order (the parent decides what "next" means).
 */
export function TypedEditor({ column, mode, edit, onChange, onCommit, onCancel, onTab, commitOnBlur, autoFocus, error }: TypedEditorProps) {
  const inputRef = useRef<HTMLInputElement | HTMLTextAreaElement | null>(null)

  useEffect(() => {
    if (autoFocus && edit.kind === 'value' && inputRef.current) {
      inputRef.current.focus()
      if ('select' in inputRef.current) inputRef.current.select()
    }
  }, [autoFocus])

  const state = stateOf(edit)
  const disabled = state !== 'value'

  function setState(next: EditorValueState) {
    if (next === state) return
    if (next === 'value') onChange({ kind: 'value', text: textOf(edit) })
    else if (next === 'null') onChange({ kind: 'null' })
    else onChange({ kind: 'default' })
  }

  function handleKey(e: KeyboardEvent) {
    if (e.key === 'Enter' && !e.shiftKey && onCommit) {
      e.preventDefault()
      onCommit()
    } else if (e.key === 'Escape') {
      e.preventDefault()
      onCancel?.()
    } else if (e.key === 'Tab' && onTab) {
      // The grid intercepts Tab to commit and open the next editable cell.
      e.preventDefault()
      onTab()
    }
  }

  const title = [
    `${column.name} ${column.type}`,
    column.nullable ? 'nullable' : 'NOT NULL',
    column.hasDefault ? 'has default' : undefined,
    error ?? undefined,
  ].filter(Boolean).join(' · ')

  return (
    <span
      class={s.editor}
      data-column={column.name}
      title={title}
      onBlur={e => {
        // Focus moving INSIDE the editor (input <-> state select) is not a
        // commit; leaving the editor entirely commits when asked (grid).
        if (!commitOnBlur) return
        if (e.currentTarget.contains(e.relatedTarget as Node | null)) return
        onCommit?.()
      }}
    >
      <select
        class={s.stateSelect}
        value={state}
        aria-label={`${column.name} value state`}
        onChange={e => setState((e.target as HTMLSelectElement).value as EditorValueState)}
        onKeyDown={handleKey}
      >
        <option value="value">value</option>
        <option value="null">NULL</option>
        {mode === 'insert' && <option value="default">DEFAULT</option>}
      </select>
      {isBoolean(column) ? (
        <select
          ref={inputRef as never}
          class={s.boolSelect}
          value={disabled ? '' : (textOf(edit) || '')}
          disabled={disabled}
          aria-label={`${column.name} boolean value`}
          onChange={e => onChange({ kind: 'value', text: (e.target as HTMLSelectElement).value })}
          onKeyDown={handleKey}
        >
          <option value=""></option>
          <option value="true">true</option>
          <option value="false">false</option>
        </select>
      ) : isJson(column) ? (
        <textarea
          ref={inputRef as never}
          class={`${s.jsonInput}${error ? ` ${s.invalid}` : ''}`}
          rows={2}
          disabled={disabled}
          aria-label={`${column.name} JSON text`}
          onInput={e => onChange({ kind: 'value', text: (e.target as HTMLTextAreaElement).value })}
          onKeyDown={handleKey}
        >{textOf(edit)}</textarea>
      ) : (
        <input
          ref={inputRef as never}
          class={`${s.input}${error ? ` ${s.invalid}` : ''}`}
          type="text"
          value={textOf(edit)}
          disabled={disabled}
          aria-label={`${column.name} value`}
          placeholder={placeholderFor(column)}
          onInput={e => onChange({ kind: 'value', text: (e.target as HTMLInputElement).value })}
          onKeyDown={handleKey}
        />
      )}
      {error && <span class={s.error} role="alert">{error}</span>}
    </span>
  )
}
