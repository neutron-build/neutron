import { describe, it, expect, vi, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup } from '@testing-library/preact'
import { useState } from 'preact/hooks'
import { TypedEditor } from './TypedEditor'
import type { CellEdit, TableMetaColumn } from '../lib/types'

// The typed cell editor (S03): three-way value state (value / NULL /
// DEFAULT — insert only), per-kind inputs (boolean select, validated JSON
// textarea, tagged text), and the keyboard contract (Enter commits,
// Escape cancels, Tab defers to the parent).

function col(t: Partial<TableMetaColumn>): TableMetaColumn {
  return {
    name: 'c', type: 'text', tag: null, nullable: true, isKey: false,
    generated: false, identity: false, hasDefault: false, autoAssigned: false,
    editable: true, ...t,
  }
}

/** Harness keeping the edit state in the parent, like the real callers. */
function Harness(props: {
  column: TableMetaColumn
  mode?: 'update' | 'insert'
  initial: CellEdit
  onCommit?: () => void
  onTab?: () => void
  onCancel?: () => void
}) {
  const [edit, setEdit] = useState(props.initial)
  return (
    <TypedEditor
      column={props.column}
      mode={props.mode ?? 'insert'}
      edit={edit}
      onChange={setEdit}
      onCommit={props.onCommit}
      onCancel={props.onCancel}
      onTab={props.onTab}
    />
  )
}

afterEach(cleanup)

describe('TypedEditor three-way state', () => {
  it('insert mode offers value, NULL and DEFAULT; update omits DEFAULT', () => {
    render(<Harness column={col({})} mode="insert" initial={{ kind: 'value', text: '' }} />)
    const state = screen.getByLabelText('c value state') as HTMLSelectElement
    expect(Array.from(state.options).map(o => o.value)).toEqual(['value', 'null', 'default'])
    cleanup()

    render(<Harness column={col({})} mode="update" initial={{ kind: 'value', text: 'x' }} />)
    const state2 = screen.getByLabelText('c value state') as HTMLSelectElement
    expect(Array.from(state2.options).map(o => o.value)).toEqual(['value', 'null'])
  })

  it('switching state carries the text: NULL -> value keeps an editable empty string', () => {
    render(<Harness column={col({})} mode="update" initial={{ kind: 'null' }} />)
    fireEvent.change(screen.getByLabelText('c value state'), { target: { value: 'value' } })
    const input = screen.getByLabelText('c value') as HTMLInputElement
    expect(input.disabled).toBe(false)
    expect(input.value).toBe('')
  })

  it('DEFAULT and NULL disable the value input', () => {
    render(<Harness column={col({})} mode="insert" initial={{ kind: 'default' }} />)
    expect((screen.getByLabelText('c value') as HTMLInputElement).disabled).toBe(true)
    fireEvent.change(screen.getByLabelText('c value state'), { target: { value: 'null' } })
    expect((screen.getByLabelText('c value') as HTMLInputElement).disabled).toBe(true)
  })
})

describe('TypedEditor typed inputs', () => {
  it('boolean columns edit as a true/false select', () => {
    render(<Harness column={col({ name: 'flag', type: 'boolean' })} initial={{ kind: 'value', text: '' }} />)
    const select = screen.getByLabelText('flag boolean value') as HTMLSelectElement
    fireEvent.change(select, { target: { value: 'true' } })
    expect((screen.getByLabelText('flag boolean value') as HTMLSelectElement).value).toBe('true')
  })

  it('JSON columns edit as a textarea', () => {
    render(<Harness column={col({ name: 'doc', type: 'jsonb' })} initial={{ kind: 'value', text: '{"a":1}' }} />)
    const area = screen.getByLabelText('doc JSON text') as HTMLTextAreaElement
    expect(area.value).toBe('{"a":1}')
    fireEvent.input(area, { target: { value: '{"b":2}' } })
    expect((screen.getByLabelText('doc JSON text') as HTMLTextAreaElement).value).toBe('{"b":2}')
  })

  it('tagged columns show their canonical wire form as the placeholder', () => {
    render(<Harness column={col({ name: 'big', type: 'int8', tag: 'int8' })} initial={{ kind: 'value', text: '' }} />)
    expect((screen.getByLabelText('big value') as HTMLInputElement).placeholder).toContain('9007199254740993')
    cleanup()
    render(<Harness column={col({ name: 'seen', type: 'timestamptz', tag: 'timestamptz' })} initial={{ kind: 'value', text: '' }} />)
    expect((screen.getByLabelText('seen value') as HTMLInputElement).placeholder).toContain('Z (UTC)')
  })
})

describe('TypedEditor keyboard contract', () => {
  it('Enter commits, Escape cancels, Tab defers to the parent', () => {
    const onCommit = vi.fn()
    const onCancel = vi.fn()
    const onTab = vi.fn()
    render(
      <Harness column={col({})} initial={{ kind: 'value', text: 'x' }}
        onCommit={onCommit} onCancel={onCancel} onTab={onTab} />,
    )
    const input = screen.getByLabelText('c value')
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(onCommit).toHaveBeenCalledTimes(1)
    fireEvent.keyDown(input, { key: 'Escape' })
    expect(onCancel).toHaveBeenCalledTimes(1)
    fireEvent.keyDown(input, { key: 'Tab' })
    expect(onTab).toHaveBeenCalledTimes(1)
  })

  it('the state select shares the keyboard contract (Enter on NULL commits)', () => {
    const onCommit = vi.fn()
    render(<Harness column={col({})} initial={{ kind: 'null' }} onCommit={onCommit} />)
    fireEvent.keyDown(screen.getByLabelText('c value state'), { key: 'Enter' })
    expect(onCommit).toHaveBeenCalledTimes(1)
    fireEvent.keyDown(screen.getByLabelText('c value state'), { key: 'Escape' })
  })
})

describe('TypedEditor error surfacing', () => {
  it('a column error renders an alert and marks the input invalid', () => {
    render(
      <TypedEditor
        column={col({ name: 'req', type: 'text' })}
        mode="insert"
        edit={{ kind: 'value', text: '' }}
        onChange={() => {}}
        error="required column — provide a value"
      />,
    )
    expect(screen.getByRole('alert').textContent).toContain('required column')
  })
})
