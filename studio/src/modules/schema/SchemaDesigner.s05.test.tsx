import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen, cleanup, fireEvent, waitFor } from '@testing-library/preact'

// S05: the designer loads tables from the shared schema document v2 and
// turns visual changes into REVIEWABLE plans from the CLI planner — never
// client-built DDL. Apply is bound to the reviewed plan id and surfaces the
// server's refusals (stale plan, migration-managed database). These tests
// drive the real component tree (SchemaDesigner -> ColumnTable ->
// PlanReview); only the fetch boundary is mocked, and assertions are on the
// REQUESTS the component sends (the SQL itself is the Go E2E legs' oracle).

vi.mock('../../lib/api', async (importOriginal) => {
  const orig = await importOriginal<typeof import('../../lib/api')>()
  return {
    ...orig,
    api: {
      ...orig.api,
      columns: vi.fn(),
      schemaObject: vi.fn(),
      codegen: vi.fn(),
      schemaPlan: vi.fn(),
      schemaApply: vi.fn(),
      schema: vi.fn(),
    },
  }
})

import { api, ApiError } from '../../lib/api'
import { activeConnection, schema } from '../../lib/store'
import { SchemaDesigner } from './SchemaDesigner'
import { Toasts } from '../../components/Toast'
import { makePlan, op } from './planFixture'
import type { Schema, SchemaObjectDetail } from '../../lib/types'

const objectMock = vi.mocked(api.schemaObject)
const columnsMock = vi.mocked(api.columns)
const codegenMock = vi.mocked(api.codegen)
const planMock = vi.mocked(api.schemaPlan)
const applyMock = vi.mocked(api.schemaApply)
const schemaMock = vi.mocked(api.schema)

const emptySchema: Schema = {
  sql: [{ schema: 'public', name: 'orders', columns: [] }], views: [],
  kv: [], vector: [], timeseries: [], document: [], graph: [], fts: [],
  geo: [], blob: [], pubsub: [], streams: [], columnar: [], datalog: null, cdc: false,
}

function ordersDetail(): SchemaObjectDetail {
  return {
    kind: 'table', schema: 'public', name: 'orders', source: 'introspection-v2', documentSHA256: 'd'.repeat(64),
    table: {
      columns: [
        { name: 'id', type: 'bigint', notNull: true, isPrimaryKey: true, default: { kind: 'identity' } },
        { name: 'code', type: 'varchar(255)', notNull: true, isPrimaryKey: false },
        { name: 'note', type: 'text', notNull: false, isPrimaryKey: false, default: { kind: 'literal', sql: "'n/a'::text" } },
      ],
      constraints: [{ name: 'orders_pkey', type: 'primary-key', columns: ['id'] }],
      indexes: [{ name: 'orders_note_idx', unique: false, method: 'btree', key: [{ column: 'note' }] }],
      references: [],
      referencedBy: [],
    },
  }
}

beforeEach(() => {
  cleanup()
  activeConnection.value = { id: 'c1', name: 'test', url: 'postgres://x', isNucleus: false }
  schema.value = emptySchema
  codegenMock.mockResolvedValue({ code: '// gen' })
  objectMock.mockImplementation(async () => ordersDetail())
  schemaMock.mockResolvedValue(emptySchema)
})

afterEach(() => {
  cleanup()
  vi.clearAllMocks()
})

async function openOrders() {
  render(<><SchemaDesigner initialSchema="public" initialTable="orders" /><Toasts /></>)
  await waitFor(() => screen.getByDisplayValue('note'))
}

describe('SchemaDesigner plan flow (S05)', () => {
  it('loads the table from the shared v2 metadata in the planner spelling', async () => {
    await openOrders()
    expect(objectMock).toHaveBeenCalledWith('c1', 'public', 'orders')
    expect(columnsMock).not.toHaveBeenCalled()
    // varchar keeps its length (the legacy information_schema read lost it).
    expect(screen.getByDisplayValue('varchar(255)')).toBeTruthy()
    // Literal default text is editable; the identity default is locked.
    expect(screen.getByDisplayValue("'n/a'::text")).toBeTruthy()
    expect(screen.getByText('identity')).toBeTruthy()
    expect(screen.getByText('orders_note_idx')).toBeTruthy()
  })

  it('plans structured changes and applies exactly the reviewed plan id', async () => {
    const reviewed = makePlan({ planId: '1'.repeat(64), operations: [op(1, 'alter table "public"."orders" rename column "note" to "remark"')] })
    planMock.mockResolvedValueOnce(reviewed)
    applyMock.mockResolvedValueOnce({ ...reviewed, applied: true, verification: 'in-sync' })
    await openOrders()
    fireEvent.input(screen.getByDisplayValue('note'), { target: { value: 'remark' } })
    fireEvent.input(screen.getByDisplayValue("'n/a'::text"), { target: { value: '' } })
    fireEvent.click(screen.getByText('Plan Changes'))
    await waitFor(() => expect(planMock).toHaveBeenCalledTimes(1))
    const changes = planMock.mock.calls[0][0].changes
    expect(changes).toEqual([
      { op: 'rename-column', schema: 'public', table: 'orders', from: 'note', to: 'remark' },
      { op: 'drop-default', schema: 'public', table: 'orders', column: 'remark' },
    ])
    expect(screen.getByText(/Migration plan — 1 statement/)).toBeTruthy()
    expect(applyMock).not.toHaveBeenCalled()
    fireEvent.click(screen.getByText('Apply plan'))
    await waitFor(() => expect(applyMock).toHaveBeenCalledTimes(1))
    expect(applyMock.mock.calls[0][0]).toEqual({ connectionId: 'c1', changes, planId: '1'.repeat(64), allowDestructive: false })
    // After apply the catalog is refreshed and the table reloaded.
    await waitFor(() => expect(schemaMock).toHaveBeenCalled())
    await waitFor(() => expect(objectMock).toHaveBeenCalledTimes(2))
  })

  it('edits address the right column after an earlier column is staged for drop', async () => {
    planMock.mockResolvedValueOnce(makePlan({ operations: [op(1, 'x')] }))
    await openOrders()
    fireEvent.click(screen.getByRole('button', { name: 'Drop column code' }))
    fireEvent.input(screen.getByDisplayValue('note'), { target: { value: 'memo' } })
    fireEvent.click(screen.getByText('Plan Changes'))
    await waitFor(() => expect(planMock).toHaveBeenCalledTimes(1))
    expect(planMock.mock.calls[0][0].changes).toEqual([
      { op: 'rename-column', schema: 'public', table: 'orders', from: 'note', to: 'memo' },
      { op: 'drop-column', schema: 'public', table: 'orders', column: 'code' },
    ])
  })

  it('refuses primary-key changes with the vocabulary boundary', async () => {
    await openOrders()
    fireEvent.click(screen.getByRole('checkbox', { name: 'code primary key' }))
    fireEvent.click(screen.getByText('Plan Changes'))
    expect(planMock).not.toHaveBeenCalled()
    expect(await screen.findByText(/outside the designer's plan vocabulary/)).toBeTruthy()
  })

  it('a data-loss plan needs the acknowledgement, which is forwarded to apply', async () => {
    const reviewed = makePlan({ operations: [op(1, 'alter table "public"."orders" drop column if exists "code"', { destructive: true, dataLoss: true })] })
    planMock.mockResolvedValueOnce(reviewed)
    applyMock.mockResolvedValueOnce({ ...reviewed, applied: true, verification: 'in-sync' })
    await openOrders()
    fireEvent.click(screen.getByRole('button', { name: 'Drop column code' }))
    fireEvent.click(screen.getByText('Plan Changes'))
    await waitFor(() => screen.getByText(/Migration plan — 1 statement/))
    expect((screen.getByText('Apply plan') as HTMLButtonElement).disabled).toBe(true)
    fireEvent.click(screen.getByRole('checkbox', { name: /can lose data/ }))
    fireEvent.click(screen.getByText('Apply plan'))
    await waitFor(() => expect(applyMock).toHaveBeenCalledTimes(1))
    expect(applyMock.mock.calls[0][0].allowDestructive).toBe(true)
    expect(applyMock.mock.calls[0][0].changes).toEqual([{ op: 'drop-column', schema: 'public', table: 'orders', column: 'code' }])
  })

  it('a stale plan is replaced by the fresh one for review, never applied blind', async () => {
    const reviewed = makePlan({ planId: '1'.repeat(64), operations: [op(1, 'alter table "public"."orders" alter column "note" set not null')] })
    const fresh = makePlan({ planId: '2'.repeat(64), operations: [] })
    planMock.mockResolvedValueOnce(reviewed)
    applyMock.mockRejectedValueOnce(new ApiError(409, 'the live catalog changed', { state: 'stale-plan', body: { state: 'stale-plan', plan: fresh } }))
    await openOrders()
    fireEvent.click(screen.getByRole('checkbox', { name: 'note nullable' }))
    fireEvent.click(screen.getByText('Plan Changes'))
    await waitFor(() => screen.getByText(/Migration plan — 1 statement/))
    fireEvent.click(screen.getByText('Apply plan'))
    await waitFor(() => screen.getByText(/Migration plan — 0 statements/))
    expect(screen.getByRole('alert').textContent).toContain('changed since you reviewed')
    expect(screen.getByRole('status').textContent).toContain('already matches')
    expect(applyMock).toHaveBeenCalledTimes(1)
    await waitFor(() => expect(schemaMock).toHaveBeenCalled())
  })

  it('a migration-managed database refusal is shown with its guidance', async () => {
    planMock.mockResolvedValueOnce(makePlan({ operations: [op(1, 'alter table "public"."orders" add column "x" text')] }))
    applyMock.mockRejectedValueOnce(new ApiError(409, 'this database has a migration history (_neutron_migrations): … run `neutron migrate generate --schema target.schema.json`', { state: 'migration-managed' }))
    await openOrders()
    fireEvent.click(screen.getByText('+ Add Column'))
    fireEvent.input(screen.getByRole('textbox', { name: 'Column name (new column 4)' }), { target: { value: 'x' } })
    fireEvent.click(screen.getByText('Plan Changes'))
    await waitFor(() => screen.getByText(/Migration plan — 1 statement/))
    expect(planMock.mock.calls[0][0].changes).toEqual([
      { op: 'add-column', schema: 'public', table: 'orders', column: 'x', type: 'text', notNull: false, default: undefined },
    ])
    fireEvent.click(screen.getByText('Apply plan'))
    const alert = await screen.findByRole('alert')
    expect(alert.textContent).toContain('migration history')
    expect(screen.getByText('Download target document')).toBeTruthy()
  })

  it('creates a table in the selected schema and then opens it', async () => {
    const reviewed = makePlan({ operations: [op(1, 'create table "public"."tags" (...)')] })
    planMock.mockResolvedValueOnce(reviewed)
    applyMock.mockResolvedValueOnce({ ...reviewed, applied: true, verification: 'in-sync' })
    render(<><SchemaDesigner /><Toasts /></>)
    fireEvent.click(screen.getByText('+ New'))
    fireEvent.input(screen.getByPlaceholderText('table_name'), { target: { value: 'tags' } })
    fireEvent.click(screen.getByText('Plan Create Table'))
    await waitFor(() => screen.getByText('Migration plan — 1 statement'))
    fireEvent.click(screen.getByText('Apply plan'))
    await waitFor(() => expect(applyMock).toHaveBeenCalledTimes(1))
    const changes = applyMock.mock.calls[0][0].changes
    expect(changes[0]).toMatchObject({ op: 'create-table', schema: 'public', table: 'tags' })
    expect(changes[0].columns?.[0]).toMatchObject({ name: 'id', isPrimaryKey: true, notNull: true })
    await waitFor(() => expect(objectMock).toHaveBeenCalledWith('c1', 'public', 'tags'))
  })

  it('surfaces verification drift honestly after apply', async () => {
    const reviewed = makePlan({ operations: [op(1, 'alter table ...')] })
    planMock.mockResolvedValueOnce(reviewed)
    applyMock.mockResolvedValueOnce({ ...reviewed, applied: true, verification: 'drift', residual: ['alter table x'] })
    await openOrders()
    fireEvent.input(screen.getByDisplayValue('note'), { target: { value: 'remark2' } })
    fireEvent.click(screen.getByText('Plan Changes'))
    await waitFor(() => screen.getByText('Migration plan — 1 statement'))
    fireEvent.click(screen.getByText('Apply plan'))
    await waitFor(() => screen.getByText(/does not match the plan \(1 residual/))
  })

  it('a table dropped concurrently is reported, not rendered stale', async () => {
    objectMock.mockReset()
    objectMock.mockRejectedValue(new ApiError(404, 'no table or view public.orders'))
    render(<SchemaDesigner initialSchema="public" initialTable="orders" />)
    const alert = await screen.findByRole('alert')
    expect(alert.textContent).toContain('gone from the live catalog')
  })
})
