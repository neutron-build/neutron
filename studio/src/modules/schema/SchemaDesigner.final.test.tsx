import { beforeEach, afterEach, describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import { SchemaDesigner } from './SchemaDesigner'
import { activeConnection, schema } from '../../lib/store'
import { makePlan, op } from './planFixture'
vi.mock('../../lib/api', async original=>{const m=await original<typeof import('../../lib/api')>();return {...m,api:{...m.api,schemaObject:vi.fn(),schemaPlan:vi.fn(),schemaApply:vi.fn(),schema:vi.fn(),codegen:vi.fn()}}})
import { api } from '../../lib/api'
const detail=(name:string,type:string)=>({kind:'table',schema:'public',name,source:'introspection-v2',documentSHA256:'d',table:{columns:[{name:'amount',type,notNull:false,isPrimaryKey:false}],constraints:[],indexes:[],references:[],referencedBy:[]}}) as never
beforeEach(()=>{
  activeConnection.value={id:'c',name:'test',url:'pg://test',isNucleus:false}
  schema.value={sql:[{schema:'public',name:'A',columns:[]},{schema:'public',name:'B',columns:[]}]} as never
  vi.mocked(api.schemaObject).mockReset();vi.mocked(api.schemaPlan).mockReset();vi.mocked(api.codegen).mockResolvedValue({code:''})
})
afterEach(cleanup)
describe('designer selection and plan ownership',()=>{
  it('older metadata cannot overwrite B or address changes from A to B',async()=>{
    let finishA!: (v:never)=>void
    vi.mocked(api.schemaObject).mockImplementation((_c,_s,t)=>t==='A'?new Promise(r=>{finishA=r}):Promise.resolve(detail('B','numeric(12,4)')))
    vi.mocked(api.schemaPlan).mockResolvedValue(makePlan({}))
    render(<SchemaDesigner initialTable="A" />)
    await waitFor(()=>expect(finishA).toBeTypeOf('function'))
    await fireEvent.click(screen.getByText('B', {selector:'button'}))
    await screen.findByDisplayValue('numeric(12,4)')
    finishA(detail('A','integer'))
    await new Promise(r=>setTimeout(r,20))
    expect(screen.queryByDisplayValue('integer')).toBeNull()
    await fireEvent.change(screen.getByDisplayValue('numeric(12,4)'),{target:{value:'bigint'}})
    await fireEvent.click(screen.getByText(/Review changes|Save Changes|Plan Changes|Save changes/))
    await waitFor(()=>expect(api.schemaPlan).toHaveBeenCalled())
    expect(vi.mocked(api.schemaPlan).mock.calls[0][0].changes).toEqual(expect.arrayContaining([expect.objectContaining({table:'B',type:'bigint'})]))
  })
  it('new-table draft survives older table response',async()=>{
    let finish!: (v:never)=>void
    vi.mocked(api.schemaObject).mockImplementation(()=>new Promise(r=>{finish=r}))
    render(<SchemaDesigner initialTable="A" />)
    await waitFor(()=>expect(finish).toBeTypeOf('function'))
    await fireEvent.click(screen.getByText('+ New'))
    finish(detail('A','integer'))
    await screen.findByDisplayValue('id')
    expect(screen.queryByDisplayValue('amount')).toBeNull()
  })
  it('cancel/new selection prevents an outstanding plan from repopulating review',async()=>{
    vi.mocked(api.schemaObject).mockResolvedValue(detail('A','integer'))
    let finish!: (v:ReturnType<typeof makePlan>)=>void
    vi.mocked(api.schemaPlan).mockImplementation(()=>new Promise(r=>{finish=r}))
    render(<SchemaDesigner initialTable="A" />)
    await screen.findByDisplayValue('amount')
    await fireEvent.change(screen.getByDisplayValue('integer'),{target:{value:'bigint'}})
    await fireEvent.click(screen.getByText(/Review changes|Save Changes|Plan Changes|Save changes/))
    await waitFor(()=>expect(finish).toBeTypeOf('function'))
    await fireEvent.click(screen.getByText('+ New'))
    finish(makePlan({}))
    await new Promise(r=>setTimeout(r,20))
    expect(screen.queryByText('Apply')).toBeNull()
    expect(screen.getByDisplayValue('id')).toBeTruthy()
  })
})

it.each(['B', 'new draft', 'unmount'])('apply refresh preserves %s continuation ownership', async next => {
  vi.mocked(api.schemaObject).mockResolvedValue(detail('B', 'integer'))
  vi.mocked(api.schemaPlan).mockResolvedValue(makePlan({ operations: [op(0, 'CREATE TABLE created_A(id bigint)')] }))
  vi.mocked(api.schemaApply).mockResolvedValue({ verification: 'in-sync', applied: true, up: ['CREATE'] } as never)
  let finish!: (v: never) => void
  vi.mocked(api.schema).mockImplementation(() => new Promise(resolve => { finish = resolve }))
  const view = render(<SchemaDesigner />)
  await fireEvent.click(screen.getByText('+ New'))
  const name = document.querySelector('input[class*="tableNameInput"]') ?? screen.getByPlaceholderText('table_name')
  await fireEvent.input(name, { target: { value: 'created_A' } })
  await fireEvent.click(screen.getByText('Plan Create Table'))
  await fireEvent.click(await screen.findByText('Apply plan'))
  await waitFor(() => expect(finish).toBeTypeOf('function'))
  if (next === 'B') await fireEvent.click(screen.getByText('B', { selector: 'button' }))
  else if (next === 'new draft') { await fireEvent.click(screen.getByText('+ New')); await fireEvent.input(screen.getByPlaceholderText('table_name'), { target: { value: 'newer' } }) }
  else view.unmount()
  finish(schema.value as never)
  await new Promise(resolve => setTimeout(resolve, 20))
  if (next === 'B') { expect(screen.getByDisplayValue('amount')).toBeTruthy(); expect(screen.queryByDisplayValue('created_A')).toBeNull() }
  else if (next === 'new draft') expect(screen.getByDisplayValue('newer')).toBeTruthy()
  else expect(view.container.textContent).toBe('')
})
it('late old-connection apply cannot publish its catalog into the active connection', async () => {
  vi.mocked(api.schemaObject).mockResolvedValue(detail('A', 'integer'))
  vi.mocked(api.schemaPlan).mockResolvedValue(makePlan({ operations: [op(0, 'ALTER TABLE A ADD c text')] }))
  let finish!: (v: never) => void
  vi.mocked(api.schemaApply).mockImplementation(() => new Promise(resolve => { finish = resolve }))
  vi.mocked(api.schema).mockResolvedValue({ sql: [{ schema: 'public', name: 'old', columns: [] }] } as never)
  render(<SchemaDesigner initialTable="A" />)
  await screen.findByDisplayValue('amount')
  await fireEvent.change(screen.getByDisplayValue('integer'), { target: { value: 'bigint' } })
  await fireEvent.click(screen.getByText(/Review changes|Save Changes|Plan Changes|Save changes/))
  await fireEvent.click(await screen.findByText('Apply plan'))
  await waitFor(() => expect(finish).toBeTypeOf('function'))
  activeConnection.value = { id: 'c2', name: 'two', url: 'pg://two', isNucleus: false }
  const current = { sql: [{ schema: 'public', name: 'current', columns: [] }] } as never
  schema.value = current
  finish({ verification: 'in-sync', applied: true, up: ['ALTER'] } as never)
  await waitFor(() => expect(api.schema).toHaveBeenCalledWith('c'))
  await new Promise(resolve => setTimeout(resolve, 20))
  expect(schema.value).toBe(current)
})

it.each([false, true].flatMap(create => ['name', 'type', 'default', 'add', 'delete', ...(create ? [] : ['index'])].map(change => ({ create, change }))))('same-view $change draft survives apply refresh (create=$create)', async ({ create, change }) => {
  vi.mocked(api.schemaObject).mockResolvedValue(detail('A', 'integer'))
  vi.mocked(api.schemaPlan).mockResolvedValue(makePlan({ operations: [op(0, create ? 'CREATE TABLE created_A(id bigint)' : 'ALTER TABLE A ADD c text')] }))
  vi.mocked(api.schemaApply).mockResolvedValue({ verification: 'in-sync', applied: true, up: ['DDL'] } as never)
  let finish!: (v: never) => void
  vi.mocked(api.schema).mockImplementation(() => new Promise(resolve => { finish = resolve }))
  render(<SchemaDesigner initialTable={create ? undefined : 'A'} />)
  if (create) {
    fireEvent.click(screen.getByText('+ New'))
    fireEvent.input(screen.getByPlaceholderText('table_name'), { target: { value: 'created_A' } })
    fireEvent.click(screen.getByText('Plan Create Table'))
  } else {
    await screen.findByDisplayValue('integer')
    fireEvent.change(screen.getByDisplayValue('integer'), { target: { value: 'bigint' } })
    fireEvent.click(screen.getByText(/Review changes|Save Changes|Plan Changes|Save changes/))
  }
  fireEvent.click(await screen.findByText('Apply plan'))
  await waitFor(() => expect(finish).toBeTypeOf('function'))
  const label = create ? 'id' : 'amount'
  if (change === 'name') fireEvent.input(screen.getByDisplayValue(label), { target: { value: 'newer_column' } })
  if (change === 'type') fireEvent.change(screen.getByLabelText(`Type of ${label}`), { target: { value: 'text' } })
  if (change === 'default') fireEvent.input(screen.getByLabelText(`Default of ${label}`), { target: { value: '42' } })
  if (change === 'add') fireEvent.click(screen.getByText('+ Add Column'))
  if (change === 'delete') fireEvent.click(screen.getByLabelText(`Drop column ${label}`))
  if (change === 'index') fireEvent.change(document.querySelector('select[class*="idxColSelect"]')!, { target: { value: 'amount' } })
  finish(schema.value as never)
  await screen.findByText(/Apply succeeded. Newer local edits were retained/)
  if (change === 'name') expect(screen.getByDisplayValue('newer_column')).toBeTruthy()
  if (change === 'type') expect((screen.getByLabelText(`Type of ${label}`) as HTMLSelectElement).value).toBe('text')
  if (change === 'default') expect(screen.getByDisplayValue('42')).toBeTruthy()
  if (change === 'add') expect(screen.getAllByPlaceholderText('column_name')).toHaveLength(2)
  if (change === 'delete') expect(screen.queryByDisplayValue(label)).toBeNull()
  if (change === 'index') expect((document.querySelector('select[class*="idxColSelect"]') as HTMLSelectElement).value).toBe('amount')
  if (create) expect(screen.getByDisplayValue('created_A')).toBeTruthy()
  expect(api.schemaObject).toHaveBeenCalledTimes(create ? 0 : 1)
  // Explicit reload is the only action allowed to replace retained edits.
  fireEvent.click(screen.getByText('Reload metadata and discard retained draft'))
  await screen.findByDisplayValue('amount')
})
