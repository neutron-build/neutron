import { beforeEach, afterEach, describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import { DocModule } from './DocModule'
import { activeConnection, toasts } from '../../lib/store'
vi.mock('../../lib/api', async original => { const m = await original<typeof import('../../lib/api')>(); return {...m,api:{...m.api,query:vi.fn()}} })
import { api } from '../../lib/api'
const query = vi.mocked(api.query)
const ok = (cell?: unknown) => ({columns:['v'],rows:cell === undefined ? [] : [[cell]],rowCount:1,duration:0})
let raw: string
beforeEach(() => {
  activeConnection.value = {id:'c1',name:'test',url:'pg://test',isNucleus:true}
  toasts.value=[]
  raw='{"a.b":"old","a":{"b":"neighbor"},"exact":9007199254740993,"fraction":0.123456789012345678901,"exp":1.234567890123456789e123}'
  query.mockReset()
  query.mockImplementation(async sql => sql.includes('DOC_QUERY') ? ok('7') : sql.includes('DOC_GET') ? ok(raw) : ok())
})
afterEach(cleanup)
async function select() { await fireEvent.click(await screen.findByText('7', {selector:'span'})) }
describe('document real handlers preserve source and binding', () => {
  it('edits the clicked literal key and retains untouched numeric lexemes', async () => {
    render(<DocModule name="Documents" />); await select()
    await fireEvent.click(screen.getByText('"old"'))
    const input=document.querySelector('input[class*="inlineEditInput"]')!
    await fireEvent.input(input,{target:{value:'"new"'}}); await fireEvent.keyDown(input,{key:'Enter'})
    await fireEvent.click(screen.getByText('Save Document'))
    await waitFor(()=>expect(query.mock.calls.some(([sql])=>sql.includes('DOC_UPDATE'))).toBe(true))
    expect(query.mock.calls.find(([sql])=>sql.includes('DOC_UPDATE'))![0]).toContain(raw.replace('old','new'))
  })
  it('saves selected A to A after collection input changes to B; refused save keeps raw draft', async () => {
    render(<DocModule name="Documents" />); await select()
    await fireEvent.click(screen.getByText('Raw'))
    const editor=document.querySelector('textarea[class*="rawEditor"]')!
    await fireEvent.input(editor,{target:{value:raw.replace('old','draft')}})
    await fireEvent.input(screen.getByTitle(/Document collection/),{target:{value:'B'}})
    query.mockImplementation(async sql => sql.includes('DOC_UPDATE') ? {...ok(),error:'denied'} : sql.includes('DOC_QUERY') ? ok('') : ok())
    await fireEvent.click(screen.getByText('Save Document'))
    await waitFor(()=>expect(toasts.value.some(t=>t.message==='denied')).toBe(true))
    const call=query.mock.calls.find(([sql])=>sql.includes('DOC_UPDATE'))!
    expect(call[0]).toContain('DOC_UPDATE(7,')
    expect((editor as HTMLTextAreaElement).value).toContain('draft')
    expect(toasts.value.some(t=>t.kind==='success')).toBe(false)
  })
  it('fences stale A list before fetching bodies under B', async () => {
    let finish!: (v: ReturnType<typeof ok>)=>void
    query.mockImplementationOnce(()=>new Promise(resolve=>{finish=resolve}))
    render(<DocModule name="Documents" />)
    await waitFor(()=>expect(finish).toBeTypeOf('function'))
    await fireEvent.input(screen.getByTitle(/Document collection/),{target:{value:'B'}})
    await waitFor(()=>expect(query.mock.calls.some(([sql])=>sql.includes("DOC_QUERY('B'"))).toBe(true))
    finish(ok('999'))
    await new Promise(r=>setTimeout(r,20))
    expect(query.mock.calls.some(([sql])=>sql.includes('999'))).toBe(false)
  })
  it.each(['denied','canceled','network','success'])('insert %s preserves exact submitted raw JSON and failure drafts', async state=> {
    query.mockImplementation(async sql=> {
      if(sql.includes('DOC_INSERT')) { if(state==='network') throw new TypeError('network'); return state==='denied' ? {...ok(),error:'denied'} : state==='canceled' ? {...ok(),canceled:true} : ok() }
      return ok('')
    })
    render(<DocModule name="Documents" />)
    await fireEvent.click(screen.getByTitle('New Document'))
    const editor=document.querySelector('textarea[class*="newDocEditor"]')!
    await fireEvent.input(editor,{target:{value:raw}})
    await fireEvent.click(screen.getByText('Create'))
    await waitFor(()=>expect(query.mock.calls.some(([sql])=>sql.includes('DOC_INSERT'))).toBe(true))
    expect(query.mock.calls.find(([sql])=>sql.includes('DOC_INSERT'))![0]).toContain(raw)
    await waitFor(()=>expect(toasts.value).toHaveLength(1))
    if(state==='success') expect(toasts.value[0].kind).toBe('success')
    else { expect(toasts.value[0].kind).toBe('error'); expect((editor as HTMLTextAreaElement).value).toBe(raw); expect(document.querySelector('textarea[class*="newDocEditor"]')).toBeTruthy() }
  })
})

// R1/R3: actual document component boundaries, pending capacity-release execution.
it('requires independent B confirmation and keeps a B draft after A delete completes', async () => {
  let finish!: (v: ReturnType<typeof ok>) => void
  query.mockImplementation(async sql => {
    if (sql.includes('DOC_DELETE')) return new Promise(resolve => { finish = resolve })
    return sql.includes('DOC_QUERY') ? ok('7') : sql.includes('DOC_GET') ? ok(raw) : ok()
  })
  render(<DocModule name="Documents" />)
  await select()
  await fireEvent.click(screen.getByTitle('Delete'))
  await fireEvent.input(screen.getByTitle(/Document collection/), { target: { value: 'B' } })
  await screen.findByText('7', { selector: 'span' })
  await fireEvent.click(screen.getByTitle('Delete'))
  expect(query.mock.calls.some(([sql]) => sql.includes('DOC_DELETE'))).toBe(false)
  // Independently confirm A, then select B while that mutation is pending.
  await fireEvent.input(screen.getByTitle(/Document collection/), { target: { value: '' } })
  await screen.findByText('7', { selector: 'span' })
  await fireEvent.click(screen.getByTitle('Delete'))
  await fireEvent.click(screen.getByTitle('Click again to confirm'))
  await waitFor(() => expect(finish).toBeTypeOf('function'))
  await fireEvent.input(screen.getByTitle(/Document collection/), { target: { value: 'B' } })
  await select()
  await fireEvent.click(screen.getByText('Raw'))
  const editor = document.querySelector('textarea[class*="rawEditor"]') as HTMLTextAreaElement
  await fireEvent.input(editor, { target: { value: raw.replace('old', 'draft B') } })
  finish(ok())
  await waitFor(() => expect(toasts.value.some(t => t.message.includes('deleted'))).toBe(true))
  expect(editor.value).toContain('draft B')
  expect(screen.getByText(/7 · B @c1/)).toBeTruthy()
})
it('reopening the same document resets inline editing and protects its newer draft from delete completion', async () => {
  let finish!: (v: ReturnType<typeof ok>) => void
  query.mockImplementation(async sql => sql.includes('DOC_DELETE') ? new Promise(resolve => { finish = resolve }) : sql.includes('DOC_QUERY') ? ok('7') : sql.includes('DOC_GET') ? ok(raw) : ok())
  render(<DocModule name="Documents" />); await select()
  await fireEvent.click(screen.getByText('"old"'))
  await fireEvent.click(screen.getByTitle('Delete')); await fireEvent.click(screen.getByTitle('Click again to confirm'))
  await waitFor(() => expect(finish).toBeTypeOf('function'))
  await select()
  expect(screen.queryByRole('textbox', { name: /JSON value at/ })).toBeNull()
  await fireEvent.click(screen.getByText('Raw'))
  const editor = document.querySelector('textarea[class*="rawEditor"]') as HTMLTextAreaElement
  await fireEvent.input(editor, { target: { value: raw.replace('old', 'reopened') } })
  finish(ok()); await waitFor(() => expect(toasts.value.some(t => t.message.includes('deleted'))).toBe(true))
  expect(editor.value).toContain('reopened')
  expect(screen.getByText(/7 · \(default\) @c1/)).toBeTruthy()
})
it.each(['"9007199254740993"', '"true"', '"null"', '9007199254740993', '0.123456789012345678901', '1e123', 'true', 'false', 'null'])('keyboard edit and tree/raw/save retain JSON type and lexeme %s', async literal => {
  raw = `{"value":${literal},"exact":9007199254740993}`
  render(<DocModule name="Documents" />); await select()
  const leaf = screen.getByRole('button', { name: 'Edit JSON value at ["value"]' })
  await fireEvent.keyDown(leaf, { key: 'Enter' })
  const input = await screen.findByRole('textbox', { name: 'JSON value at ["value"]' }) as HTMLInputElement
  expect(input.value).toBe(literal)
  await waitFor(() => expect(document.activeElement).toBe(input))
  await fireEvent.keyDown(input, { key: 'Enter' })
  await waitFor(() => expect(document.activeElement).toBe(screen.getByRole('button', { name: 'Edit JSON value at ["value"]' })))
  await fireEvent.click(screen.getByText('Raw'))
  expect((document.querySelector('textarea[class*="rawEditor"]') as HTMLTextAreaElement).value).toBe(raw)
  await fireEvent.click(screen.getByText('Tree')); await fireEvent.click(screen.getByText('Save Document'))
  await waitFor(() => expect(query.mock.calls.some(([sql]) => sql.includes('DOC_UPDATE'))).toBe(true))
  expect(query.mock.calls.find(([sql]) => sql.includes('DOC_UPDATE'))![0]).toContain(raw)
})
it('invalid inline JSON shows a labeled error and keeps the editor open', async () => {
  render(<DocModule name="Documents" />); await select()
  await fireEvent.keyDown(screen.getByRole('button', { name: 'Edit JSON value at ["a.b"]' }), { key: ' ' })
  const input = await screen.findByRole('textbox', { name: 'JSON value at ["a.b"]' })
  await fireEvent.input(input, { target: { value: 'unquoted invalid' } }); await fireEvent.keyDown(input, { key: 'Enter' })
  expect(screen.getByRole('alert').textContent).toContain('Invalid JSON')
  expect(screen.getByRole('textbox', { name: 'JSON value at ["a.b"]' })).toBe(input)
  expect(query.mock.calls.some(([sql]) => sql.includes('DOC_UPDATE'))).toBe(false)
})
