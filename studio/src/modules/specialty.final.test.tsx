import { beforeEach, afterEach, describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import { KVModule } from './kv/KVModule'
import { BlobModule } from './blob/BlobModule'
import { ColumnarModule } from './columnar/ColumnarModule'
import { DatalogModule } from './datalog/DatalogModule'
import { StreamsModule } from './streams/StreamsModule'
import { DocModule } from './document/DocModule'
import { activeConnection, toasts } from '../lib/store'
vi.mock('../lib/api',async original=>{const m=await original<typeof import('../lib/api')>();return {...m,api:{...m.api,query:vi.fn()}}})
import { api } from '../lib/api'
const query=vi.mocked(api.query)
const ok=(cell?:unknown)=>({columns:['v'],rows:cell===undefined?[]:[[cell]],rowCount:1,duration:0})
const input=async(el:Element,value:string)=>fireEvent.input(el,{target:{value}})
const click=async(text:string|RegExp)=>fireEvent.click(screen.getByText(text,{selector:'button'}))
const draft=()=>expect(screen.getByDisplayValue('draft')).toBeTruthy()
const cases: {name:string; pattern:RegExp; action:()=>Promise<void>; preserve:()=>void}[]=[
 {name:'KV new',pattern:/KV_SET/,action:async()=>{render(<KVModule name="kv"/>);await fireEvent.click(screen.getByTitle('New Key'));await input(screen.getByPlaceholderText('Key'),'new');await input(screen.getByPlaceholderText('Value'),'draft');await click('Set')},preserve:draft},
 {name:'KV inline',pattern:/KV_SET/,action:async()=>{render(<KVModule name="kv"/>);await fireEvent.click(await screen.findByTitle('Click to edit'));await input(document.querySelector('textarea')!,'draft');await click('Save')},preserve:draft},
 {name:'KV selected',pattern:/KV_SET/,action:async()=>{render(<KVModule name="kv"/>);await fireEvent.click(await screen.findByText('key'));await input(document.querySelector('textarea')!,'draft');await click('Save')},preserve:draft},
 {name:'KV delete',pattern:/KV_DEL/,action:async()=>{render(<KVModule name="kv"/>);await fireEvent.click(await screen.findByText('key'));await fireEvent.click(screen.getByTitle('Delete key'));await fireEvent.click(screen.getByTitle('Click again to confirm'))},preserve:()=>expect(document.querySelector('textarea')).toBeTruthy()},
 {name:'blob delete',pattern:/BLOB_DELETE/,action:async()=>{render(<BlobModule name="blobs"/>);await fireEvent.click(await screen.findByTitle('blob'));await fireEvent.click(screen.getByTitle('Delete blob'));await fireEvent.click(screen.getByTitle('Click again to confirm'))},preserve:()=>expect(screen.getByText('Blob details')).toBeTruthy()},
 {name:'columnar insert',pattern:/COLUMNAR_INSERT/,action:async()=>{render(<ColumnarModule name="table"/>);await input(screen.getByPlaceholderText('col1=val1, col2=val2'),'note=draft');await click('Insert')},preserve:()=>expect(screen.getByDisplayValue('note=draft')).toBeTruthy()},
 {name:'stream append',pattern:/STREAM_XADD/,action:async()=>{render(<StreamsModule name="A"/>);await input(screen.getByPlaceholderText('field'),'note');await input(screen.getByPlaceholderText('value'),'draft');await click('Append')},preserve:draft},
 {name:'stream group create',pattern:/STREAM_XGROUP_CREATE/,action:async()=>{render(<StreamsModule name="A"/>);await click('+ Create Group');await input(screen.getByPlaceholderText('my-consumer-group'),'draft');await click('Create')},preserve:draft},
 {name:'stream ACK',pattern:/STREAM_XACK/,action:async()=>{render(<StreamsModule name="A"/>);await input(screen.getByPlaceholderText('group'),'G');await input(screen.getByPlaceholderText('consumer'),'C');await click('Read as group');await screen.findByText('1-0');await input(screen.getByPlaceholderText('stream name'),'B');await click('ACK')},preserve:()=>expect(screen.getByText('1-0')).toBeTruthy()},
 {name:'document raw save',pattern:/DOC_UPDATE/,action:async()=>{render(<DocModule name="docs"/>);await fireEvent.click(await screen.findByText('7',{selector:'span'}));await click('Raw');await input(document.querySelector('textarea')!,'{"note":"draft"}');await click('Save Document')},preserve:()=>expect(screen.getByDisplayValue('{"note":"draft"}')).toBeTruthy()},
 {name:'document tree save',pattern:/DOC_UPDATE/,action:async()=>{render(<DocModule name="docs"/>);await fireEvent.click(await screen.findByText('7',{selector:'span'}));await fireEvent.click(screen.getByText('"old"'));await input(document.querySelector('input[class*="inlineEditInput"]')!,'"draft"');await fireEvent.keyDown(document.querySelector('input[class*="inlineEditInput"]')!,{key:'Enter'});await click('Save Document')},preserve:()=>expect(screen.getByText('"draft"')).toBeTruthy()},
 {name:'document delete',pattern:/DOC_DELETE/,action:async()=>{render(<DocModule name="docs"/>);await fireEvent.click(await screen.findByText('7',{selector:'span'}));await fireEvent.click(screen.getByTitle('Delete'));await fireEvent.click(screen.getByTitle('Click again to confirm'))},preserve:()=>expect(screen.getByText('"old"')).toBeTruthy()},
 {name:'Datalog assert',pattern:/DATALOG_ASSERT/,action:async()=>{render(<DatalogModule/>);await input(document.querySelector('textarea')!,'p("draft").\nq(X) :- p(X).\n?- q(X)');await click('▶ Evaluate')},preserve:()=>{expect(screen.queryByText(/tuples ·/)).toBeNull();expect(query.mock.calls.some(([sql])=>sql.includes('DATALOG_RULE'))).toBe(false)}},
 {name:'Datalog rule',pattern:/DATALOG_RULE/,action:async()=>{render(<DatalogModule/>);await input(document.querySelector('textarea')!,'p("draft").\nq(X) :- p(X).\n?- q(X)');await click('▶ Evaluate')},preserve:()=>{expect(screen.queryByText(/tuples ·/)).toBeNull();expect(query.mock.calls.some(([sql])=>sql.includes('DATALOG_QUERY'))).toBe(false)}},
]
beforeEach(()=>{activeConnection.value={id:'c',name:'test',url:'pg://test',isNucleus:true};toasts.value=[];query.mockReset()})
afterEach(cleanup)
describe.each(cases)('$name actual handler failure ordering',c=>{
 it.each(['error','canceled','network','success'])('%s outcome',async state=>{
  query.mockImplementation(async sql=>{
   if(c.pattern.test(sql)) { if(state==='network') throw new TypeError('network'); return state==='error'?{...ok(),error:'denied'}:state==='canceled'?{...ok(),canceled:true}:ok() }
   if(sql.includes('KV_KEYS')) return ok('["key"]')
   if(sql.includes('KV_GET')) return {...ok(),rows:[['old',-1]]}
   if(sql.includes('BLOB_LIST')) return ok('["blob"]')
   if(sql.includes('BLOB_META')) return ok('{"size":1,"content_type":"text/plain","created_at":0}')
   if(sql.includes('DOC_QUERY')) return ok('7')
   if(sql.includes('DOC_GET')) return ok('{"note":"old"}')
   if(sql.includes('STREAM_XREADGROUP')) return ok('[{"id":"1-0","fields":{"note":"draft"}}]')
   if(sql.includes('DATALOG_QUERY')) return ok('[["draft"]]')
   return ok()
  })
  await c.action()
  await waitFor(()=>expect(query.mock.calls.some(([sql])=>c.pattern.test(sql))).toBe(true))
  if(state==='success') {
   if(c.name.startsWith('Datalog')) await screen.findByText(/tuples ·/)
   else await waitFor(()=>expect(toasts.value.some(t=>t.kind==='success'||t.kind==='info')).toBe(true))
  } else {
   await waitFor(()=>expect(toasts.value.some(t=>t.kind==='error')).toBe(true))
   expect(toasts.value.some(t=>t.kind==='success'||t.kind==='info')).toBe(false)
   c.preserve()
  }
  expect(query.mock.calls.filter(([sql])=>c.pattern.test(sql))).toHaveLength(1)
  if(c.name==='stream ACK') expect(query.mock.calls.find(([sql])=>c.pattern.test(sql))![0]).toContain("STREAM_XACK('A', 'G'")
 })
})

it('an intermediate Datalog assertion failure stops later facts, rules, and query publication',async()=>{
 let asserts=0
 query.mockImplementation(async sql=>sql.includes('DATALOG_ASSERT')&&++asserts===2?{...ok(),error:'second assertion denied'}:ok())
 render(<DatalogModule/>);
 await input(document.querySelector('textarea')!,'p("a").\np("b").\np("c").\nq(X) :- p(X).\n?- q(X)')
 await click('▶ Evaluate')
 await waitFor(()=>expect(toasts.value.some(t=>t.message.includes('second assertion denied'))).toBe(true))
 expect(asserts).toBe(2)
 expect(query.mock.calls.some(([sql])=>sql.includes('DATALOG_RULE')||sql.includes('DATALOG_QUERY'))).toBe(false)
 expect(screen.queryByText(/tuples ·/)).toBeNull()
})

it.each(['KV inline', 'columnar insert', 'stream append'])('%s success preserves a draft changed while the write is pending', async name => {
  const c = cases.find(c => c.name === name)!
  let finish!: (v: ReturnType<typeof ok>) => void
  query.mockImplementation(async sql => {
    if (c.pattern.test(sql)) return new Promise(resolve => { finish = resolve })
    if (sql.includes('KV_KEYS')) return ok('["key"]')
    if (sql.includes('KV_GET')) return { ...ok(), rows: [['old', -1]] }
    return ok()
  })
  await c.action()
  await waitFor(() => expect(finish).toBeTypeOf('function'))
  const control = name === 'KV inline' ? document.querySelector('textarea')! : name === 'columnar insert' ? screen.getByPlaceholderText('col1=val1, col2=val2') : screen.getByPlaceholderText('value')
  await input(control, 'newer draft')
  if (name === 'KV inline') { await fireEvent.keyDown(control, { key: 'Enter', ctrlKey: true }); await fireEvent.keyDown(control, { key: 'Enter', ctrlKey: true }) }
  finish(ok())
  await waitFor(() => expect(toasts.value.some(t => t.kind === 'success')).toBe(true))
  expect(screen.getByDisplayValue('newer draft')).toBeTruthy()
  expect(query.mock.calls.filter(([sql]) => c.pattern.test(sql))).toHaveLength(1)
})
it('repeated ACK clicks share one request and leave a newer consumer read intact', async () => {
  let finish!: (v: ReturnType<typeof ok>) => void
  query.mockImplementation(async sql => sql.includes('STREAM_XACK') ? new Promise(resolve => { finish = resolve }) : sql.includes('STREAM_XREADGROUP') ? ok('[{"id":"1-0","fields":{"note":"draft"}}]') : ok())
  render(<StreamsModule name="A" />)
  await input(screen.getByPlaceholderText('group'), 'G'); await input(screen.getByPlaceholderText('consumer'), 'C'); await click('Read as group')
  await screen.findByText('1-0'); await click('ACK'); await click('ACK')
  expect(query.mock.calls.filter(([sql]) => sql.includes('STREAM_XACK'))).toHaveLength(1)
  await click('Read as group')
  await waitFor(() => expect(query.mock.calls.filter(([sql]) => sql.includes('STREAM_XREADGROUP'))).toHaveLength(2))
  finish(ok()); await waitFor(() => expect(toasts.value.some(t => t.message.includes('ACK'))).toBe(true))
  expect(screen.getByText('1-0')).toBeTruthy()
})
