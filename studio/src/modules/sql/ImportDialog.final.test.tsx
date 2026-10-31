import { describe, it, expect, vi, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import { ImportDialog } from './ImportDialog'
import type { TableMeta } from '../../lib/types'
vi.mock('../../lib/api', async original=>{const m=await original<typeof import('../../lib/api')>();return {...m,api:{...m.api,tableMeta:vi.fn().mockRejectedValue(new Error('offline')),previewOperations:vi.fn()}}})
import { api } from '../../lib/api'
const meta={binding:'e:1',columns:['name','email'].map(name=>({name,type:'text',nullable:true,tag:null,insertable:true,hasDefault:false,generated:false,identity:false,autoAssigned:false}))} as TableMeta
const props={connectionId:'c',schema:'public',table:'t',meta,onClose:()=>{}}
function file(text:string,name:string,delay?:Promise<void>,error=false){
 const f=new File([text],name,{type:'text/csv'})
 Object.defineProperty(f,'lastModified',{value:1})
 const slice=f.slice.bind(f)
 Object.defineProperty(f,'slice',{value:(...args:Parameters<Blob['slice']>)=>{const part=slice(...args); const read=part.text.bind(part);Object.defineProperty(part,'text',{value:async()=>{await delay;if(error)throw new Error('stale file error');return read()}});return part}})
 return f
}
function choose(f:File){const input=document.querySelector('input[type="file"]')!;Object.defineProperty(input,'files',{value:[f],configurable:true});fireEvent.change(input)}
afterEach(()=>{cleanup();vi.clearAllMocks()})
describe('import setup generations',()=>{
 it.each([false,true])('older preview/error cannot replace newer column order (error=%s)',async error=>{
  let release!:()=>void
  const delay=new Promise<void>(r=>{release=r})
  const sendBatch=vi.fn().mockResolvedValue({rowsAffected:1,rowsInserted:1,replayed:false})
  render(<ImportDialog {...props} transport={{sendBatch,outcome:vi.fn()}}/>)
  choose(file('name,email\nold,old@x\n','A.csv',delay,error))
  choose(file('email,name\nb@x,B\n','B.csv'))
  await screen.findByText('b@x')
  release()
  await new Promise(r=>setTimeout(r,20))
  expect(screen.queryByText('stale file error')).toBeNull()
  expect(screen.queryByText('old@x')).toBeNull()
  await fireEvent.click(screen.getByText('Import',{selector:'button'}))
  await waitFor(()=>expect(sendBatch).toHaveBeenCalled())
  expect(sendBatch.mock.calls[0][0].rows[0]).toEqual({email:'b@x',name:'B'})
 })
 it('new file has no import action before its own preview is ready',async()=>{
  let release!:()=>void
  const delay=new Promise<void>(r=>{release=r})
  render(<ImportDialog {...props}/>)
  choose(file('name,email\nA,a@x\n','A.csv'))
  await screen.findByText('a@x')
  choose(file('name,email\nB,b@x\n','B.csv',delay))
  expect(screen.queryByText('Import',{selector:'button'})).toBeNull()
  release();await screen.findByText('b@x')
  expect(screen.getByText('Import',{selector:'button'})).toBeTruthy()
 })
 it('changing value options invalidates a pending database dry-run result',async()=>{
  let finish!:(v:never)=>void
  vi.mocked(api.previewOperations).mockImplementation(()=>new Promise(r=>{finish=r}))
  render(<ImportDialog {...props}/>)
  choose(file('name,email\nA,a@x\n','A.csv'))
  await screen.findByText('a@x')
  await fireEvent.click(screen.getByText('Dry-run first batch'))
  await waitFor(()=>expect(finish).toBeTypeOf('function'))
  const nullMarker=screen.getByPlaceholderText('none')
  await fireEvent.input(nullMarker,{target:{value:'NULL'}})
  finish({counts:{insert:1},operations:[]} as never)
  await new Promise(r=>setTimeout(r,20))
  expect(screen.queryByText(/would insert/)).toBeNull()
 })
})

it('CSV delimiter changes fence the old in-flight preview and refresh column mapping',async()=>{
 let release!:()=>void
 const delay=new Promise<void>(r=>{release=r})
 const f=file('name;email\nB;b@x\n','source.csv',delay)
 render(<ImportDialog {...props}/>)
 choose(f)
 await fireEvent.change(screen.getByLabelText('Delimiter'),{target:{value:';'}})
 release()
 await screen.findByText('b@x')
 expect(screen.queryByText('name;email')).toBeNull()
 expect(screen.getByText('Import',{selector:'button'})).toBeTruthy()
})

it('journal persistence failure warns and sends no initial batch', async () => {
 const sendBatch = vi.fn()
 const store = {getItem:()=>null,setItem:()=>{throw new Error('quota')},removeItem:()=>{}}
 render(<ImportDialog {...props} storage={store} transport={{sendBatch,outcome:vi.fn()}} />)
 choose(file('name,email\nA,a@x\n','persist.csv'))
 await screen.findByText('a@x')
 fireEvent.click(screen.getByText('Import',{selector:'button'}))
 await screen.findByText(/Import journal could not be saved/)
 expect(sendBatch).not.toHaveBeenCalled()
})

it('journal failure after an acknowledged batch stops further transport batches', async () => {
 let saves = 0
 const store = {getItem:()=>null,setItem:()=>{if (++saves >= 4) throw new Error('quota')},removeItem:()=>{}}
 const sendBatch = vi.fn(async (request: {operationId:string; rows:unknown[]}) => ({operationId:request.operationId, applied:request.rows.length}))
 render(<ImportDialog {...props} storage={store} transport={{sendBatch,outcome:vi.fn()}} />)
 choose(file('name,email\n'+Array.from({length:513},(_,i)=>`name${i},mail${i}@x`).join('\n')+'\n','batches.csv'))
 await screen.findByText('mail0@x')
 fireEvent.click(screen.getByText('Import',{selector:'button'}))
 await screen.findByText(/Import journal could not be saved/)
 await new Promise(resolve=>setTimeout(resolve,0))
 expect(sendBatch).toHaveBeenCalledTimes(1)
})
