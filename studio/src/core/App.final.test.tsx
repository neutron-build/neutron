import { beforeEach, afterEach, describe, it, expect, vi } from 'vitest'
import { render, cleanup, waitFor } from '@testing-library/preact'
vi.mock('./Shell',()=>({Shell:()=>null}))
vi.mock('./ConnectionManager',()=>({ConnectionManager:()=>null}))
vi.mock('./CommandPalette',()=>({CommandPalette:()=>null}))
vi.mock('../lib/api',async original=>{const m=await original<typeof import('../lib/api')>();return {...m,api:{...m.api,connections:{list:vi.fn(),connect:vi.fn()},limits:vi.fn().mockRejectedValue(new Error('unused'))}}})
import { App, applyDeepLink } from './App'
import { api } from '../lib/api'
import { connections, activeConnection, tabs, closeTab, schema } from '../lib/store'
const conn=(id:string)=>({id,name:id,url:'pg://test',isNucleus:false})
const result={features:{isNucleus:false,version:'',models:[]},schema:{sql:[]} as never}
beforeEach(()=>{connections.value=[conn('A'),conn('B')];activeConnection.value=null;tabs.value=[];vi.clearAllMocks();vi.mocked(api.connections.connect).mockResolvedValue(result);window.history.replaceState(null,'','#/c/A/sql')})
afterEach(cleanup)
describe('deep-link activation generations',()=>{
 it('back/forward can revisit a link after its tab was closed',async()=>{
  render(<App/>);await waitFor(()=>expect(tabs.value).toHaveLength(1));closeTab(tabs.value[0].id)
  window.history.replaceState(null,'','#/c/A/diagnostics');window.dispatchEvent(new Event('hashchange'))
  await waitFor(()=>expect(tabs.value.some(t=>t.kind==='diagnostics')).toBe(true))
  window.history.replaceState(null,'','#/c/A/sql');window.dispatchEvent(new Event('hashchange'))
  await waitFor(()=>expect(tabs.value.some(t=>t.kind==='sql-editor')).toBe(true))
 })
 it('same link can retry a failed connection',async()=>{
  vi.mocked(api.connections.connect).mockRejectedValueOnce(new Error('offline'))
  render(<App/>);await waitFor(()=>expect(api.connections.connect).toHaveBeenCalledTimes(1))
  window.dispatchEvent(new Event('hashchange'))
  await waitFor(()=>expect(activeConnection.value?.id).toBe('A'))
  expect(tabs.value).toHaveLength(1)
 })
 it('older completion cannot activate A or overwrite B schema',async()=>{
  let finish!:(r:typeof result)=>void
  vi.mocked(api.connections.connect).mockImplementation(id=>id==='A'?new Promise(resolve=>{finish=resolve}):Promise.resolve(result))
  let generation=1
  const a=applyDeepLink('#/c/A/sql',()=>generation===1)
  await waitFor(()=>expect(finish).toBeTypeOf('function'))
  generation=2
  await applyDeepLink('#/c/B/sql',()=>generation===2)
  finish({...result,schema:{sql:[{name:'stale'}]} as never})
  await a
  expect(activeConnection.value?.id).toBe('B')
  expect(schema.value?.sql).toEqual([])
  expect(tabs.value).toHaveLength(1)
 })
})
