import { test, afterAll } from 'vitest'
import assert from 'node:assert/strict'
import { neutronFetch } from '../src/bridge'
const original = globalThis.fetch
const originalWindow = Object.getOwnPropertyDescriptor(globalThis, 'window')
afterAll(() => { globalThis.fetch = original; if (originalWindow) Object.defineProperty(globalThis, 'window', originalWindow); else delete (globalThis as any).window })
const captured: Request[] = []
;(globalThis as any).window = { __TAURI__: {} }
globalThis.fetch = async (input, init) => { const request=new Request(input,init); captured.push(request); return new Response('ok') }
test('POST Request retains body, headers and method', async () => {
  const input=new Request('https://example.test/api',{method:'POST',body:'payload',headers:{'X-Custom':'yes'}})
  await neutronFetch(input); const r=captured.pop()!; assert.equal(r.method,'POST'); assert.equal(r.headers.get('X-Custom'),'yes'); assert.equal(await r.text(),'payload')
})
test('explicit overrides and abort propagate', async () => {
  const c=new AbortController(); const input=new Request('https://example.test/api',{method:'POST',body:'payload',signal:c.signal})
  await neutronFetch(input,{headers:{'X-Override':'ok'}}); const r=captured.pop()!; c.abort(); assert.equal(r.signal.aborted,true); assert.equal(r.headers.get('X-Override'),'ok'); assert.equal(await r.text(),'payload')
})
test('string/URL inputs and development capability', async () => {
  await neutronFetch('/api'); assert.equal(captured.pop()!.url,'neutron://localhost/api')
  await neutronFetch(new URL('https://example.test/api')); assert.equal(captured.pop()!.url,'https://example.test/api')
  Object.assign((globalThis as any).window,{__NEUTRON_DEV_MODE__:true,__NEUTRON_DEV_TOKEN__:'secret'})
  await neutronFetch('/api',{method:'POST',body:'body'}); const r=captured.pop()!; assert.equal(r.headers.get('X-Neutron-Dev-Token'),'secret'); assert.equal(await r.text(),'body')
  delete (globalThis as any).window.__NEUTRON_DEV_TOKEN__; await assert.rejects(neutronFetch('/api'),/capability/)
})
