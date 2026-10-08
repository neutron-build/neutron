import { test } from 'node:test'
import assert from 'node:assert/strict'
import { OTAClient, canonicalManifest } from '../packages/neutron-native/src/ota/client.js'
import { getModule, hasModule, clearCache, registerModule } from '../packages/neutron-native/src/turbomodule/registry.js'
import type { NativeOTAAdapter, OTABootState, UpdateManifest } from '../packages/neutron-native/src/ota/types.js'
const hash = 'a'.repeat(64)
const config = { endpoint: 'https://updates.example', publicKey: 'trusted', checkInterval: 0, channel: 'production', updateStrategy: 'next-launch' as const }
function harness() {
  let boot: OTABootState = { currentUpdateId: null, lastGoodUpdateId: null, pendingUpdateId: null, buildNumber: 0, consecutiveCrashes: 0, launchPending: false }
  const calls: string[] = []
  const adapter: NativeOTAAdapter = {
    runtimeVersion: '0.76.0', appVersion: '1.0.0', nativeBootTracking: true,
    readBootState: async () => ({...boot}), markHealthy: async () => ({...boot, launchPending:false, consecutiveCrashes:0}),
    recordCrash: async () => (boot = {...boot, consecutiveCrashes:boot.consecutiveCrashes+1}),
    rollback: async () => (boot = {...boot, currentUpdateId:boot.lastGoodUpdateId, pendingUpdateId:null, consecutiveCrashes:0}),
    verifyManifest: async (body, sig, key) => { calls.push('verify'); return body === canonicalManifest(manifest) && sig === 'valid' && key === 'trusted' },
    sha256: async () => hash, beginStage: async () => { calls.push('begin') },
    stageChunk: async () => { calls.push('chunk') }, deleteStagedPath: async () => { calls.push('delete') },
    stagedBundleHash: async () => hash, publishPending: async m => (boot = {...boot, pendingUpdateId:m.id, buildNumber:m.buildNumber}),
    discardStage: async () => { calls.push('discard') }, reload: async () => { calls.push('reload') },
  }
  const manifest: UpdateManifest = { id:'update1', version:'1.0.1', buildNumber:1, runtimeVersion:'0.76.0', channel:'production', bundleHash:hash,
    signature:'valid', downloadSize:1, createdAt:'2026-10-07', chunks:[{path:'bundle.js', hash, size:1, url:'https://updates.example/chunk', operation:'add'}, {path:'old.js', hash:'', size:0, url:'', operation:'delete'}] }
  const original = globalThis.fetch
  globalThis.fetch = async input => new Response(String(input).endsWith('/check') ? JSON.stringify(manifest) : new Uint8Array([1]))
  return { manifest, adapter, calls, client:new OTAClient(config, adapter), restore:() => { globalThis.fetch = original } }
}
test('callable JSI proxy and has/get use one resolver', () => {
  clearCache(); const module = { moduleName:'native', methods:[] }; (globalThis as any).__turboModuleProxy = (name:string) => name === 'Native' ? module : null
  assert.equal(hasModule('Native'),true); assert.equal(getModule('Native'),module)
  registerModule('Fallback',() => ({moduleName:'fallback', methods:[]})); assert.equal(getModule('Fallback')?.moduleName,'fallback')
  delete (globalThis as any).__turboModuleProxy; clearCache()
})
test('missing native adapter fails before fetch', async () => {
  await assert.rejects(new OTAClient(config).checkForUpdate(), /unsupported/)
})
test('signed staged update applies deletes and confirms publication', async () => {
  const h=harness(); try { await h.client.checkForUpdate(); assert.equal(await h.client.downloadAndApply(),true); assert.ok(h.calls.includes('delete')); assert.equal(h.calls[0],'verify') } finally { h.restore() }
})
for (const defect of ['signature','channel','runtimeVersion','buildNumber','path','size','verifier','digest','storage','reload'] as const) {
  test(`rejects ${defect}`, async () => {
    const h=harness(); try {
      if (defect === 'signature') h.manifest.signature=undefined
      if (defect === 'channel') h.manifest.channel='other'
      if (defect === 'runtimeVersion') h.manifest.runtimeVersion='other'
      if (defect === 'buildNumber') h.manifest.buildNumber=0
      if (defect === 'path') h.manifest.chunks[0].path='../escape'
      if (defect === 'size') h.manifest.chunks[0].size=2
      if (defect === 'verifier') h.adapter.verifyManifest=async()=>false
      if (['digest','storage','reload'].includes(defect)) {
        await h.client.checkForUpdate()
        if (defect==='digest') h.adapter.stagedBundleHash=async()=> 'b'.repeat(64)
        if (defect==='storage') h.adapter.stageChunk=async()=> { throw new Error('storage failed') }
        if (defect==='reload') { h.client=new OTAClient({...config,updateStrategy:'immediate'},h.adapter); await h.client.checkForUpdate(); h.adapter.reload=async()=> { throw new Error('reload failed') } }
        await assert.rejects(h.client.downloadAndApply())
      } else { await assert.rejects(h.client.checkForUpdate()); assert.ok(!h.calls.includes('begin')) }
    } finally { h.restore() }
  })
}
test('native crash state survives new clients; rollback requires confirmation', async () => {
  const h=harness(); try {
    await h.client.recordCrash(); await new OTAClient(config,h.adapter).recordCrash()
    assert.equal(await new OTAClient(config,h.adapter).recordCrash(),true)
    h.adapter.rollback=async()=>({...await h.adapter.readBootState(), pendingUpdateId:'bad'})
    await assert.rejects(h.client.rollback(),/not confirmed/)
  } finally { h.restore() }
})

test('startup without adapter publishes an error for the existing initOTA caller',async()=>{
  const client=new OTAClient(config)
  await client.start()
  assert.equal(client.getState().status,'error')
  assert.match(client.getState().error!,/unsupported/)
})

test('real Ed25519 authentication rejects signed-field tampering and a wrong trusted key',async()=>{
  const {generateKeyPairSync,sign,verify}=await import('node:crypto')
  const trusted=generateKeyPairSync('ed25519'), wrong=generateKeyPairSync('ed25519')
  const publicKey=trusted.publicKey.export({type:'spki',format:'pem'}).toString()
  const h=harness()
  try {
    h.manifest.signature=sign(null,Buffer.from(canonicalManifest(h.manifest)),trusted.privateKey).toString('base64')
    h.adapter.verifyManifest=async(data,signature,key)=>verify(null,Buffer.from(data),key,Buffer.from(signature,'base64'))
    h.client=new OTAClient({...config,publicKey},h.adapter)
    assert.equal((await h.client.checkForUpdate())?.id,'update1')
    h.manifest.version='9.9.9'
    await assert.rejects(h.client.checkForUpdate(),/signature verification failed/)
    h.manifest.version='1.0.1'
    const badKey=wrong.publicKey.export({type:'spki',format:'pem'}).toString()
    await assert.rejects(new OTAClient({...config,publicKey:badKey},h.adapter).checkForUpdate(),/signature verification failed/)
    assert.ok(!h.calls.includes('begin'))
  } finally {h.restore()}
})

test('stale startup rejection cannot stop the new generation or leak polling timers', async () => {
  const h = harness(), intervals = new Set<number>()
  const realSet = globalThis.setInterval, realClear = globalThis.clearInterval
  let sequence = 0, rejectOld!: (error: Error) => void, reads = 0
  const read = h.adapter.readBootState
  globalThis.setInterval = ((() => { const id = ++sequence; intervals.add(id); return id }) as unknown) as typeof setInterval
  globalThis.clearInterval = ((id: number) => intervals.delete(id)) as unknown as typeof clearInterval
  h.adapter.readBootState = () => ++reads === 1 ? new Promise((_resolve, reject) => { rejectOld = reject }) : read()
  const client = new OTAClient({ ...config, checkInterval: 60 }, h.adapter)
  try {
    const old = client.start(); client.stop(); await client.start()
    assert.equal(intervals.size, 1); rejectOld(new Error('old failure')); await old
    assert.equal(client.getState().status, 'available'); await client.start(); assert.equal(intervals.size, 1)
    client.stop(); assert.equal(intervals.size, 0)
  } finally { client.stop(); globalThis.setInterval = realSet; globalThis.clearInterval = realClear; h.restore() }
})

test('native signing vector matches canonical fields, key encoding and assembled inventory', async () => {
  const { readFileSync } = await import('node:fs'), { createHash, createPublicKey, verify } = await import('node:crypto')
  const path = await import('node:path')
  const root = path.resolve(__dirname, '../modules/neutron-ota/tests/fixtures')
  const vector = JSON.parse(readFileSync(path.join(root, 'vector.json'), 'utf8'))
  assert.equal(canonicalManifest(vector.manifest), vector.canonical)
  assert.equal(verify(null, Buffer.from(vector.canonical), vector.publicKeyPEM, Buffer.from(vector.manifest.signature, 'base64')), true)
  const der = createPublicKey(vector.publicKeyPEM).export({ type: 'spki', format: 'der' })
  assert.equal(der.subarray(0, 12).toString('hex'), '302a300506032b6570032100')
  assert.equal(der.subarray(12).toString('base64'), vector.publicKey)
  const sha = (bytes: Uint8Array) => createHash('sha256').update(bytes).digest('hex')
  const inventory = 'assets/retained.txt\0' + sha(readFileSync(path.join(root, 'packaged/assets/retained.txt'))) + '\n' + 'index.jsbundle\0' + sha(Buffer.from(vector.chunkBase64, 'base64')) + '\n'
  assert.equal(sha(Buffer.from(inventory)), vector.manifest.bundleHash)
  assert.equal(verify(null, Buffer.from(vector.canonical + ' '), vector.publicKeyPEM, Buffer.from(vector.manifest.signature, 'base64')), false)
})

test('typed production entry uses public TurboModuleRegistry and transports native bytes', async () => {
  const { createNativeOTAAdapter } = await import('../packages/neutron-native/src/ota/native-adapter.js')
  assert.throws(() => createNativeOTAAdapter(), /Missing public RN module/)
  const h = harness(), boot = JSON.stringify(await h.adapter.readBootState())
  let received: string | undefined
  ;(globalThis as any).__platformHostModules = { NeutronOTA: {
    ...Object.fromEntries(['readBootState', 'markHealthy', 'recordCrash', 'rollback', 'verifyManifest', 'sha256', 'beginStage', 'stageChunk', 'deleteStagedPath', 'stagedBundleHash', 'publishPending', 'discardStage', 'reload'].map(name => [name, async () => {}])),
    getConstants: () => ({ runtimeVersion: '0.76.0', appVersion: '1.0.0', nativeBootTracking: true }),
    readBootState: async () => boot, stageChunk: async (_id: string, _path: string, bytes: string) => { received = bytes },
  } }
  try {
    const adapter = createNativeOTAAdapter(); assert.deepEqual(await adapter.readBootState(), JSON.parse(boot))
    await adapter.stageChunk('update1', 'bundle.js', new Uint8Array([0, 1, 254, 255]).buffer)
    assert.equal(received, Buffer.from([0, 1, 254, 255]).toString('base64'))
  } finally { delete (globalThis as any).__platformHostModules; h.restore() }
})
