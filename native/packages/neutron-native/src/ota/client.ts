/** Authenticated OTA orchestration. Native code owns durable boot, staging and rollback. */
import type { NativeOTAConfig, NativeOTAAdapter, UpdateManifest, OTAState, OTABootState } from './types.js'
const MAX_DOWNLOAD = 64 * 1024 * 1024
const HASH = /^[a-f0-9]{64}$/
const METHODS = ['readBootState', 'markHealthy', 'recordCrash', 'rollback', 'verifyManifest', 'sha256', 'beginStage', 'stageChunk', 'deleteStagedPath', 'stagedBundleHash', 'publishPending', 'discardStage', 'reload'] as const

/** Fixed field order binds routing, rollback policy, paths and payload digests. */
export function canonicalManifest(m: UpdateManifest): string {
  return JSON.stringify({ id: m.id, version: m.version, buildNumber: m.buildNumber,
    runtimeVersion: m.runtimeVersion, channel: m.channel, bundleHash: m.bundleHash,
    downloadSize: m.downloadSize, createdAt: m.createdAt, minAppVersion: m.minAppVersion ?? null,
    chunks: m.chunks.map(c => ({ path: c.path, hash: c.hash, size: c.size, url: c.url, operation: c.operation })) })
}
function version(v: string): number[] {
  if (!/^\d+\.\d+\.\d+$/.test(v)) throw new Error('Unsupported app version format')
  return v.split('.').map(Number)
}
function validateBoot(b: OTABootState): OTABootState {
  if (!b || !Number.isSafeInteger(b.buildNumber) || b.buildNumber < 0
      || !Number.isSafeInteger(b.consecutiveCrashes) || b.consecutiveCrashes < 0
      || typeof b.launchPending !== 'boolean'
      || [b.currentUpdateId, b.lastGoodUpdateId, b.pendingUpdateId].some(id => id !== null && (typeof id !== 'string' || !id))) {
    throw new Error('Invalid durable native boot state')
  }
  return b
}

export class OTAClient {
  private state: OTAState = { status: 'up-to-date', currentUpdateId: null, availableUpdate: null,
    downloadProgress: 0, error: null, isFirstLaunchAfterUpdate: false, consecutiveCrashes: 0 }
  private checkTimer: ReturnType<typeof setInterval> | null = null
  private listeners = new Set<(state: OTAState) => void>()
  private running = false
  private generation = 0
  private checking: { generation: number, task: Promise<UpdateManifest | null> } | null = null
  private applying: Promise<boolean> | null = null

  constructor(private config: NativeOTAConfig, private adapter?: NativeOTAAdapter) {
    this.config = { ...config }
  }
  private native(): NativeOTAAdapter {
    const a = this.adapter ?? (globalThis as any).__neutronOTA
    if (!a || a.nativeBootTracking !== true || !a.runtimeVersion || !a.appVersion
        || METHODS.some(name => typeof a[name] !== 'function')) {
      throw new Error('OTA unsupported: a complete durable native adapter with native boot tracking is required')
    }
    if (!this.config.publicKey?.trim()) throw new Error('OTA requires a trusted public key')
    return a
  }
  private boot(b: OTABootState): void {
    validateBoot(b)
    this.updateState({ currentUpdateId: b.currentUpdateId, consecutiveCrashes: b.consecutiveCrashes,
      isFirstLaunchAfterUpdate: b.launchPending })
  }
  /** Idempotent startup. Failures are published to state; native boot counted failed launches. */
  async start(): Promise<void> {
    if (this.running) return
    this.running = true
    const generation = ++this.generation
    try {
      const a = this.native()
      const b = validateBoot(await a.readBootState())
      if (!this.running || generation !== this.generation) return
      this.boot(b)
      if (b.consecutiveCrashes >= 3) await this.rollback()
      await this.checkForUpdate()
      if (this.running && generation === this.generation && this.config.checkInterval > 0) {
        this.checkTimer = setInterval(() => { void this.checkForUpdate().catch(() => {}) }, this.config.checkInterval * 1000)
      }
    } catch (error) {
      if (generation !== this.generation) return
      this.running = false; this.fail(error)
    }
  }
  stop(): void {
    this.running = false; ++this.generation
    if (this.checkTimer !== null) clearInterval(this.checkTimer)
    this.checkTimer = null
    this.checking = null
  }
  private owned(generation: number): void {
    if (generation !== this.generation) throw new Error('OTA session superseded')
  }
  private async validateManifest(m: UpdateManifest, a: NativeOTAAdapter, b: OTABootState): Promise<void> {
    if (!m || typeof m.id !== 'string' || !/^[A-Za-z0-9_-]{1,128}$/.test(m.id)
      || m.runtimeVersion !== a.runtimeVersion || m.channel !== this.config.channel
      || !Number.isSafeInteger(m.buildNumber) || m.buildNumber <= b.buildNumber
      || !HASH.test(m.bundleHash) || !m.signature || !Array.isArray(m.chunks) || m.chunks.length > 4096
      || !Number.isSafeInteger(m.downloadSize) || m.downloadSize < 0 || m.downloadSize > MAX_DOWNLOAD) {
      throw new Error('Invalid, unsigned, incompatible or rollback OTA manifest')
    }
    if (m.minAppVersion) {
      const current = version(a.appVersion), minimum = version(m.minAppVersion)
      for (let i = 0; i < 3; i++) {
        if (current[i] > minimum[i]) break
        if (current[i] < minimum[i]) throw new Error('OTA requires a newer app version')
      }
    }
    let size = 0
    const paths = new Set<string>()
    for (const c of m.chunks) {
      if (!c || typeof c.path !== 'string' || c.path.length > 1024 || c.path.includes('\\') || c.path.includes('\0')
          || c.path.split('/').some(p => !p || p === '.' || p === '..') || paths.has(c.path)
          || !['add', 'modify', 'delete'].includes(c.operation)
          || !Number.isSafeInteger(c.size) || c.size < 0 || c.size > MAX_DOWNLOAD) throw new Error('Invalid OTA chunk path/operation/size')
      paths.add(c.path)
      if (c.operation === 'delete') { if (c.size !== 0) throw new Error('Delete chunk has payload'); continue }
      if (!HASH.test(c.hash) || new URL(c.url).protocol !== 'https:') throw new Error('Invalid OTA chunk hash or URL')
      size += c.size
    }
    if (size !== m.downloadSize) throw new Error('OTA download size mismatch')
    if (await a.verifyManifest(canonicalManifest(m), m.signature, this.config.publicKey!) !== true) throw new Error('OTA manifest signature verification failed')
  }
  checkForUpdate(): Promise<UpdateManifest | null> {
    const generation = this.generation
    if (this.checking?.generation === generation) return this.checking.task
    const task = this.check(generation)
    this.checking = { generation, task }
    void task.finally(() => { if (this.checking?.task === task) this.checking = null }).catch(() => {})
    return task
  }
  private async check(generation: number): Promise<UpdateManifest | null> {
    this.updateState({ status: 'checking', error: null })
    try {
      const a = this.native(), b = validateBoot(await a.readBootState())
      this.owned(generation); this.boot(b)
      if (new URL(this.config.endpoint).protocol !== 'https:') throw new Error('OTA endpoint must use HTTPS')
      const response = await fetch(`${this.config.endpoint}/check`, { method: 'POST', redirect: 'error',
        headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ currentUpdateId: b.currentUpdateId,
          channel: this.config.channel, runtimeVersion: a.runtimeVersion, appVersion: a.appVersion }) })
      this.owned(generation)
      if (response.status === 304) { this.updateState({ status: 'up-to-date', availableUpdate: null }); return null }
      if (!response.ok) throw new Error(`Update check failed: ${response.status}`)
      const bytes = await this.readBounded(response, 1024 * 1024)
      const m: UpdateManifest = JSON.parse(new TextDecoder().decode(bytes))
      await this.validateManifest(m, a, b)
      this.owned(generation)
      this.updateState({ status: 'available', availableUpdate: m }); return m
    } catch (error) { if (generation === this.generation) { this.updateState({ availableUpdate: null }); this.fail(error) }; throw error }
  }
  downloadAndApply(): Promise<boolean> {
    if (this.applying) return this.applying
    const task = this.download()
    this.applying = task
    void task.finally(() => { if (this.applying === task) this.applying = null }).catch(() => {})
    return task
  }
  private async download(): Promise<boolean> {
    const generation = this.generation
    // Snapshot prevents callers mutating getState() from changing an admitted update.
    const m = this.state.availableUpdate ? JSON.parse(JSON.stringify(this.state.availableUpdate)) as UpdateManifest : null
    if (!m) return false
    let a: NativeOTAAdapter | undefined
    let staged = false
    try {
      a = this.native()
      await this.validateManifest(m, a, validateBoot(await a.readBootState()))
      this.owned(generation)
      this.updateState({ status: 'downloading', downloadProgress: 0 })
      await a.beginStage(m); staged = true
      let downloaded = 0
      for (const c of m.chunks) {
        this.owned(generation)
        if (c.operation === 'delete') { await a.deleteStagedPath(m.id, c.path); continue }
        const response = await fetch(c.url, { redirect: 'error' })
        if (!response.ok) throw new Error(`Chunk download failed: ${response.status}`)
        const data = await this.readBounded(response, c.size)
        if (data.byteLength !== c.size || await a.sha256(data) !== c.hash) throw new Error(`Chunk size/hash mismatch: ${c.path}`)
        this.owned(generation)
        await a.stageChunk(m.id, c.path, data)
        this.owned(generation)
        downloaded += data.byteLength
        this.updateState({ downloadProgress: m.downloadSize ? downloaded / m.downloadSize : 1 })
      }
      if (await a.stagedBundleHash(m.id) !== m.bundleHash) throw new Error('Assembled bundle digest mismatch')
      // Recheck durable monotonic build immediately before atomic publication.
      await this.validateManifest(m, a, validateBoot(await a.readBootState()))
      this.owned(generation)
      const published = validateBoot(await a.publishPending(m))
      if (published.pendingUpdateId !== m.id) throw new Error('Native publication did not confirm pending update')
      staged = false
      this.owned(generation)
      this.boot(published)
      this.updateState({ status: 'downloaded', downloadProgress: 1 })
      if (this.config.updateStrategy === 'immediate') {
        this.updateState({ status: 'applying' }); await a.reload(m.id)
      }
      return true
    } catch (error) {
      if (staged && a) { try { await a.discardStage(m.id) } catch { /* original failure remains authoritative */ } }
      if (generation === this.generation) this.fail(error); throw error
    }
  }
  private async readBounded(response: Response, limit: number): Promise<ArrayBuffer> {
    if (!response.body) throw new Error('Missing OTA response body')
    const reader = response.body.getReader(), chunks: Uint8Array[] = []
    let size = 0
    try {
      while (true) {
        const part = await reader.read()
        if (part.done) break
        size += part.value.byteLength
        if (size > limit) throw new Error('OTA payload exceeds admitted size')
        chunks.push(part.value)
      }
    } catch (error) { await reader.cancel().catch(() => {}); throw error }
    finally { reader.releaseLock() }
    const result = new Uint8Array(size)
    let offset = 0
    for (const chunk of chunks) { result.set(chunk, offset); offset += chunk.byteLength }
    return result.buffer
  }
  async recordCrash(): Promise<boolean> {
    const generation = this.generation
    const b = validateBoot(await this.native().recordCrash()); this.owned(generation); this.boot(b)
    if (b.consecutiveCrashes >= 3) { await this.rollback(); return true }
    return false
  }
  async rollback(): Promise<void> {
    const generation = this.generation
    const a = this.native(), before = validateBoot(await a.readBootState())
    this.owned(generation)
    const after = validateBoot(await a.rollback())
    this.owned(generation)
    if (after.currentUpdateId !== before.lastGoodUpdateId || after.pendingUpdateId !== null || after.consecutiveCrashes !== 0) throw new Error('Native rollback was not confirmed')
    this.boot(after); this.updateState({ status: 'rolled-back' })
  }
  async markSuccessfulLaunch(): Promise<void> {
    const generation = this.generation
    const b = validateBoot(await this.native().markHealthy())
    this.owned(generation)
    if (b.launchPending || b.consecutiveCrashes !== 0) throw new Error('Native healthy launch was not confirmed')
    this.boot(b)
  }
  getState(): Readonly<OTAState> { return this.state }
  subscribe(listener: (state: OTAState) => void): () => void { this.listeners.add(listener); return () => this.listeners.delete(listener) }
  private fail(error: unknown): void { this.updateState({ status: 'error', error: error instanceof Error ? error.message : 'OTA failed' }) }
  private updateState(partial: Partial<OTAState>): void {
    this.state = { ...this.state, ...partial }
    for (const listener of this.listeners) listener(this.state)
  }
}
