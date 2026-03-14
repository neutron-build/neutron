/**
 * Tests for OTA update client.
 */

import { OTAClient } from '../client'
import type { NativeOTAConfig, UpdateManifest } from '../types'

const makeConfig = (overrides: Partial<NativeOTAConfig> = {}): NativeOTAConfig => ({
  endpoint: 'https://ota.example.com',
  channel: 'production',
  checkInterval: 0, // disable periodic checks for tests
  updateStrategy: 'next-launch',
  ...overrides,
})

const makeManifest = (overrides: Partial<UpdateManifest> = {}): UpdateManifest => ({
  id: 'update-123',
  version: '1.1.0',
  buildNumber: 1,
  runtimeVersion: '0.76.0',
  channel: 'production',
  bundleHash: 'abc123',
  downloadSize: 1000,
  chunks: [],
  createdAt: '2026-01-01T00:00:00Z',
  ...overrides,
})

describe('OTAClient', () => {
  let originalFetch: typeof global.fetch

  beforeEach(() => {
    originalFetch = global.fetch
    jest.useFakeTimers()
  })

  afterEach(() => {
    global.fetch = originalFetch
    jest.useRealTimers()
    const g = globalThis as any
    delete g.__neutronOTA
  })

  it('starts in up-to-date state', () => {
    const client = new OTAClient(makeConfig())
    const state = client.getState()
    expect(state.status).toBe('up-to-date')
    expect(state.currentUpdateId).toBeNull()
    expect(state.availableUpdate).toBeNull()
    expect(state.downloadProgress).toBe(0)
    expect(state.error).toBeNull()
    expect(state.consecutiveCrashes).toBe(0)
  })

  it('checkForUpdate fetches from endpoint and returns manifest', async () => {
    const manifest = makeManifest()
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: () => Promise.resolve(manifest),
    })

    const client = new OTAClient(makeConfig())
    const result = await client.checkForUpdate()
    expect(result).toEqual(manifest)
    expect(client.getState().status).toBe('available')
    expect(client.getState().availableUpdate).toEqual(manifest)
  })

  it('checkForUpdate returns null on 304 (not modified)', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      status: 304,
    })

    const client = new OTAClient(makeConfig())
    const result = await client.checkForUpdate()
    expect(result).toBeNull()
    expect(client.getState().status).toBe('up-to-date')
  })

  it('checkForUpdate handles fetch errors gracefully', async () => {
    global.fetch = jest.fn().mockRejectedValue(new Error('Network error'))

    const client = new OTAClient(makeConfig())
    const result = await client.checkForUpdate()
    expect(result).toBeNull()
    expect(client.getState().status).toBe('error')
    expect(client.getState().error).toBe('Network error')
  })

  it('checkForUpdate handles HTTP errors', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      status: 500,
    })

    const client = new OTAClient(makeConfig())
    const result = await client.checkForUpdate()
    expect(result).toBeNull()
    expect(client.getState().status).toBe('error')
    expect(client.getState().error).toContain('500')
  })

  it('checkForUpdate respects minAppVersion', async () => {
    const g = globalThis as any
    g.__neutronOTA = { appVersion: '1.0.0' }

    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: () => Promise.resolve(makeManifest({ minAppVersion: '2.0.0' })),
    })

    const client = new OTAClient(makeConfig())
    const result = await client.checkForUpdate()
    expect(result).toBeNull()
    expect(client.getState().status).toBe('up-to-date')
  })

  it('downloadAndApply returns false when no update available', async () => {
    const client = new OTAClient(makeConfig())
    const result = await client.downloadAndApply()
    expect(result).toBe(false)
  })

  it('downloadAndApply downloads chunks and verifies hashes', async () => {
    const chunkData = new ArrayBuffer(100)
    const manifest = makeManifest({
      downloadSize: 100,
      chunks: [
        { path: 'bundle.js', url: 'https://cdn.example.com/chunk1', size: 100, hash: 'fakehash', operation: 'add' as any },
      ],
    })

    const g = globalThis as any
    g.__neutronOTA = {
      sha256: jest.fn().mockResolvedValue('fakehash'),
      storeChunk: jest.fn().mockResolvedValue(undefined),
      markPending: jest.fn(),
    }

    global.fetch = jest.fn()
      .mockResolvedValueOnce({ ok: true, status: 200, json: () => Promise.resolve(manifest) })
      .mockResolvedValueOnce({ ok: true, arrayBuffer: () => Promise.resolve(chunkData) })

    const client = new OTAClient(makeConfig())
    await client.checkForUpdate()
    const result = await client.downloadAndApply()
    expect(result).toBe(true)
    expect(client.getState().downloadProgress).toBe(1)
    expect(g.__neutronOTA.storeChunk).toHaveBeenCalledWith('bundle.js', chunkData)
  })

  it('downloadAndApply fails on hash mismatch', async () => {
    const manifest = makeManifest({
      downloadSize: 100,
      chunks: [
        { path: 'bad.js', url: 'https://cdn.example.com/bad', size: 100, hash: 'expected', operation: 'add' as any },
      ],
    })

    const g = globalThis as any
    g.__neutronOTA = {
      sha256: jest.fn().mockResolvedValue('different-hash'),
      storeChunk: jest.fn(),
    }

    global.fetch = jest.fn()
      .mockResolvedValueOnce({ ok: true, status: 200, json: () => Promise.resolve(manifest) })
      .mockResolvedValueOnce({ ok: true, arrayBuffer: () => Promise.resolve(new ArrayBuffer(100)) })

    const client = new OTAClient(makeConfig())
    await client.checkForUpdate()
    const result = await client.downloadAndApply()
    expect(result).toBe(false)
    expect(client.getState().status).toBe('error')
    expect(client.getState().error).toContain('hash mismatch')
  })

  it('downloadAndApply skips delete-operation chunks', async () => {
    const manifest = makeManifest({
      downloadSize: 0,
      chunks: [
        { path: 'old.js', url: '', size: 0, hash: '', operation: 'delete' as any },
      ],
    })

    const g = globalThis as any
    g.__neutronOTA = { markPending: jest.fn() }

    global.fetch = jest.fn()
      .mockResolvedValueOnce({ ok: true, status: 200, json: () => Promise.resolve(manifest) })

    const client = new OTAClient(makeConfig())
    await client.checkForUpdate()
    const result = await client.downloadAndApply()
    expect(result).toBe(true)
  })

  it('recordCrash increments crash counter', async () => {
    const client = new OTAClient(makeConfig())
    const rolled = await client.recordCrash()
    expect(rolled).toBe(false)
    expect(client.getState().consecutiveCrashes).toBe(1)
  })

  it('recordCrash triggers rollback after 3 crashes', async () => {
    const g = globalThis as any
    g.__neutronOTA = { rollback: jest.fn() }

    const client = new OTAClient(makeConfig())
    // Simulate having a current update
    ;(client as any).state.currentUpdateId = 'update-1'

    await client.recordCrash() // 1
    await client.recordCrash() // 2
    const rolled = await client.recordCrash() // 3 — triggers rollback
    expect(rolled).toBe(true)
    expect(client.getState().status).toBe('rolled-back')
    expect(client.getState().currentUpdateId).toBeNull()
    expect(client.getState().consecutiveCrashes).toBe(0)
  })

  it('rollback calls native rollback and resets state', async () => {
    const g = globalThis as any
    g.__neutronOTA = { rollback: jest.fn() }

    const client = new OTAClient(makeConfig())
    await client.rollback()
    expect(g.__neutronOTA.rollback).toHaveBeenCalled()
    expect(client.getState().status).toBe('rolled-back')
  })

  it('markSuccessfulLaunch resets crash counter', () => {
    const client = new OTAClient(makeConfig())
    ;(client as any).state.consecutiveCrashes = 2
    ;(client as any).state.isFirstLaunchAfterUpdate = true
    client.markSuccessfulLaunch()
    expect(client.getState().consecutiveCrashes).toBe(0)
    expect(client.getState().isFirstLaunchAfterUpdate).toBe(false)
  })

  it('subscribe notifies on state changes', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: () => Promise.resolve(makeManifest()),
    })

    const client = new OTAClient(makeConfig())
    const states: string[] = []
    const unsub = client.subscribe((state) => states.push(state.status))
    await client.checkForUpdate()
    unsub()
    expect(states).toContain('checking')
    expect(states).toContain('available')
  })

  it('start() calls checkForUpdate immediately', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      status: 304,
    })

    const client = new OTAClient(makeConfig())
    client.start()
    // fetch should have been called
    expect(global.fetch).toHaveBeenCalled()
    client.stop()
  })

  it('start() sets up periodic checks when interval > 0', () => {
    global.fetch = jest.fn().mockResolvedValue({ ok: false, status: 304 })
    const client = new OTAClient(makeConfig({ checkInterval: 60 }))
    client.start()
    expect(global.fetch).toHaveBeenCalledTimes(1)
    jest.advanceTimersByTime(60000)
    expect(global.fetch).toHaveBeenCalledTimes(2)
    client.stop()
  })

  it('stop() clears the periodic timer', () => {
    global.fetch = jest.fn().mockResolvedValue({ ok: false, status: 304 })
    const client = new OTAClient(makeConfig({ checkInterval: 10 }))
    client.start()
    client.stop()
    jest.advanceTimersByTime(20000)
    // Only the initial call should count
    expect(global.fetch).toHaveBeenCalledTimes(1)
  })

  it('meetsMinVersion works correctly', () => {
    const g = globalThis as any
    g.__neutronOTA = { appVersion: '2.1.0' }

    const client = new OTAClient(makeConfig())
    // Access private method via bracket notation
    expect((client as any).meetsMinVersion('1.0.0')).toBe(true)
    expect((client as any).meetsMinVersion('2.1.0')).toBe(true)
    expect((client as any).meetsMinVersion('2.1.1')).toBe(false)
    expect((client as any).meetsMinVersion('3.0.0')).toBe(false)
  })
})
