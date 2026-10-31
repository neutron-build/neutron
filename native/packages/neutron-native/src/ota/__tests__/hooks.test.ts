/**
 * NF-NR-16 regressions — the OTA hook layer: initialization is reachable
 * through the barrel, the hook reads live state, and a replaced client's
 * stale callbacks are fenced away from the successor's state.
 */

import { initOTA, disposeOTA, useOTA } from '../index'
import { OTAClient } from '../client'
import type { OTAState } from '../types'

export {}

const config = { endpoint: 'https://updates.example', publicKey: 'k', checkInterval: 0, channel: 'production', updateStrategy: 'next-launch' as const }

function emitFrom(client: OTAClient, state: OTAState): void {
  ;(client as unknown as { listeners: Set<(s: OTAState) => void> }).listeners.forEach(l => l(state))
}

describe('NF-NR-16 OTA hook layer', () => {
  afterEach(() => disposeOTA())

  it('initOTA is exported through the barrel and starts a client', () => {
    const client = initOTA(config)
    expect(client).toBeInstanceOf(OTAClient)
  })

  it('the current client\'s state updates reach the hook', () => {
    const client = initOTA(config)
    emitFrom(client, { status: 'downloading', currentUpdateId: 'u1', availableUpdate: null, downloadProgress: 0.5, error: null, isFirstLaunchAfterUpdate: false, consecutiveCrashes: 0 })
    const ota = useOTA()
    expect(ota.status).toBe('downloading')
    expect(ota.downloadProgress).toBe(0.5)
  })

  it('a replaced client cannot write state for its successor (fenced callbacks)', () => {
    const first = initOTA(config)
    const second = initOTA(config)
    expect(first).not.toBe(second)
    emitFrom(first, { status: 'error', currentUpdateId: null, availableUpdate: null, downloadProgress: 0, error: 'stale-write', isFirstLaunchAfterUpdate: false, consecutiveCrashes: 0 })
    const ota = useOTA()
    // The successor may report its own environment (no native adapter in
    // tests), but the REPLACED client's write must never appear.
    expect(ota.error).not.toBe('stale-write')
  })

  it('the hook exposes the documented action surface', () => {
    initOTA(config)
    const ota = useOTA()
    expect(typeof ota.checkForUpdate).toBe('function')
    expect(typeof ota.downloadAndApply).toBe('function')
    expect(typeof ota.rollback).toBe('function')
    expect(typeof ota.markSuccessfulLaunch).toBe('function')
  })
})
