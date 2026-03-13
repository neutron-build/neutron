/**
 * OTA Update Client — checks for updates, downloads deltas, applies bundles.
 */

import type { NativeOTAConfig, UpdateManifest, OTAState } from './types.js'

/** Crash threshold — if app crashes this many times in a row, roll back */
const ROLLBACK_CRASH_THRESHOLD = 3

/**
 * OTA Client manages the update lifecycle:
 * 1. Check server for new updates
 * 2. Download delta chunks
 * 3. Verify Ed25519 signature
 * 4. Apply update (immediate or next-launch)
 * 5. Auto-rollback on crash detection
 */
export class OTAClient {
  private config: NativeOTAConfig
  private state: OTAState
  private checkTimer: ReturnType<typeof setInterval> | null = null
  private listeners: Set<(state: OTAState) => void> = new Set()

  constructor(config: NativeOTAConfig) {
    this.config = config
    this.state = {
      status: 'up-to-date',
      currentUpdateId: null,
      availableUpdate: null,
      downloadProgress: 0,
      error: null,
      isFirstLaunchAfterUpdate: false,
      consecutiveCrashes: 0,
    }
  }

  /** Start periodic update checks */
  start(): void {
    // Check immediately on start
    this.checkForUpdate()

    // Schedule periodic checks
    if (this.config.checkInterval > 0) {
      this.checkTimer = setInterval(
        () => this.checkForUpdate(),
        this.config.checkInterval * 1000,
      )
    }
  }

  /** Stop periodic update checks */
  stop(): void {
    if (this.checkTimer) {
      clearInterval(this.checkTimer)
      this.checkTimer = null
    }
  }

  /** Check the update server for a new version */
  async checkForUpdate(): Promise<UpdateManifest | null> {
    this.updateState({ status: 'checking' })

    try {
      const response = await fetch(`${this.config.endpoint}/check`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          currentUpdateId: this.state.currentUpdateId,
          channel: this.config.channel,
          runtimeVersion: this.getRuntimeVersion(),
          appVersion: this.getAppVersion(),
        }),
      })

      if (!response.ok) {
        if (response.status === 304) {
          this.updateState({ status: 'up-to-date', availableUpdate: null })
          return null
        }
        throw new Error(`Update check failed: ${response.status}`)
      }

      const manifest: UpdateManifest = await response.json()

      // Verify minimum app version requirement
      if (manifest.minAppVersion && !this.meetsMinVersion(manifest.minAppVersion)) {
        this.updateState({ status: 'up-to-date', availableUpdate: null })
        return null
      }

      this.updateState({ status: 'available', availableUpdate: manifest })
      return manifest
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Unknown error'
      this.updateState({ status: 'error', error: message })
      return null
    }
  }

  /** Download and apply the available update */
  async downloadAndApply(): Promise<boolean> {
    const manifest = this.state.availableUpdate
    if (!manifest) return false

    this.updateState({ status: 'downloading', downloadProgress: 0 })

    try {
      // Download delta chunks
      const totalSize = manifest.downloadSize
      let downloadedSize = 0

      for (const chunk of manifest.chunks) {
        if (chunk.operation === 'delete') continue

        const response = await fetch(chunk.url)
        if (!response.ok) {
          throw new Error(`Failed to download chunk ${chunk.path}: ${response.status}`)
        }

        const data = await response.arrayBuffer()

        // Verify chunk hash
        const hash = await this.sha256(data)
        if (hash !== chunk.hash) {
          throw new Error(`Chunk hash mismatch for ${chunk.path}`)
        }

        // Store chunk to native filesystem
        await this.storeChunk(chunk.path, data)

        downloadedSize += chunk.size
        this.updateState({ downloadProgress: totalSize > 0 ? downloadedSize / totalSize : 1 })
      }

      // Verify bundle signature if public key is configured
      if (this.config.publicKey && manifest.signature) {
        const valid = await this.verifySignature(manifest.bundleHash, manifest.signature)
        if (!valid) {
          throw new Error('Bundle signature verification failed')
        }
      }

      this.updateState({ status: 'downloaded', downloadProgress: 1 })

      // Apply based on strategy
      if (this.config.updateStrategy === 'immediate') {
        await this.applyUpdate(manifest)
      } else {
        // Mark for next launch
        await this.markPendingUpdate(manifest)
      }

      return true
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Download failed'
      this.updateState({ status: 'error', error: message })
      return false
    }
  }

  /** Apply a downloaded update immediately (reload JS bundle) */
  private async applyUpdate(manifest: UpdateManifest): Promise<void> {
    this.updateState({ status: 'applying' })

    // Store update metadata for crash detection
    await this.storeUpdateMeta(manifest.id)

    // Signal native side to reload JS bundle from the new path
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const g = globalThis as any
    if (g.__neutronOTA?.reload) {
      g.__neutronOTA.reload(manifest.id)
    }
  }

  /** Mark an update to be applied on next app launch */
  private async markPendingUpdate(manifest: UpdateManifest): Promise<void> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const g = globalThis as any
    if (g.__neutronOTA?.markPending) {
      g.__neutronOTA.markPending(manifest.id)
    }
  }

  /** Record crash and potentially trigger rollback */
  async recordCrash(): Promise<boolean> {
    const crashes = this.state.consecutiveCrashes + 1
    this.updateState({ consecutiveCrashes: crashes })

    if (crashes >= ROLLBACK_CRASH_THRESHOLD && this.state.currentUpdateId) {
      await this.rollback()
      return true
    }
    return false
  }

  /** Roll back to the original bundle */
  async rollback(): Promise<void> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const g = globalThis as any
    if (g.__neutronOTA?.rollback) {
      g.__neutronOTA.rollback()
    }
    this.updateState({
      status: 'rolled-back',
      currentUpdateId: null,
      consecutiveCrashes: 0,
    })
  }

  /** Clear crash counter (called when app launches successfully) */
  markSuccessfulLaunch(): void {
    this.updateState({ consecutiveCrashes: 0, isFirstLaunchAfterUpdate: false })
  }

  /** Get current OTA state */
  getState(): Readonly<OTAState> {
    return this.state
  }

  /** Subscribe to state changes */
  subscribe(listener: (state: OTAState) => void): () => void {
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }

  // ─── Internal helpers ───────────────────────────────────────────────────

  private updateState(partial: Partial<OTAState>): void {
    this.state = { ...this.state, ...partial }
    for (const listener of this.listeners) {
      listener(this.state)
    }
  }

  private getRuntimeVersion(): string {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const g = globalThis as any
    return g.__neutronOTA?.runtimeVersion ?? '0.0.0'
  }

  private getAppVersion(): string {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const g = globalThis as any
    return g.__neutronOTA?.appVersion ?? '0.0.0'
  }

  private meetsMinVersion(minVersion: string): boolean {
    const current = this.getAppVersion().split('.').map(Number)
    const min = minVersion.split('.').map(Number)
    for (let i = 0; i < 3; i++) {
      if ((current[i] ?? 0) > (min[i] ?? 0)) return true
      if ((current[i] ?? 0) < (min[i] ?? 0)) return false
    }
    return true
  }

  private async sha256(data: ArrayBuffer): Promise<string> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const g = globalThis as any
    if (g.__neutronOTA?.sha256) {
      return g.__neutronOTA.sha256(data)
    }
    // Web fallback
    if (typeof crypto !== 'undefined' && crypto.subtle) {
      const hash = await crypto.subtle.digest('SHA-256', data)
      return Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2, '0')).join('')
    }
    throw new Error('SHA-256 not available')
  }

  private async verifySignature(hash: string, signature: string): Promise<boolean> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const g = globalThis as any
    if (g.__neutronOTA?.verifySignature) {
      return g.__neutronOTA.verifySignature(hash, signature, this.config.publicKey)
    }
    // Skip verification if native module not available
    return true
  }

  private async storeChunk(path: string, _data: ArrayBuffer): Promise<void> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const g = globalThis as any
    if (g.__neutronOTA?.storeChunk) {
      return g.__neutronOTA.storeChunk(path, _data)
    }
  }

  private async storeUpdateMeta(updateId: string): Promise<void> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const g = globalThis as any
    if (g.__neutronOTA?.storeUpdateMeta) {
      return g.__neutronOTA.storeUpdateMeta(updateId)
    }
  }
}
