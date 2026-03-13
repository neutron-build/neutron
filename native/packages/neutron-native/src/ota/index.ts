/**
 * OTA (Over-The-Air) Update System
 *
 * Delta update protocol — only downloads changed chunks, not the full bundle.
 * Ed25519 signature verification prevents tampered bundles.
 * Automatic rollback on crash detection.
 *
 * Architecture:
 *   App start → checkForUpdate() → download delta → verify signature
 *   → apply (immediate or next-launch) → rollback if crash detected
 */

export type { NativeOTAConfig, UpdateManifest, UpdateStatus, OTAState } from './types.js'
export { OTAClient } from './client.js'
export { useOTA } from './hooks.js'
