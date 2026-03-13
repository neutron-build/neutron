/**
 * OTA Update types.
 */

/** Configuration from neutron.config.ts */
export interface NativeOTAConfig {
  /** Update server endpoint */
  endpoint: string
  /** Ed25519 public key for signature verification (base64 or PEM) */
  publicKey?: string
  /** Check interval in seconds (default: 3600) */
  checkInterval: number
  /** Apply strategy */
  updateStrategy: 'next-launch' | 'immediate'
  /** Rollout channel */
  channel: string
}

/** Server response describing an available update */
export interface UpdateManifest {
  /** Unique update ID */
  id: string
  /** Semantic version of the update */
  version: string
  /** Build number — monotonically increasing */
  buildNumber: number
  /** Runtime version this update targets (e.g. '0.76.0') */
  runtimeVersion: string
  /** Channel this update belongs to */
  channel: string
  /** SHA-256 hash of the full bundle */
  bundleHash: string
  /** Ed25519 signature of the bundle hash (base64) */
  signature?: string
  /** Delta chunks — only the files that changed */
  chunks: DeltaChunk[]
  /** Total download size in bytes */
  downloadSize: number
  /** Timestamp (ISO 8601) */
  createdAt: string
  /** Minimum app version required (semver) */
  minAppVersion?: string
  /** Release notes */
  releaseNotes?: string
}

/** A single changed file in a delta update */
export interface DeltaChunk {
  /** Relative path within the bundle */
  path: string
  /** SHA-256 hash of this chunk */
  hash: string
  /** Size in bytes */
  size: number
  /** Download URL for this chunk */
  url: string
  /** Operation type */
  operation: 'add' | 'modify' | 'delete'
}

/** Current update status */
export type UpdateStatus =
  | 'up-to-date'
  | 'checking'
  | 'available'
  | 'downloading'
  | 'downloaded'
  | 'applying'
  | 'error'
  | 'rolled-back'

/** Full OTA state exposed to the app */
export interface OTAState {
  /** Current status */
  status: UpdateStatus
  /** Currently running update ID (null if on original bundle) */
  currentUpdateId: string | null
  /** Available update manifest (null if up-to-date) */
  availableUpdate: UpdateManifest | null
  /** Download progress (0-1) */
  downloadProgress: number
  /** Error message if status is 'error' */
  error: string | null
  /** Whether the current session is the first launch after an update */
  isFirstLaunchAfterUpdate: boolean
  /** Number of consecutive crashes (used for rollback detection) */
  consecutiveCrashes: number
}
