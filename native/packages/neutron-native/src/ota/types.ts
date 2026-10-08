/**
 * OTA Update types.
 */

/** Configuration from neutron.config.ts */
export interface NativeOTAConfig {
  /** Update server endpoint */
  endpoint: string
  /** Native-provisioned Ed25519 key: base64 raw 32 bytes or SPKI DER/PEM */
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
  /** SHA-256 of the sorted assembled file inventory (see native-ota protocol) */
  bundleHash: string
  /** Base64 Ed25519 signature over UTF-8 canonicalManifest(), including bundleHash */
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

/** Native boot code owns this record and marks an attempted launch BEFORE JS starts. */
export interface OTABootState {
  currentUpdateId: string | null
  lastGoodUpdateId: string | null
  pendingUpdateId: string | null
  buildNumber: number
  consecutiveCrashes: number
  launchPending: boolean
}

/** No JS-only fallback: every operation must be backed by native durable storage. */
export interface NativeOTAAdapter {
  runtimeVersion: string
  appVersion: string
  /** Confirms the boot marker is written by native code before loading JS. */
  nativeBootTracking: true
  readBootState(): Promise<OTABootState>
  markHealthy(): Promise<OTABootState>
  recordCrash(): Promise<OTABootState>
  rollback(): Promise<OTABootState>
  verifyManifest(canonicalManifest: string, signature: string, publicKey: string): Promise<boolean>
  sha256(data: ArrayBuffer): Promise<string>
  beginStage(manifest: UpdateManifest): Promise<void>
  stageChunk(updateId: string, path: string, data: ArrayBuffer): Promise<void>
  deleteStagedPath(updateId: string, path: string): Promise<void>
  /** Digest over the fully assembled staged bundle, including unchanged files. */
  stagedBundleHash(updateId: string): Promise<string>
  /** Atomic durable publication and pending metadata, never modifies the running bundle. */
  publishPending(manifest: UpdateManifest): Promise<OTABootState>
  discardStage(updateId: string): Promise<void>
  /** Resolves only after native reload acceptance; rejects unsupported hosts. */
  reload(updateId: string): Promise<void>
}
