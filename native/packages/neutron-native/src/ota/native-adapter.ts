/** Public RN module lookup, with no private JSI/global adapter installation. */
import { TurboModuleRegistry, type TurboModule } from 'react-native'
import type { NativeOTAAdapter, OTABootState } from './types.js'
import { canonicalManifest } from './client.js'
interface Module extends TurboModule {
  getConstants(): { runtimeVersion: string, appVersion: string, nativeBootTracking: boolean }
  readBootState(): Promise<string>; markHealthy(): Promise<string>; recordCrash(): Promise<string>; rollback(): Promise<string>
  verifyManifest(canonical: string, signature: string, key: string): Promise<boolean>
  sha256(base64: string): Promise<string>
  beginStage(manifest: string, canonical: string): Promise<void>
  stageChunk(id: string, path: string, base64: string): Promise<void>
  deleteStagedPath(id: string, path: string): Promise<void>; stagedBundleHash(id: string): Promise<string>
  publishPending(manifest: string, canonical: string): Promise<string>
  discardStage(id: string): Promise<void>; reload(id: string): Promise<void>
}
function base64(data: ArrayBuffer): string {
  const bytes = new Uint8Array(data), alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'
  const parts: string[] = []
  for (let i = 0; i < bytes.length; i += 3) {
    const n = (bytes[i] << 16) | ((bytes[i + 1] ?? 0) << 8) | (bytes[i + 2] ?? 0)
    parts.push(alphabet[n >>> 18], alphabet[(n >>> 12) & 63], i + 1 < bytes.length ? alphabet[(n >>> 6) & 63] : '=', i + 2 < bytes.length ? alphabet[n & 63] : '=')
  }
  return parts.join('')
}
export function createNativeOTAAdapter(): NativeOTAAdapter {
  const module = TurboModuleRegistry.getEnforcing<Module>('NeutronOTA'), constants = module.getConstants()
  const methods = ['readBootState', 'markHealthy', 'recordCrash', 'rollback', 'verifyManifest', 'sha256', 'beginStage', 'stageChunk', 'deleteStagedPath', 'stagedBundleHash', 'publishPending', 'discardStage', 'reload'] as const
  if (!constants.nativeBootTracking || !constants.runtimeVersion || !constants.appVersion || methods.some(name => typeof module[name] !== 'function')) throw new Error('NeutronOTA must expose its complete module and be configured before native JS boot')
  const state = async (result: Promise<string>) => JSON.parse(await result) as OTABootState
  return { ...constants, nativeBootTracking: true,
    readBootState: () => state(module.readBootState()), markHealthy: () => state(module.markHealthy()),
    recordCrash: () => state(module.recordCrash()), rollback: () => state(module.rollback()),
    verifyManifest: (data, signature, key) => module.verifyManifest(data, signature, key),
    sha256: data => module.sha256(base64(data)),
    beginStage: manifest => module.beginStage(JSON.stringify(manifest), canonicalManifest(manifest)),
    stageChunk: (id, path, data) => module.stageChunk(id, path, base64(data)),
    deleteStagedPath: (id, path) => module.deleteStagedPath(id, path), stagedBundleHash: id => module.stagedBundleHash(id),
    publishPending: manifest => state(module.publishPending(JSON.stringify(manifest), canonicalManifest(manifest))),
    discardStage: id => module.discardStage(id), reload: id => module.reload(id) }
}
