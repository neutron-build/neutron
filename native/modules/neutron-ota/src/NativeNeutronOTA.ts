import type { TurboModule } from 'react-native'
import { TurboModuleRegistry } from 'react-native'
export interface Spec extends TurboModule {
  getConstants(): { runtimeVersion: string, appVersion: string, nativeBootTracking: boolean }
  readBootState(): Promise<string>
  markHealthy(): Promise<string>
  recordCrash(): Promise<string>
  rollback(): Promise<string>
  verifyManifest(canonical: string, signature: string, publicKey: string): Promise<boolean>
  sha256(bytesBase64: string): Promise<string>
  beginStage(manifestJSON: string, canonical: string): Promise<void>
  stageChunk(id: string, path: string, bytesBase64: string): Promise<void>
  deleteStagedPath(id: string, path: string): Promise<void>
  stagedBundleHash(id: string): Promise<string>
  publishPending(manifestJSON: string, canonical: string): Promise<string>
  discardStage(id: string): Promise<void>
  reload(id: string): Promise<void>
}
export default TurboModuleRegistry.getEnforcing<Spec>('NeutronOTA')
