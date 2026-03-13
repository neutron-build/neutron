/**
 * Camera TurboModule — photo/video capture via native camera APIs.
 *
 * iOS: AVCaptureSession
 * Android: CameraX
 */

import type { TurboModule, ModuleMethod, CaptureResult, NativeResult } from '../types.js'
import { getModule, registerModule } from '../registry.js'

export interface CameraOptions {
  /** 'front' or 'back' camera (default: 'back') */
  facing?: 'front' | 'back'
  /** Photo quality 0-1 (default: 0.85) */
  quality?: number
  /** Max width/height — downsample if larger */
  maxWidth?: number
  maxHeight?: number
  /** Enable video recording */
  mediaType?: 'photo' | 'video' | 'mixed'
  /** Max video duration in seconds */
  maxDuration?: number
  /** Save to camera roll after capture */
  saveToGallery?: boolean
}

export interface CameraModule extends TurboModule {
  moduleName: 'NeutronCamera'

  /** Open the native camera UI and capture a photo or video */
  capture(options?: CameraOptions): Promise<NativeResult<CaptureResult>>

  /** Pick from the photo library instead of the live camera */
  pickFromGallery(options?: CameraOptions): Promise<NativeResult<CaptureResult[]>>

  /** Check if camera hardware is available */
  isAvailable(): boolean

  /** Check camera permission status without prompting */
  checkPermission(): Promise<'granted' | 'denied' | 'not-determined'>
}

const METHODS: readonly ModuleMethod[] = [
  { name: 'capture', kind: 'async' },
  { name: 'pickFromGallery', kind: 'async' },
  { name: 'isAvailable', kind: 'sync' },
  { name: 'checkPermission', kind: 'async' },
] as const

// Register JS-side stub (throws on call — native module required)
registerModule<CameraModule>('NeutronCamera', () => ({
  moduleName: 'NeutronCamera',
  methods: METHODS,
  async capture() {
    return { ok: false, error: { code: 'UNAVAILABLE', message: 'Camera module not linked' } }
  },
  async pickFromGallery() {
    return { ok: false, error: { code: 'UNAVAILABLE', message: 'Camera module not linked' } }
  },
  isAvailable() { return false },
  async checkPermission() { return 'denied' as const },
}))

/**
 * Hook to access the Camera TurboModule.
 *
 * @example
 * ```tsx
 * const camera = useCamera()
 * const result = await camera.capture({ facing: 'back', quality: 0.9 })
 * if (result.ok) console.log(result.value.uri)
 * ```
 */
export function useCamera(): CameraModule {
  const mod = getModule<CameraModule>('NeutronCamera')
  if (!mod) throw new Error('[neutron-native] NeutronCamera module not available')
  return mod
}
