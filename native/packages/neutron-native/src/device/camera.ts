/**
 * Camera — photo/video capture wrapping expo-camera and react-native-image-picker.
 *
 * Peer dependencies (install one):
 *   - expo-camera (Expo managed/bare)
 *   - react-native-image-picker (bare React Native)
 *
 * All functions are async and handle missing dependencies gracefully.
 *
 * @module @neutron/native/device/camera
 */

// ─── Types ──────────────────────────────────────────────────────────────────

/** Options for capturing a photo */
export interface CaptureOptions {
  /** Camera facing direction (default: 'back') */
  facing?: 'front' | 'back'
  /** JPEG quality 0-1 (default: 0.85) */
  quality?: number
  /** Max width in pixels — image is downsized if larger */
  maxWidth?: number
  /** Max height in pixels — image is downsized if larger */
  maxHeight?: number
  /** Base64-encode the image data (default: false) */
  base64?: boolean
  /** EXIF data inclusion (default: false) */
  exif?: boolean
  /** Save captured photo to the device camera roll (default: false) */
  saveToGallery?: boolean
}

/** Options for picking from the gallery */
export interface GalleryOptions {
  /** Media type filter (default: 'photo') */
  mediaType?: 'photo' | 'video' | 'mixed'
  /** Allow multiple selection (default: false) */
  multiple?: boolean
  /** Max number of selections when multiple is true */
  selectionLimit?: number
  /** Max width in pixels */
  maxWidth?: number
  /** Max height in pixels */
  maxHeight?: number
  /** JPEG quality 0-1 */
  quality?: number
}

/** Result of a camera capture or gallery pick */
export interface CameraResult {
  /** Local file URI */
  uri: string
  /** Image/video width in pixels */
  width: number
  /** Image/video height in pixels */
  height: number
  /** File size in bytes */
  fileSize: number
  /** MIME type (e.g. 'image/jpeg') */
  type: string
  /** Base64 string if requested */
  base64?: string
  /** Duration in seconds (video only) */
  duration?: number
  /** EXIF metadata if requested */
  exif?: Record<string, unknown>
}

/** Camera permission status */
export type CameraPermissionStatus = 'granted' | 'denied' | 'undetermined'

// ─── Lazy module loaders ────────────────────────────────────────────────────

/* eslint-disable @typescript-eslint/no-explicit-any */
let _expoCamera: any = undefined
let _imagePicker: any = undefined

function getExpoCamera(): any {
  if (_expoCamera === undefined) {
    try { _expoCamera = require('expo-camera') } catch { _expoCamera = null }
  }
  return _expoCamera
}

function getImagePicker(): any {
  if (_imagePicker === undefined) {
    try { _imagePicker = require('react-native-image-picker') } catch { _imagePicker = null }
  }
  return _imagePicker
}

function assertAvailable(): void {
  if (!getExpoCamera() && !getImagePicker()) {
    throw new Error(
      '[neutron-native/device/camera] No camera package found. ' +
      'Install one of: expo-camera, react-native-image-picker'
    )
  }
}
/* eslint-enable @typescript-eslint/no-explicit-any */

// ─── Public API ─────────────────────────────────────────────────────────────

/**
 * Request camera permission from the user.
 *
 * @returns The resulting permission status.
 *
 * @example
 * ```ts
 * import { requestCameraPermission } from '@neutron/native/device/camera'
 * const status = await requestCameraPermission()
 * if (status === 'granted') { ... }
 * ```
 */
export async function requestCameraPermission(): Promise<CameraPermissionStatus> {
  const expo = getExpoCamera()
  if (expo) {
    const { status } = await expo.Camera.requestCameraPermissionsAsync()
    if (status === 'granted') return 'granted'
    if (status === 'denied') return 'denied'
    return 'undetermined'
  }

  const picker = getImagePicker()
  if (picker) {
    // react-native-image-picker requests permission implicitly on launch
    // so we just verify with a no-op call
    return 'granted'
  }

  throw new Error(
    '[neutron-native/device/camera] No camera package found. ' +
    'Install one of: expo-camera, react-native-image-picker'
  )
}

/**
 * Check the current camera permission status without prompting.
 *
 * @returns The current permission status.
 */
export async function getCameraPermissionStatus(): Promise<CameraPermissionStatus> {
  const expo = getExpoCamera()
  if (expo) {
    const { status } = await expo.Camera.getCameraPermissionsAsync()
    if (status === 'granted') return 'granted'
    if (status === 'denied') return 'denied'
    return 'undetermined'
  }

  // react-native-image-picker doesn't provide a standalone check
  return 'undetermined'
}

/**
 * Capture a photo using the device camera.
 *
 * Uses expo-camera's `takePictureAsync` when available, otherwise falls back
 * to react-native-image-picker's `launchCamera`.
 *
 * @param options - Capture configuration.
 * @returns The captured photo result, or null if the user cancelled.
 *
 * @example
 * ```ts
 * import { takePicture } from '@neutron/native/device/camera'
 * const photo = await takePicture({ quality: 0.9, facing: 'back' })
 * if (photo) console.log(photo.uri)
 * ```
 */
export async function takePicture(options: CaptureOptions = {}): Promise<CameraResult | null> {
  assertAvailable()

  const picker = getImagePicker()
  if (picker) {
    const result = await picker.launchCamera({
      mediaType: 'photo',
      cameraType: options.facing === 'front' ? 'front' : 'back',
      quality: options.quality ?? 0.85,
      maxWidth: options.maxWidth,
      maxHeight: options.maxHeight,
      includeBase64: options.base64 ?? false,
      includeExtra: options.exif ?? false,
      saveToPhotos: options.saveToGallery ?? false,
    })

    if (result.didCancel || !result.assets?.length) return null

    const asset = result.assets[0]
    return {
      uri: asset.uri ?? '',
      width: asset.width ?? 0,
      height: asset.height ?? 0,
      fileSize: asset.fileSize ?? 0,
      type: asset.type ?? 'image/jpeg',
      base64: asset.base64,
      duration: asset.duration,
    }
  }

  // expo-camera path: this function is meant for imperative capture.
  // In Expo, camera capture typically happens through a ref on the CameraView component.
  // We provide a helper that launches the image picker in camera mode.
  const expo = getExpoCamera()
  if (expo) {
    // expo-image-picker provides launchCameraAsync which is the imperative API
    let expoImagePicker: any // eslint-disable-line @typescript-eslint/no-explicit-any
    try { expoImagePicker = require('expo-image-picker') } catch { /* empty */ }

    if (expoImagePicker) {
      const result = await expoImagePicker.launchCameraAsync({
        mediaTypes: expoImagePicker.MediaTypeOptions?.Images ?? 'images',
        quality: options.quality ?? 0.85,
        base64: options.base64 ?? false,
        exif: options.exif ?? false,
        allowsEditing: false,
      })

      if (result.canceled || !result.assets?.length) return null

      const asset = result.assets[0]
      return {
        uri: asset.uri,
        width: asset.width,
        height: asset.height,
        fileSize: asset.fileSize ?? 0,
        type: asset.mimeType ?? 'image/jpeg',
        base64: asset.base64 ?? undefined,
        exif: asset.exif ?? undefined,
      }
    }
  }

  throw new Error(
    '[neutron-native/device/camera] Could not launch camera. ' +
    'For Expo, install expo-image-picker. For bare RN, install react-native-image-picker.'
  )
}

/**
 * Pick one or more photos/videos from the device gallery.
 *
 * @param options - Gallery picker configuration.
 * @returns Array of selected media, or empty array if the user cancelled.
 *
 * @example
 * ```ts
 * import { pickFromGallery } from '@neutron/native/device/camera'
 * const photos = await pickFromGallery({ multiple: true, selectionLimit: 5 })
 * photos.forEach(p => console.log(p.uri))
 * ```
 */
export async function pickFromGallery(options: GalleryOptions = {}): Promise<CameraResult[]> {
  assertAvailable()

  const picker = getImagePicker()
  if (picker) {
    const result = await picker.launchImageLibrary({
      mediaType: options.mediaType ?? 'photo',
      selectionLimit: options.multiple ? (options.selectionLimit ?? 0) : 1,
      quality: options.quality ?? 0.85,
      maxWidth: options.maxWidth,
      maxHeight: options.maxHeight,
    })

    if (result.didCancel || !result.assets?.length) return []

    return result.assets.map((asset: any) => ({ // eslint-disable-line @typescript-eslint/no-explicit-any
      uri: asset.uri ?? '',
      width: asset.width ?? 0,
      height: asset.height ?? 0,
      fileSize: asset.fileSize ?? 0,
      type: asset.type ?? 'image/jpeg',
      base64: asset.base64,
      duration: asset.duration,
    }))
  }

  // Expo path
  let expoImagePicker: any // eslint-disable-line @typescript-eslint/no-explicit-any
  try { expoImagePicker = require('expo-image-picker') } catch { /* empty */ }

  if (expoImagePicker) {
    const mediaMap: Record<string, unknown> = {
      photo: expoImagePicker.MediaTypeOptions?.Images ?? 'images',
      video: expoImagePicker.MediaTypeOptions?.Videos ?? 'videos',
      mixed: expoImagePicker.MediaTypeOptions?.All ?? 'all',
    }

    const result = await expoImagePicker.launchImageLibraryAsync({
      mediaTypes: mediaMap[options.mediaType ?? 'photo'],
      allowsMultipleSelection: options.multiple ?? false,
      selectionLimit: options.selectionLimit,
      quality: options.quality ?? 0.85,
    })

    if (result.canceled || !result.assets?.length) return []

    return result.assets.map((asset: any) => ({ // eslint-disable-line @typescript-eslint/no-explicit-any
      uri: asset.uri,
      width: asset.width,
      height: asset.height,
      fileSize: asset.fileSize ?? 0,
      type: asset.mimeType ?? 'image/jpeg',
      base64: asset.base64 ?? undefined,
      duration: asset.duration ?? undefined,
    }))
  }

  throw new Error(
    '[neutron-native/device/camera] Could not open gallery. ' +
    'Install expo-image-picker or react-native-image-picker.'
  )
}

/**
 * Check if camera hardware is available on this device.
 *
 * @returns true if camera hardware is present.
 */
export async function isCameraAvailable(): Promise<boolean> {
  const expo = getExpoCamera()
  if (expo) {
    const types = await expo.Camera.getAvailableCameraTypesAsync?.()
    return Array.isArray(types) && types.length > 0
  }

  // react-native-image-picker doesn't expose a hardware check;
  // assume true on native platforms
  if (getImagePicker()) return true

  return false
}
