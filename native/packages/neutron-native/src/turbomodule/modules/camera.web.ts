/**
 * Camera TurboModule — web implementation using getUserMedia + Canvas.
 *
 * Captures a single frame from the camera stream by:
 *   1. Opening a MediaStream via navigator.mediaDevices.getUserMedia
 *   2. Drawing the video frame onto a hidden <canvas>
 *   3. Converting to a data URL (JPEG or PNG)
 *   4. Stopping all tracks immediately
 *
 * Browser support: Chrome 53+, Firefox 36+, Safari 11+, Edge 12+
 */

import type { CameraModule, CameraOptions } from './camera.js'
import type { CaptureResult, NativeResult, ModuleMethod } from '../types.js'
import { registerWebModule } from '../registry.js'

const METHODS: readonly ModuleMethod[] = [
  { name: 'capture', kind: 'async' },
  { name: 'pickFromGallery', kind: 'async' },
  { name: 'isAvailable', kind: 'sync' },
  { name: 'checkPermission', kind: 'async' },
] as const

const WEB_CAMERA: CameraModule = {
  moduleName: 'NeutronCamera',
  methods: METHODS,

  async capture(options?: CameraOptions): Promise<NativeResult<CaptureResult>> {
    if (typeof navigator === 'undefined' || !navigator.mediaDevices?.getUserMedia) {
      return { ok: false, error: { code: 'UNAVAILABLE', message: 'getUserMedia not supported in this browser' } }
    }

    try {
      const stream = await navigator.mediaDevices.getUserMedia({
        video: {
          facingMode: options?.facing === 'front' ? 'user' : 'environment',
          width: { ideal: options?.maxWidth || 1280 },
          height: { ideal: options?.maxHeight || 720 },
        },
      })

      // Create a hidden video element and wait for it to produce frames
      const video = document.createElement('video')
      video.srcObject = stream
      video.setAttribute('playsinline', 'true') // Required for iOS Safari
      await video.play()

      // Wait a frame for the video dimensions to settle
      await new Promise((r) => requestAnimationFrame(r))

      const canvas = document.createElement('canvas')
      canvas.width = video.videoWidth
      canvas.height = video.videoHeight
      const ctx = canvas.getContext('2d')
      if (!ctx) {
        stream.getTracks().forEach((t) => t.stop())
        return { ok: false, error: { code: 'UNAVAILABLE', message: 'Canvas 2D context unavailable' } }
      }
      ctx.drawImage(video, 0, 0)

      // Stop the camera stream immediately
      stream.getTracks().forEach((t) => t.stop())

      const quality = options?.quality ?? 0.85
      const mimeType = options?.mediaType === 'photo' || !options?.mediaType ? 'image/jpeg' : 'image/jpeg'
      const dataUrl = canvas.toDataURL(mimeType, quality)

      return {
        ok: true,
        value: {
          uri: dataUrl,
          width: canvas.width,
          height: canvas.height,
          fileSize: Math.round(dataUrl.length * 0.75), // Approximate decoded size
          type: 'photo',
        },
      }
    } catch (err: unknown) {
      const error = err instanceof Error ? err : new Error(String(err))
      const code = error.name === 'NotAllowedError' ? 'PERMISSION_DENIED'
        : error.name === 'NotFoundError' ? 'UNAVAILABLE'
        : 'UNAVAILABLE'
      return { ok: false, error: { code, message: error.message } }
    }
  },

  async pickFromGallery(): Promise<NativeResult<CaptureResult[]>> {
    // Use a hidden file input to let the user pick images
    return new Promise((resolve) => {
      const input = document.createElement('input')
      input.type = 'file'
      input.accept = 'image/*,video/*'
      input.multiple = true

      input.onchange = async () => {
        const files = input.files
        if (!files || files.length === 0) {
          resolve({ ok: true, value: [] })
          return
        }

        const results: CaptureResult[] = []
        for (let i = 0; i < files.length; i++) {
          const file = files[i]
          const uri = URL.createObjectURL(file)

          // Get dimensions for images
          let width = 0
          let height = 0
          if (file.type.startsWith('image/')) {
            try {
              const dims = await getImageDimensions(uri)
              width = dims.width
              height = dims.height
            } catch { /* dimensions unknown */ }
          }

          results.push({
            uri,
            width,
            height,
            fileSize: file.size,
            type: file.type.startsWith('video/') ? 'video' : 'photo',
          })
        }

        resolve({ ok: true, value: results })
      }

      // Handle cancel — the change event won't fire
      input.oncancel = () => resolve({ ok: true, value: [] })

      input.click()
    })
  },

  isAvailable(): boolean {
    return typeof navigator !== 'undefined'
      && typeof navigator.mediaDevices !== 'undefined'
      && typeof navigator.mediaDevices.getUserMedia === 'function'
  },

  async checkPermission(): Promise<'granted' | 'denied' | 'not-determined'> {
    try {
      const result = await navigator.permissions.query({ name: 'camera' as PermissionName })
      if (result.state === 'granted') return 'granted'
      if (result.state === 'denied') return 'denied'
      return 'not-determined'
    } catch {
      return 'not-determined'
    }
  },
}

/** Helper to get image dimensions via an Image element */
function getImageDimensions(uri: string): Promise<{ width: number; height: number }> {
  return new Promise((resolve, reject) => {
    const img = new Image()
    img.onload = () => resolve({ width: img.naturalWidth, height: img.naturalHeight })
    img.onerror = reject
    img.src = uri
  })
}

// ─── Register web implementation ─────────────────────────────────────────────

registerWebModule('NeutronCamera', () => WEB_CAMERA)
