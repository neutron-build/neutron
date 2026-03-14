/**
 * Clipboard TurboModule — web implementation using the Clipboard API.
 *
 * Uses navigator.clipboard (Async Clipboard API) for reading and
 * writing text. Image support uses ClipboardItem where available.
 *
 * Requires a secure context (HTTPS) in production.
 *
 * Browser support: Chrome 66+, Firefox 63+, Safari 13.1+, Edge 79+
 */

import type { ClipboardModule } from './clipboard.js'
import type { NativeSubscription, NativeEventCallback, ModuleMethod } from '../types.js'
import { registerWebModule } from '../registry.js'

const METHODS: readonly ModuleMethod[] = [
  { name: 'getString', kind: 'async' },
  { name: 'setString', kind: 'sync' },
  { name: 'hasString', kind: 'async' },
  { name: 'getImage', kind: 'async' },
  { name: 'setImage', kind: 'sync' },
  { name: 'onChange', kind: 'sync' },
] as const

const WEB_CLIPBOARD: ClipboardModule = {
  moduleName: 'NeutronClipboard',
  methods: METHODS,

  async getString(): Promise<string> {
    if (typeof navigator === 'undefined' || !navigator.clipboard?.readText) {
      return ''
    }
    try {
      return await navigator.clipboard.readText()
    } catch {
      return ''
    }
  },

  setString(text: string): void {
    if (typeof navigator === 'undefined' || !navigator.clipboard?.writeText) {
      return
    }
    // writeText returns a Promise, but the interface is sync.
    // Fire and forget — matches the native module's sync signature.
    navigator.clipboard.writeText(text).catch(() => {})
  },

  async hasString(): Promise<boolean> {
    if (typeof navigator === 'undefined' || !navigator.clipboard?.readText) {
      return false
    }
    try {
      const text = await navigator.clipboard.readText()
      return text.length > 0
    } catch {
      return false
    }
  },

  async getImage(): Promise<string | null> {
    if (typeof navigator === 'undefined' || !navigator.clipboard?.read) {
      return null
    }
    try {
      const items = await navigator.clipboard.read()
      for (const item of items) {
        for (const type of item.types) {
          if (type.startsWith('image/')) {
            const blob = await item.getType(type)
            return await blobToBase64(blob)
          }
        }
      }
      return null
    } catch {
      return null
    }
  },

  setImage(base64: string): void {
    // Writing images via the Clipboard API requires ClipboardItem
    if (typeof ClipboardItem === 'undefined' || !navigator.clipboard?.write) {
      return
    }

    // Convert base64 to blob and write
    try {
      const binary = atob(base64.replace(/^data:image\/\w+;base64,/, ''))
      const bytes = new Uint8Array(binary.length)
      for (let i = 0; i < binary.length; i++) {
        bytes[i] = binary.charCodeAt(i)
      }
      const blob = new Blob([bytes], { type: 'image/png' })
      const item = new ClipboardItem({ 'image/png': blob })
      navigator.clipboard.write([item]).catch(() => {})
    } catch {
      // Silently fail — matches native module behavior
    }
  },

  onChange(callback: NativeEventCallback<{ content: string }>): NativeSubscription {
    // The web platform does not have a reliable clipboard change event.
    // Use a periodic poll as a best-effort approach (every 2s).
    // This is a known limitation compared to native.
    let lastContent = ''
    let active = true

    const poll = async () => {
      if (!active) return
      try {
        const text = await navigator.clipboard.readText()
        if (text !== lastContent) {
          lastContent = text
          callback({ content: text })
        }
      } catch {
        // Permission denied or not focused — skip this cycle
      }
      if (active) {
        setTimeout(poll, 2000)
      }
    }

    // Start polling after a short delay
    setTimeout(poll, 2000)

    return {
      remove() {
        active = false
      },
    }
  },
}

/** Convert a Blob to a base64 data URL string */
function blobToBase64(blob: Blob): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.onloadend = () => resolve(reader.result as string)
    reader.onerror = reject
    reader.readAsDataURL(blob)
  })
}

// ─── Register web implementation ─────────────────────────────────────────────

registerWebModule('NeutronClipboard', () => WEB_CLIPBOARD)
