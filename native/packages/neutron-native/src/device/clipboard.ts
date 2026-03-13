/**
 * Clipboard — system clipboard access wrapping @react-native-clipboard/clipboard
 * and expo-clipboard.
 *
 * Peer dependencies (install one):
 *   - @react-native-clipboard/clipboard (community standard)
 *   - expo-clipboard (Expo managed/bare)
 *
 * @module @neutron/native/device/clipboard
 */

// ─── Lazy module loaders ────────────────────────────────────────────────────

/* eslint-disable @typescript-eslint/no-explicit-any */
let _communityClipboard: any = undefined
let _expoClipboard: any = undefined

function getCommunityClipboard(): any {
  if (_communityClipboard === undefined) {
    try { _communityClipboard = require('@react-native-clipboard/clipboard') } catch { _communityClipboard = null }
  }
  // The module exports { default: Clipboard } or Clipboard directly
  if (_communityClipboard) {
    return _communityClipboard.default ?? _communityClipboard
  }
  return null
}

function getExpoClipboard(): any {
  if (_expoClipboard === undefined) {
    try { _expoClipboard = require('expo-clipboard') } catch { _expoClipboard = null }
  }
  return _expoClipboard
}

function assertAvailable(): void {
  if (!getCommunityClipboard() && !getExpoClipboard()) {
    throw new Error(
      '[neutron-native/device/clipboard] No clipboard package found. ' +
      'Install one of: @react-native-clipboard/clipboard, expo-clipboard'
    )
  }
}
/* eslint-enable @typescript-eslint/no-explicit-any */

// ─── Public API ─────────────────────────────────────────────────────────────

/**
 * Get the current text content from the system clipboard.
 *
 * @returns The clipboard text, or an empty string if empty.
 *
 * @example
 * ```ts
 * import { getString } from '@neutron/native/device/clipboard'
 * const text = await getString()
 * console.log('Clipboard:', text)
 * ```
 */
export async function getString(): Promise<string> {
  assertAvailable()

  const community = getCommunityClipboard()
  if (community) {
    return community.getString()
  }

  const expo = getExpoClipboard()
  if (expo) {
    return expo.getStringAsync()
  }

  return ''
}

/**
 * Set text content on the system clipboard.
 *
 * @param text - The text to copy to the clipboard.
 *
 * @example
 * ```ts
 * import { setString } from '@neutron/native/device/clipboard'
 * await setString('Hello, world!')
 * ```
 */
export async function setString(text: string): Promise<void> {
  assertAvailable()

  const community = getCommunityClipboard()
  if (community) {
    community.setString(text)
    return
  }

  const expo = getExpoClipboard()
  if (expo) {
    await expo.setStringAsync(text)
    return
  }
}

/**
 * Check if the clipboard currently contains text.
 *
 * @returns true if the clipboard has text content.
 *
 * @example
 * ```ts
 * import { hasString } from '@neutron/native/device/clipboard'
 * if (await hasString()) {
 *   const text = await getString()
 * }
 * ```
 */
export async function hasString(): Promise<boolean> {
  assertAvailable()

  const community = getCommunityClipboard()
  if (community) {
    return community.hasString()
  }

  const expo = getExpoClipboard()
  if (expo) {
    return expo.hasStringAsync()
  }

  return false
}

/**
 * Get an image from the clipboard as a base64-encoded string.
 *
 * @returns Base64-encoded image data, or null if no image is available.
 *
 * @example
 * ```ts
 * import { getImage } from '@neutron/native/device/clipboard'
 * const base64 = await getImage()
 * if (base64) console.log('Got image from clipboard')
 * ```
 */
export async function getImage(): Promise<string | null> {
  const expo = getExpoClipboard()
  if (expo) {
    const result = await expo.getImageAsync?.({ format: 'png' })
    return result?.data ?? null
  }

  const community = getCommunityClipboard()
  if (community?.getImage) {
    const result = await community.getImage()
    return result ?? null
  }

  return null
}

/**
 * Set an image on the clipboard from a base64-encoded string.
 *
 * @param base64 - Base64-encoded image data.
 *
 * @example
 * ```ts
 * import { setImage } from '@neutron/native/device/clipboard'
 * await setImage(myBase64ImageData)
 * ```
 */
export async function setImage(base64: string): Promise<void> {
  const expo = getExpoClipboard()
  if (expo) {
    await expo.setImageAsync?.(base64)
    return
  }

  // @react-native-clipboard/clipboard does not support setting images
  // in all versions; fail gracefully
  throw new Error(
    '[neutron-native/device/clipboard] setImage requires expo-clipboard. ' +
    '@react-native-clipboard/clipboard does not support writing images.'
  )
}

/**
 * Get the URL content from the clipboard (iOS only).
 *
 * @returns The URL string, or null if no URL is available.
 */
export async function getUrl(): Promise<string | null> {
  const expo = getExpoClipboard()
  if (expo) {
    const hasUrl = await expo.hasUrlAsync?.()
    if (hasUrl) {
      return expo.getUrlAsync?.() ?? null
    }
    return null
  }

  // Fall back to getString and check if it looks like a URL
  const text = await getString()
  if (text && /^https?:\/\//i.test(text)) return text
  return null
}

/**
 * Add a listener for clipboard content changes.
 *
 * Note: Clipboard change detection support varies by platform and package.
 * On iOS 16+, UIPasteboard change notifications are limited by the system.
 *
 * @param callback - Called when clipboard content changes.
 * @returns A subscription handle; call `.remove()` to stop listening.
 *
 * @example
 * ```ts
 * import { addListener } from '@neutron/native/device/clipboard'
 * const sub = addListener((newContent) => {
 *   console.log('Clipboard changed:', newContent)
 * })
 * // later: sub.remove()
 * ```
 */
export function addListener(
  callback: (content: string) => void,
): { remove(): void } {
  const expo = getExpoClipboard()
  if (expo?.addClipboardListener) {
    const sub = expo.addClipboardListener(
      (event: any) => callback(event?.content ?? ''), // eslint-disable-line @typescript-eslint/no-explicit-any
    )
    return { remove: () => expo.removeClipboardListener(sub) }
  }

  const community = getCommunityClipboard()
  if (community?.addListener) {
    const sub = community.addListener(callback)
    return { remove: () => sub?.remove?.() }
  }

  // No listener support — return a no-op subscription
  return { remove() {} }
}
