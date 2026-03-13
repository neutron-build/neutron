/**
 * Clipboard TurboModule — system clipboard access.
 *
 * iOS: UIPasteboard.general
 * Android: ClipboardManager
 */

import type { TurboModule, ModuleMethod, NativeSubscription, NativeEventCallback } from '../types.js'
import { getModule, registerModule } from '../registry.js'

export interface ClipboardModule extends TurboModule {
  moduleName: 'NeutronClipboard'

  /** Get the current clipboard text content */
  getString(): Promise<string>

  /** Set the clipboard text content */
  setString(text: string): void

  /** Check if the clipboard has text content */
  hasString(): Promise<boolean>

  /** Get clipboard image as base64 (if available) */
  getImage(): Promise<string | null>

  /** Set clipboard image from base64 */
  setImage(base64: string): void

  /** Listen for clipboard content changes */
  onChange(callback: NativeEventCallback<{ content: string }>): NativeSubscription
}

const METHODS: readonly ModuleMethod[] = [
  { name: 'getString', kind: 'async' },
  { name: 'setString', kind: 'sync' },
  { name: 'hasString', kind: 'async' },
  { name: 'getImage', kind: 'async' },
  { name: 'setImage', kind: 'sync' },
  { name: 'onChange', kind: 'sync' },
] as const

registerModule<ClipboardModule>('NeutronClipboard', () => ({
  moduleName: 'NeutronClipboard',
  methods: METHODS,
  async getString() { return '' },
  setString() {},
  async hasString() { return false },
  async getImage() { return null },
  setImage() {},
  onChange() { return { remove() {} } },
}))

/**
 * Hook to access the Clipboard TurboModule.
 *
 * @example
 * ```tsx
 * const clipboard = useClipboard()
 * await clipboard.setString('Hello!')
 * const text = await clipboard.getString()
 * ```
 */
export function useClipboard(): ClipboardModule {
  const mod = getModule<ClipboardModule>('NeutronClipboard')
  if (!mod) throw new Error('[neutron-native] NeutronClipboard module not available')
  return mod
}
