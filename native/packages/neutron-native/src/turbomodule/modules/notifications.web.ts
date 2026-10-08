/**
 * Notifications TurboModule — web implementation using the Notification API.
 *
 * Uses the W3C Notification API for local notifications and the
 * Badging API (navigator.setAppBadge) for badge counts.
 *
 * Push tokens (getToken) are not available on web — returns UNAVAILABLE.
 *
 * Browser support: Chrome 22+, Firefox 22+, Safari 15.4+, Edge 14+
 */

import type { NotificationsModule, NotificationResponse } from './notifications.js'
import type { NativeResult, NativeSubscription, NativeEventCallback, NotificationPayload, ModuleMethod } from '../types.js'
import { registerWebModule } from '../registry.js'

const METHODS: readonly ModuleMethod[] = [
  { name: 'requestPermission', kind: 'async' },
  { name: 'checkPermission', kind: 'async' },
  { name: 'getToken', kind: 'async' },
  { name: 'scheduleLocal', kind: 'async' },
  { name: 'cancel', kind: 'async' },
  { name: 'cancelAll', kind: 'async' },
  { name: 'getBadgeCount', kind: 'async' },
  { name: 'setBadgeCount', kind: 'async' },
  { name: 'onReceived', kind: 'sync' },
  { name: 'onResponse', kind: 'sync' },
] as const

/** Track scheduled notification timeouts so they can be cancelled */
const _scheduledTimers = new Map<string, ReturnType<typeof setTimeout>>()

/** Track active Notification instances for the onResponse listener */
const _activeNotifications = new Map<string, Notification>()

/** Event listeners for notification clicks */
const _responseCallbacks = new Set<(response: NotificationResponse) => void>()

const WEB_NOTIFICATIONS: NotificationsModule = {
  moduleName: 'NeutronNotifications',
  methods: METHODS,

  async requestPermission(): Promise<'granted' | 'denied'> {
    if (typeof window === 'undefined' || !('Notification' in window)) {
      return 'denied'
    }
    const result = await Notification.requestPermission()
    return result === 'granted' ? 'granted' : 'denied'
  },

  async checkPermission(): Promise<'granted' | 'denied' | 'not-determined'> {
    if (typeof window === 'undefined' || !('Notification' in window)) {
      return 'denied'
    }
    if (Notification.permission === 'granted') return 'granted'
    if (Notification.permission === 'denied') return 'denied'
    return 'not-determined'
  },

  async getToken(): Promise<NativeResult<string>> {
    // Push tokens are a native-only concept (APNS / FCM).
    // Web push uses a different mechanism (PushSubscription via Service Workers).
    return { ok: false, error: { code: 'UNAVAILABLE', message: 'Push tokens are not available on web. Use Service Worker push subscription instead.' } }
  },

  async scheduleLocal(payload: NotificationPayload & {
    fireAfter?: number
    fireAt?: string
    repeat?: 'minute' | 'hour' | 'day' | 'week'
  }): Promise<NativeResult<string>> {
    if (typeof window === 'undefined' || !('Notification' in window)) {
      return { ok: false, error: { code: 'UNAVAILABLE', message: 'Notification API not supported' } }
    }

    if (Notification.permission !== 'granted') {
      return { ok: false, error: { code: 'PERMISSION_DENIED', message: 'Notification permission not granted' } }
    }

    // Repeating delivery is NOT implemented on web: a single setTimeout
    // would silently deliver once while claiming a schedule (NF-NR-11).
    if (payload.repeat !== undefined) {
      return { ok: false, error: { code: 'UNSUPPORTED', message: 'Repeating notifications are not supported by the web module; schedule each delivery explicitly.' } }
    }

    // Validate the requested delay/date: a garbage date is an argument
    // error, not an immediate notification (NaN comparisons are false,
    // which used to fire instantly).
    let delayMs = 0
    if (payload.fireAfter !== undefined) {
      if (!Number.isFinite(payload.fireAfter) || payload.fireAfter < 0) {
        return { ok: false, error: { code: 'INVALID_ARGUMENT', message: `fireAfter must be a non-negative number of seconds (got ${String(payload.fireAfter)})` } }
      }
      delayMs = payload.fireAfter * 1000
    } else if (payload.fireAt !== undefined) {
      const at = new Date(payload.fireAt).getTime()
      if (!Number.isFinite(at)) {
        return { ok: false, error: { code: 'INVALID_ARGUMENT', message: `fireAt is not a valid date (got ${String(payload.fireAt)})` } }
      }
      delayMs = Math.max(0, at - Date.now())
    }

    const id = typeof crypto !== 'undefined' && crypto.randomUUID
      ? crypto.randomUUID()
      : `notif-${Date.now()}-${Math.random().toString(36).slice(2)}`

    const showNotification = (): NativeResult<null> => {
      try {
        const notification = new Notification(payload.title, {
          body: payload.body,
          tag: id,
          data: payload.data,
          silent: !payload.sound,
        })

        _activeNotifications.set(id, notification)

        notification.onclick = () => {
          const response: NotificationResponse = {
            id,
            action: 'default',
            payload,
          }
          _responseCallbacks.forEach((cb) => cb(response))
        }

        notification.onclose = () => {
          _activeNotifications.delete(id)
        }
        return { ok: true, value: null }
      } catch (err) {
        // Constructor errors surface instead of vanishing in a timer
        // (NF-NR-11); immediate deliveries return them to the caller,
        // delayed ones are reported on the console with their id.
        const message = err instanceof Error ? err.message : String(err)
        if (delayMs === 0) {
          return { ok: false, error: { code: 'UNAVAILABLE', message } }
        }
        console.error(`[neutron-native/notifications] scheduled notification ${id} failed to construct: ${message}`)
        return { ok: false, error: { code: 'UNAVAILABLE', message } }
      }
    }

    if (delayMs > 0) {
      const timer = setTimeout(() => {
        // Fired timers clean their own records so cancel() reflects only
        // what is genuinely still pending (NF-NR-11).
        _scheduledTimers.delete(id)
        showNotification()
      }, delayMs)
      _scheduledTimers.set(id, timer)
      return { ok: true, value: id }
    }

    const immediate = showNotification()
    if (!immediate.ok) {
      return { ok: false, error: immediate.error }
    }
    return { ok: true, value: id }
  },

  async cancel(id: string): Promise<void> {
    const timer = _scheduledTimers.get(id)
    if (timer) {
      clearTimeout(timer)
      _scheduledTimers.delete(id)
    }
    const notification = _activeNotifications.get(id)
    if (notification) {
      notification.close()
      _activeNotifications.delete(id)
    }
  },

  async cancelAll(): Promise<void> {
    for (const [id, timer] of _scheduledTimers) {
      clearTimeout(timer)
      _scheduledTimers.delete(id)
    }
    for (const [id, notification] of _activeNotifications) {
      notification.close()
      _activeNotifications.delete(id)
    }
  },

  async getBadgeCount(): Promise<number> {
    // The Badging API doesn't provide a getter; return 0
    return 0
  },

  async setBadgeCount(count: number): Promise<void> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const nav = navigator as any
    if (typeof nav.setAppBadge === 'function') {
      try {
        if (count > 0) {
          await nav.setAppBadge(count)
        } else {
          await nav.clearAppBadge()
        }
      } catch {
        // Badging API not supported or permission denied
      }
    }
  },

  onReceived(_callback: NativeEventCallback<NotificationPayload>): NativeSubscription {
    // On web, "received in foreground" happens when we show the notification.
    // There is no separate event — the Notification API fires onclick, not onshow in a useful way.
    // Best-effort: listen for ServiceWorker messages if available.
    // For now, return a no-op subscription since web notifications are inherently foreground.
    return { remove() {} }
  },

  onResponse(callback: NativeEventCallback<NotificationResponse>): NativeSubscription {
    _responseCallbacks.add(callback)
    return {
      remove() {
        _responseCallbacks.delete(callback)
      },
    }
  },
}

// ─── Register web implementation ─────────────────────────────────────────────

registerWebModule('NeutronNotifications', () => WEB_NOTIFICATIONS)
