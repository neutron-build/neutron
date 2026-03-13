/**
 * Notifications TurboModule — local + push notifications.
 *
 * iOS: UNUserNotificationCenter
 * Android: FCM (Firebase Cloud Messaging) + NotificationManager
 */

import type {
  TurboModule, ModuleMethod, NativeResult,
  NativeSubscription, NativeEventCallback, NotificationPayload,
} from '../types.js'
import { getModule, registerModule } from '../registry.js'

export interface NotificationResponse {
  id: string
  action: string
  payload: NotificationPayload
  userText?: string
}

export interface NotificationsModule extends TurboModule {
  moduleName: 'NeutronNotifications'

  /** Request notification permission */
  requestPermission(): Promise<'granted' | 'denied'>

  /** Check current permission status */
  checkPermission(): Promise<'granted' | 'denied' | 'not-determined'>

  /** Get the push token (APNS token on iOS, FCM token on Android) */
  getToken(): Promise<NativeResult<string>>

  /** Schedule a local notification */
  scheduleLocal(payload: NotificationPayload & {
    /** Trigger after delay (seconds) */
    fireAfter?: number
    /** Trigger at specific date (ISO 8601) */
    fireAt?: string
    /** Repeat interval */
    repeat?: 'minute' | 'hour' | 'day' | 'week'
  }): Promise<NativeResult<string>>

  /** Cancel a scheduled local notification by ID */
  cancel(id: string): Promise<void>

  /** Cancel all scheduled local notifications */
  cancelAll(): Promise<void>

  /** Get the badge count (iOS only) */
  getBadgeCount(): Promise<number>

  /** Set the badge count (iOS only) */
  setBadgeCount(count: number): Promise<void>

  /** Subscribe to notification received events (foreground) */
  onReceived(callback: NativeEventCallback<NotificationPayload>): NativeSubscription

  /** Subscribe to notification response events (tapped) */
  onResponse(callback: NativeEventCallback<NotificationResponse>): NativeSubscription
}

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

registerModule<NotificationsModule>('NeutronNotifications', () => ({
  moduleName: 'NeutronNotifications',
  methods: METHODS,
  async requestPermission() { return 'denied' as const },
  async checkPermission() { return 'denied' as const },
  async getToken() { return { ok: false, error: { code: 'UNAVAILABLE', message: 'Notifications module not linked' } } },
  async scheduleLocal() { return { ok: false, error: { code: 'UNAVAILABLE', message: 'Notifications module not linked' } } },
  async cancel() {},
  async cancelAll() {},
  async getBadgeCount() { return 0 },
  async setBadgeCount() {},
  onReceived() { return { remove() {} } },
  onResponse() { return { remove() {} } },
}))

/**
 * Hook to access the Notifications TurboModule.
 *
 * @example
 * ```tsx
 * const notif = useNotifications()
 * await notif.requestPermission()
 * await notif.scheduleLocal({ title: 'Reminder', body: 'Check in!', fireAfter: 60 })
 * ```
 */
export function useNotifications(): NotificationsModule {
  const mod = getModule<NotificationsModule>('NeutronNotifications')
  if (!mod) throw new Error('[neutron-native] NeutronNotifications module not available')
  return mod
}
