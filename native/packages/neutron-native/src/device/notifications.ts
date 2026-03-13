/**
 * Notifications — push and local notification management wrapping
 * expo-notifications and @react-native-firebase/messaging.
 *
 * Peer dependencies (install one):
 *   - expo-notifications (Expo managed/bare)
 *   - @react-native-firebase/messaging + @notifee/react-native (bare React Native)
 *
 * @module @neutron/native/device/notifications
 */

// ─── Types ──────────────────────────────────────────────────────────────────

/** Notification content for scheduling or display */
export interface NotificationContent {
  /** Notification title */
  title: string
  /** Notification body text */
  body?: string
  /** Custom data payload */
  data?: Record<string, string>
  /** Badge number (iOS only) */
  badge?: number
  /** Sound name or 'default' */
  sound?: string | 'default'
  /** Category identifier for actionable notifications */
  categoryId?: string
  /** Thread identifier for notification grouping */
  threadId?: string
  /** Subtitle (iOS only) */
  subtitle?: string
}

/** Trigger configuration for local notifications */
export interface NotificationTrigger {
  /** Fire after a delay in seconds */
  seconds?: number
  /** Fire at a specific date (ISO 8601 string or Date) */
  date?: string | Date
  /** Repeat interval */
  repeats?: boolean
  /** Channel ID (Android only, default: 'default') */
  channelId?: string
}

/** A received notification */
export interface ReceivedNotification {
  /** Unique notification identifier */
  id: string
  /** The notification content */
  content: NotificationContent
  /** Action identifier if user interacted with an action button */
  actionId?: string
  /** User-entered text from a text input action */
  userText?: string
}

/** Notification permission status */
export type NotificationPermissionStatus = 'granted' | 'denied' | 'undetermined'

/** Handle to unsubscribe from notification events */
export interface NotificationSubscription {
  /** Stop listening for events */
  remove(): void
}

// ─── Lazy module loaders ────────────────────────────────────────────────────

/* eslint-disable @typescript-eslint/no-explicit-any */
let _expoNotifications: any = undefined
let _firebaseMessaging: any = undefined
let _notifee: any = undefined

function getExpoNotifications(): any {
  if (_expoNotifications === undefined) {
    try { _expoNotifications = require('expo-notifications') } catch { _expoNotifications = null }
  }
  return _expoNotifications
}

function getFirebaseMessaging(): any {
  if (_firebaseMessaging === undefined) {
    try { _firebaseMessaging = require('@react-native-firebase/messaging') } catch { _firebaseMessaging = null }
  }
  return _firebaseMessaging
}

function getNotifee(): any {
  if (_notifee === undefined) {
    try { _notifee = require('@notifee/react-native') } catch { _notifee = null }
  }
  return _notifee
}

function assertAvailable(): void {
  if (!getExpoNotifications() && !getFirebaseMessaging()) {
    throw new Error(
      '[neutron-native/device/notifications] No notifications package found. ' +
      'Install one of: expo-notifications, @react-native-firebase/messaging'
    )
  }
}
/* eslint-enable @typescript-eslint/no-explicit-any */

// ─── Public API ─────────────────────────────────────────────────────────────

/**
 * Request permission to send notifications.
 *
 * @returns The resulting permission status.
 *
 * @example
 * ```ts
 * import { requestPermission } from '@neutron/native/device/notifications'
 * const status = await requestPermission()
 * if (status === 'granted') { ... }
 * ```
 */
export async function requestPermission(): Promise<NotificationPermissionStatus> {
  const expo = getExpoNotifications()
  if (expo) {
    const { status } = await expo.requestPermissionsAsync()
    if (status === 'granted') return 'granted'
    if (status === 'denied') return 'denied'
    return 'undetermined'
  }

  const firebase = getFirebaseMessaging()
  if (firebase) {
    const messaging = firebase.default ?? firebase
    const fn = typeof messaging === 'function' ? messaging() : messaging
    const authStatus = await fn.requestPermission()
    // Firebase returns 1 (AUTHORIZED) or 2 (PROVISIONAL) for granted
    if (authStatus === 1 || authStatus === 2) return 'granted'
    return 'denied'
  }

  throw new Error(
    '[neutron-native/device/notifications] No notifications package found.'
  )
}

/**
 * Check the current notification permission status.
 *
 * @returns The current permission status.
 */
export async function getPermissionStatus(): Promise<NotificationPermissionStatus> {
  const expo = getExpoNotifications()
  if (expo) {
    const { status } = await expo.getPermissionsAsync()
    if (status === 'granted') return 'granted'
    if (status === 'denied') return 'denied'
    return 'undetermined'
  }

  const firebase = getFirebaseMessaging()
  if (firebase) {
    const messaging = firebase.default ?? firebase
    const fn = typeof messaging === 'function' ? messaging() : messaging
    const hasPermission = await fn.hasPermission()
    if (hasPermission === 1 || hasPermission === 2) return 'granted'
    if (hasPermission === 0) return 'denied'
    return 'undetermined'
  }

  return 'undetermined'
}

/**
 * Get the device push token (APNS on iOS, FCM on Android).
 *
 * @returns The push token string.
 *
 * @example
 * ```ts
 * import { getToken } from '@neutron/native/device/notifications'
 * const token = await getToken()
 * // Send token to your backend for push delivery
 * ```
 */
export async function getToken(): Promise<string> {
  assertAvailable()

  const expo = getExpoNotifications()
  if (expo) {
    const token = await expo.getExpoPushTokenAsync()
    return token.data
  }

  const firebase = getFirebaseMessaging()
  if (firebase) {
    const messaging = firebase.default ?? firebase
    const fn = typeof messaging === 'function' ? messaging() : messaging
    return fn.getToken()
  }

  throw new Error('[neutron-native/device/notifications] Cannot retrieve push token')
}

/**
 * Get the device's native push token (APNS/FCM) as opposed to an Expo push token.
 *
 * @returns The native device push token.
 */
export async function getDevicePushToken(): Promise<string> {
  assertAvailable()

  const expo = getExpoNotifications()
  if (expo) {
    const token = await expo.getDevicePushTokenAsync()
    return token.data
  }

  const firebase = getFirebaseMessaging()
  if (firebase) {
    const messaging = firebase.default ?? firebase
    const fn = typeof messaging === 'function' ? messaging() : messaging
    return fn.getToken()
  }

  throw new Error('[neutron-native/device/notifications] Cannot retrieve device push token')
}

/**
 * Subscribe to foreground notification events.
 *
 * @param callback - Called when a notification is received while the app is in the foreground.
 * @returns A subscription handle; call `.remove()` to unsubscribe.
 *
 * @example
 * ```ts
 * import { onMessage } from '@neutron/native/device/notifications'
 * const sub = onMessage((notification) => {
 *   console.log('Received:', notification.content.title)
 * })
 * // later: sub.remove()
 * ```
 */
export function onMessage(
  callback: (notification: ReceivedNotification) => void,
): NotificationSubscription {
  const expo = getExpoNotifications()
  if (expo) {
    const sub = expo.addNotificationReceivedListener(
      (event: any) => { // eslint-disable-line @typescript-eslint/no-explicit-any
        callback({
          id: event.request?.identifier ?? '',
          content: {
            title: event.request?.content?.title ?? '',
            body: event.request?.content?.body,
            data: event.request?.content?.data,
            badge: event.request?.content?.badge,
            sound: event.request?.content?.sound,
          },
        })
      },
    )
    return { remove: () => sub.remove() }
  }

  const firebase = getFirebaseMessaging()
  if (firebase) {
    const messaging = firebase.default ?? firebase
    const fn = typeof messaging === 'function' ? messaging() : messaging
    const unsub = fn.onMessage(
      (msg: any) => { // eslint-disable-line @typescript-eslint/no-explicit-any
        callback({
          id: msg.messageId ?? '',
          content: {
            title: msg.notification?.title ?? '',
            body: msg.notification?.body,
            data: msg.data,
          },
        })
      },
    )
    return { remove: typeof unsub === 'function' ? unsub : () => {} }
  }

  return { remove() {} }
}

/**
 * Subscribe to notification interaction events (user tapped a notification).
 *
 * @param callback - Called when the user interacts with a notification.
 * @returns A subscription handle.
 */
export function onNotificationResponse(
  callback: (notification: ReceivedNotification) => void,
): NotificationSubscription {
  const expo = getExpoNotifications()
  if (expo) {
    const sub = expo.addNotificationResponseReceivedListener(
      (response: any) => { // eslint-disable-line @typescript-eslint/no-explicit-any
        callback({
          id: response.notification?.request?.identifier ?? '',
          content: {
            title: response.notification?.request?.content?.title ?? '',
            body: response.notification?.request?.content?.body,
            data: response.notification?.request?.content?.data,
          },
          actionId: response.actionIdentifier,
          userText: response.userText,
        })
      },
    )
    return { remove: () => sub.remove() }
  }

  const firebase = getFirebaseMessaging()
  if (firebase) {
    const messaging = firebase.default ?? firebase
    const fn = typeof messaging === 'function' ? messaging() : messaging
    const unsub = fn.onNotificationOpenedApp(
      (msg: any) => { // eslint-disable-line @typescript-eslint/no-explicit-any
        callback({
          id: msg.messageId ?? '',
          content: {
            title: msg.notification?.title ?? '',
            body: msg.notification?.body,
            data: msg.data,
          },
        })
      },
    )
    return { remove: typeof unsub === 'function' ? unsub : () => {} }
  }

  return { remove() {} }
}

/**
 * Schedule a local notification.
 *
 * @param content - Notification content to display.
 * @param trigger - When to fire the notification.
 * @returns The scheduled notification identifier.
 *
 * @example
 * ```ts
 * import { scheduleLocal } from '@neutron/native/device/notifications'
 * const id = await scheduleLocal(
 *   { title: 'Reminder', body: 'Time to check in!' },
 *   { seconds: 60 }
 * )
 * ```
 */
export async function scheduleLocal(
  content: NotificationContent,
  trigger: NotificationTrigger = {},
): Promise<string> {
  assertAvailable()

  const expo = getExpoNotifications()
  if (expo) {
    let expoTrigger: any = null // eslint-disable-line @typescript-eslint/no-explicit-any

    if (trigger.seconds) {
      expoTrigger = { seconds: trigger.seconds, repeats: trigger.repeats ?? false }
    } else if (trigger.date) {
      const date = trigger.date instanceof Date ? trigger.date : new Date(trigger.date)
      expoTrigger = { date, repeats: trigger.repeats ?? false }
    }

    return expo.scheduleNotificationAsync({
      content: {
        title: content.title,
        body: content.body,
        data: content.data,
        badge: content.badge,
        sound: content.sound ?? 'default',
        categoryIdentifier: content.categoryId,
        subtitle: content.subtitle,
      },
      trigger: expoTrigger,
    })
  }

  // Firebase path: use @notifee for local notifications
  const notifee = getNotifee()
  if (notifee) {
    const ntf = notifee.default ?? notifee

    // Ensure default channel on Android
    const channelId = trigger.channelId ?? 'default'
    await ntf.createChannel?.({ id: channelId, name: 'Default' })

    let ntfTrigger: any = undefined // eslint-disable-line @typescript-eslint/no-explicit-any
    if (trigger.seconds) {
      ntfTrigger = {
        type: 1, // TriggerType.TIMESTAMP
        timestamp: Date.now() + trigger.seconds * 1000,
        repeatFrequency: trigger.repeats ? -1 : undefined,
      }
    } else if (trigger.date) {
      const ts = trigger.date instanceof Date ? trigger.date.getTime() : new Date(trigger.date).getTime()
      ntfTrigger = {
        type: 1,
        timestamp: ts,
        repeatFrequency: trigger.repeats ? -1 : undefined,
      }
    }

    if (ntfTrigger) {
      return ntf.createTriggerNotification(
        {
          title: content.title,
          body: content.body,
          data: content.data,
          android: { channelId },
          ios: { sound: content.sound ?? 'default' },
        },
        ntfTrigger,
      )
    }

    // Immediate display
    return ntf.displayNotification({
      title: content.title,
      body: content.body,
      data: content.data,
      android: { channelId },
      ios: { sound: content.sound ?? 'default' },
    })
  }

  throw new Error(
    '[neutron-native/device/notifications] Cannot schedule local notification. ' +
    'For bare RN, install @notifee/react-native alongside @react-native-firebase/messaging.'
  )
}

/**
 * Cancel a scheduled notification by its identifier.
 *
 * @param id - The notification identifier returned by scheduleLocal.
 */
export async function cancelNotification(id: string): Promise<void> {
  const expo = getExpoNotifications()
  if (expo) {
    await expo.cancelScheduledNotificationAsync(id)
    return
  }

  const notifee = getNotifee()
  if (notifee) {
    const ntf = notifee.default ?? notifee
    await ntf.cancelNotification(id)
    return
  }
}

/**
 * Cancel all scheduled notifications.
 */
export async function cancelAllNotifications(): Promise<void> {
  const expo = getExpoNotifications()
  if (expo) {
    await expo.cancelAllScheduledNotificationsAsync()
    return
  }

  const notifee = getNotifee()
  if (notifee) {
    const ntf = notifee.default ?? notifee
    await ntf.cancelAllNotifications()
    return
  }
}

/**
 * Get the current badge count (iOS only).
 *
 * @returns The current badge number.
 */
export async function getBadgeCount(): Promise<number> {
  const expo = getExpoNotifications()
  if (expo) {
    return expo.getBadgeCountAsync()
  }

  const notifee = getNotifee()
  if (notifee) {
    const ntf = notifee.default ?? notifee
    return ntf.getBadgeCount?.() ?? 0
  }

  return 0
}

/**
 * Set the badge count (iOS only).
 *
 * @param count - Badge number to display (0 to clear).
 */
export async function setBadgeCount(count: number): Promise<void> {
  const expo = getExpoNotifications()
  if (expo) {
    await expo.setBadgeCountAsync(count)
    return
  }

  const notifee = getNotifee()
  if (notifee) {
    const ntf = notifee.default ?? notifee
    await ntf.setBadgeCount?.(count)
  }
}
