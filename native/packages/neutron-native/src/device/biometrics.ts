/**
 * Biometrics — fingerprint and Face ID authentication wrapping
 * expo-local-authentication and react-native-biometrics.
 *
 * Peer dependencies (install one):
 *   - expo-local-authentication (Expo managed/bare)
 *   - react-native-biometrics (bare React Native)
 *
 * @module @neutron/native/device/biometrics
 */

// ─── Types ──────────────────────────────────────────────────────────────────

/** Supported biometric authentication types */
export type BiometryType =
  | 'FaceID'
  | 'TouchID'
  | 'Fingerprint'
  | 'Iris'
  | 'none'

/** Options for the authentication prompt */
export interface AuthenticateOptions {
  /** Reason string displayed to the user (iOS Face ID / Touch ID prompt) */
  promptMessage?: string
  /** Title for the biometric prompt (Android BiometricPrompt) */
  title?: string
  /** Subtitle text (Android only) */
  subtitle?: string
  /** Description text (Android only) */
  description?: string
  /** Cancel button label */
  cancelLabel?: string
  /** Allow device passcode/PIN as a fallback (default: false) */
  fallbackToDeviceCredential?: boolean
  /** Disable the automatic error dialog on Android (default: false) */
  disableDeviceFallback?: boolean
}

/** Result of a biometric authentication attempt */
export interface AuthenticateResult {
  /** Whether authentication succeeded */
  success: boolean
  /** Error message if authentication failed */
  error?: string
  /** Error code for programmatic handling */
  errorCode?: 'USER_CANCEL' | 'AUTHENTICATION_FAILED' | 'NOT_ENROLLED' | 'NOT_AVAILABLE' | 'LOCKOUT' | 'UNKNOWN'
}

/** Biometric availability information */
export interface BiometricAvailability {
  /** Whether biometric hardware is present and usable */
  available: boolean
  /** The type of biometrics supported */
  biometryType: BiometryType
  /** Whether biometric data is enrolled (e.g., fingerprints registered) */
  isEnrolled: boolean
}

// ─── Lazy module loaders ────────────────────────────────────────────────────

/* eslint-disable @typescript-eslint/no-explicit-any */
let _expoLocalAuth: any = undefined
let _rnBiometrics: any = undefined

function getExpoLocalAuth(): any {
  if (_expoLocalAuth === undefined) {
    try { _expoLocalAuth = require('expo-local-authentication') } catch { _expoLocalAuth = null }
  }
  return _expoLocalAuth
}

function getRNBiometrics(): any {
  if (_rnBiometrics === undefined) {
    try { _rnBiometrics = require('react-native-biometrics') } catch { _rnBiometrics = null }
  }
  return _rnBiometrics
}

function assertAvailable(): void {
  if (!getExpoLocalAuth() && !getRNBiometrics()) {
    throw new Error(
      '[neutron-native/device/biometrics] No biometrics package found. ' +
      'Install one of: expo-local-authentication, react-native-biometrics'
    )
  }
}
/* eslint-enable @typescript-eslint/no-explicit-any */

// ─── Public API ─────────────────────────────────────────────────────────────

/**
 * Check if biometric authentication is available on this device.
 *
 * @returns Availability information including the biometric type.
 *
 * @example
 * ```ts
 * import { isAvailable } from '@neutron/native/device/biometrics'
 * const { available, biometryType } = await isAvailable()
 * if (available) console.log(`${biometryType} is available`)
 * ```
 */
export async function isAvailable(): Promise<BiometricAvailability> {
  const expo = getExpoLocalAuth()
  if (expo) {
    const hasHardware = await expo.hasHardwareAsync()
    const isEnrolled = await expo.isEnrolledAsync()
    const supportedTypes = await expo.supportedAuthenticationTypesAsync()

    let biometryType: BiometryType = 'none'
    // expo-local-authentication: 1 = Fingerprint, 2 = FacialRecognition, 3 = Iris
    if (supportedTypes.includes(2)) biometryType = 'FaceID'
    else if (supportedTypes.includes(1)) biometryType = 'TouchID'
    else if (supportedTypes.includes(3)) biometryType = 'Iris'

    return { available: hasHardware && isEnrolled, biometryType, isEnrolled }
  }

  const rnBio = getRNBiometrics()
  if (rnBio) {
    const ReactNativeBiometrics = rnBio.default ?? rnBio
    const biometrics = typeof ReactNativeBiometrics === 'function'
      ? new ReactNativeBiometrics()
      : ReactNativeBiometrics

    const { available, biometryType: rawType } = await biometrics.isSensorAvailable()

    let biometryType: BiometryType = 'none'
    if (rawType === 'FaceID') biometryType = 'FaceID'
    else if (rawType === 'TouchID') biometryType = 'TouchID'
    else if (rawType === 'Biometrics') biometryType = 'Fingerprint'

    return { available, biometryType, isEnrolled: available }
  }

  return { available: false, biometryType: 'none', isEnrolled: false }
}

/**
 * Get the list of supported biometric types on this device.
 *
 * @returns Array of supported biometry types.
 *
 * @example
 * ```ts
 * import { getSupportedTypes } from '@neutron/native/device/biometrics'
 * const types = await getSupportedTypes()
 * // e.g. ['FaceID'] or ['Fingerprint']
 * ```
 */
export async function getSupportedTypes(): Promise<BiometryType[]> {
  const expo = getExpoLocalAuth()
  if (expo) {
    const supportedTypes = await expo.supportedAuthenticationTypesAsync()
    const typeMap: Record<number, BiometryType> = {
      1: 'Fingerprint',
      2: 'FaceID',
      3: 'Iris',
    }
    return supportedTypes
      .map((t: number) => typeMap[t])
      .filter((t: BiometryType | undefined): t is BiometryType => t !== undefined)
  }

  const rnBio = getRNBiometrics()
  if (rnBio) {
    const { biometryType } = await isAvailable()
    return biometryType !== 'none' ? [biometryType] : []
  }

  return []
}

/**
 * Authenticate the user with biometrics (fingerprint, Face ID, etc.).
 *
 * @param options - Prompt configuration.
 * @returns The authentication result.
 *
 * @example
 * ```ts
 * import { authenticate } from '@neutron/native/device/biometrics'
 * const result = await authenticate({
 *   promptMessage: 'Verify your identity',
 *   fallbackToDeviceCredential: true,
 * })
 * if (result.success) console.log('Authenticated!')
 * ```
 */
export async function authenticate(
  options: AuthenticateOptions = {},
): Promise<AuthenticateResult> {
  assertAvailable()

  const expo = getExpoLocalAuth()
  if (expo) {
    try {
      const result = await expo.authenticateAsync({
        promptMessage: options.promptMessage ?? 'Authenticate',
        cancelLabel: options.cancelLabel ?? 'Cancel',
        disableDeviceFallback: options.disableDeviceFallback ?? !options.fallbackToDeviceCredential,
        fallbackLabel: options.fallbackToDeviceCredential ? 'Use Passcode' : undefined,
      })

      if (result.success) {
        return { success: true }
      }

      let errorCode: AuthenticateResult['errorCode'] = 'UNKNOWN'
      if (result.error === 'user_cancel') errorCode = 'USER_CANCEL'
      else if (result.error === 'not_enrolled') errorCode = 'NOT_ENROLLED'
      else if (result.error === 'not_available') errorCode = 'NOT_AVAILABLE'
      else if (result.error === 'lockout') errorCode = 'LOCKOUT'
      else if (result.error === 'authentication') errorCode = 'AUTHENTICATION_FAILED'

      return {
        success: false,
        error: result.error ?? 'Authentication failed',
        errorCode,
      }
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : 'Unknown error'
      return { success: false, error: message, errorCode: 'UNKNOWN' }
    }
  }

  const rnBio = getRNBiometrics()
  if (rnBio) {
    const ReactNativeBiometrics = rnBio.default ?? rnBio
    const biometrics = typeof ReactNativeBiometrics === 'function'
      ? new ReactNativeBiometrics({ allowDeviceCredentials: options.fallbackToDeviceCredential ?? false })
      : ReactNativeBiometrics

    try {
      const result = await biometrics.simplePrompt({
        promptMessage: options.promptMessage ?? options.title ?? 'Authenticate',
        cancelButtonText: options.cancelLabel ?? 'Cancel',
        fallbackPromptMessage: options.fallbackToDeviceCredential ? 'Use device passcode' : undefined,
      })

      if (result.success) {
        return { success: true }
      }

      return {
        success: false,
        error: result.error ?? 'Authentication failed',
        errorCode: result.error === 'User cancellation' ? 'USER_CANCEL' : 'AUTHENTICATION_FAILED',
      }
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : 'Unknown error'
      return { success: false, error: message, errorCode: 'UNKNOWN' }
    }
  }

  return { success: false, error: 'No biometrics provider available', errorCode: 'NOT_AVAILABLE' }
}

/**
 * Check the biometric enrollment security level.
 * Returns true if the device has biometric credentials enrolled (at least one
 * fingerprint, face, or iris registered).
 *
 * @returns true if biometric data is enrolled on the device.
 */
export async function isEnrolled(): Promise<boolean> {
  const expo = getExpoLocalAuth()
  if (expo) {
    return expo.isEnrolledAsync()
  }

  const { isEnrolled: enrolled } = await isAvailable()
  return enrolled
}

/**
 * Get the security level of the device.
 * Useful for deciding whether to allow biometric auth or require a PIN.
 *
 * @returns The device security level.
 */
export async function getSecurityLevel(): Promise<'none' | 'pin' | 'biometric'> {
  const expo = getExpoLocalAuth()
  if (expo) {
    const level = await expo.getEnrolledLevelAsync?.()
    // expo: 0 = NONE, 1 = SECRET (PIN/pattern), 2 = BIOMETRIC
    if (level === 2) return 'biometric'
    if (level === 1) return 'pin'
    return 'none'
  }

  const { available } = await isAvailable()
  return available ? 'biometric' : 'none'
}
