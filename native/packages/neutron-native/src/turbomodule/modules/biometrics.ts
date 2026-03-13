/**
 * Biometrics TurboModule — fingerprint / face authentication.
 *
 * iOS: LocalAuthentication (LAContext)
 * Android: BiometricPrompt (AndroidX)
 */

import type { TurboModule, ModuleMethod, BiometricResult } from '../types.js'
import { getModule, registerModule } from '../registry.js'

export interface BiometricPromptOptions {
  /** Prompt title shown to the user */
  title?: string
  /** Subtitle (Android only) */
  subtitle?: string
  /** Description text */
  description?: string
  /** Cancel button label */
  cancelLabel?: string
  /** Allow device passcode as fallback */
  allowDeviceCredential?: boolean
  /** iOS: reason string for Face ID / Touch ID prompt */
  reason?: string
}

export interface BiometricsModule extends TurboModule {
  moduleName: 'NeutronBiometrics'

  /** Check if biometric hardware is available */
  isAvailable(): Promise<{ available: boolean; biometryType: 'FaceID' | 'TouchID' | 'Fingerprint' | 'Iris' | 'none' }>

  /** Authenticate the user with biometrics */
  authenticate(options?: BiometricPromptOptions): Promise<BiometricResult>

  /** Check if biometric data has changed since last enrollment (sensor invalidation) */
  isEnrolled(): Promise<boolean>
}

const METHODS: readonly ModuleMethod[] = [
  { name: 'isAvailable', kind: 'async' },
  { name: 'authenticate', kind: 'async' },
  { name: 'isEnrolled', kind: 'async' },
] as const

registerModule<BiometricsModule>('NeutronBiometrics', () => ({
  moduleName: 'NeutronBiometrics',
  methods: METHODS,
  async isAvailable() { return { available: false, biometryType: 'none' as const } },
  async authenticate() { return { success: false, error: 'Biometrics module not linked' } },
  async isEnrolled() { return false },
}))

/**
 * Hook to access the Biometrics TurboModule.
 *
 * @example
 * ```tsx
 * const bio = useBiometrics()
 * const { available, biometryType } = await bio.isAvailable()
 * if (available) {
 *   const result = await bio.authenticate({ reason: 'Confirm payment' })
 *   if (result.success) proceedWithPayment()
 * }
 * ```
 */
export function useBiometrics(): BiometricsModule {
  const mod = getModule<BiometricsModule>('NeutronBiometrics')
  if (!mod) throw new Error('[neutron-native] NeutronBiometrics module not available')
  return mod
}
