/**
 * Biometrics TurboModule — web implementation using the Web Authentication API.
 *
 * Uses PublicKeyCredential and navigator.credentials.get() with
 * userVerification: 'required' to trigger platform authenticator
 * (Windows Hello, Touch ID in Safari, Android biometric prompt).
 *
 * Browser support: Chrome 67+, Firefox 60+, Safari 14+, Edge 18+
 */

import type { BiometricsModule, BiometricPromptOptions } from './biometrics.js'
import type { BiometricResult, ModuleMethod } from '../types.js'
import { registerWebModule } from '../registry.js'

const METHODS: readonly ModuleMethod[] = [
  { name: 'isAvailable', kind: 'async' },
  { name: 'authenticate', kind: 'async' },
  { name: 'isEnrolled', kind: 'async' },
] as const

const WEB_BIOMETRICS: BiometricsModule = {
  moduleName: 'NeutronBiometrics',
  methods: METHODS,

  async isAvailable(): Promise<{ available: boolean; biometryType: 'FaceID' | 'TouchID' | 'Fingerprint' | 'Iris' | 'none' }> {
    if (typeof window === 'undefined' || !window.PublicKeyCredential) {
      return { available: false, biometryType: 'none' }
    }

    try {
      const available = await PublicKeyCredential.isUserVerifyingPlatformAuthenticatorAvailable()
      // Web Authentication does not distinguish between Face/Touch/Fingerprint.
      // Return a generic type if available.
      return { available, biometryType: available ? 'Fingerprint' : 'none' }
    } catch {
      return { available: false, biometryType: 'none' }
    }
  },

  async authenticate(options?: BiometricPromptOptions): Promise<BiometricResult> {
    if (typeof window === 'undefined' || !window.PublicKeyCredential) {
      return { success: false, error: 'WebAuthn not supported in this browser' }
    }

    try {
      // Generate a random challenge for the assertion
      const challenge = new Uint8Array(32)
      crypto.getRandomValues(challenge)

      await navigator.credentials.get({
        publicKey: {
          challenge,
          timeout: 60000,
          userVerification: 'required',
          rpId: window.location.hostname,
          // Allow any credential — we're using this for biometric verification,
          // not traditional WebAuthn login
          allowCredentials: [],
        },
      })

      return { success: true }
    } catch (err: unknown) {
      const error = err instanceof Error ? err : new Error(String(err))
      const isCancel = error.name === 'NotAllowedError'
      return {
        success: false,
        error: error.message,
        biometryType: undefined,
      }
    }
  },

  async isEnrolled(): Promise<boolean> {
    if (typeof window === 'undefined' || !window.PublicKeyCredential) {
      return false
    }

    try {
      return await PublicKeyCredential.isUserVerifyingPlatformAuthenticatorAvailable()
    } catch {
      return false
    }
  },
}

// ─── Register web implementation ─────────────────────────────────────────────

registerWebModule('NeutronBiometrics', () => WEB_BIOMETRICS)
