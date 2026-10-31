import { getModule, clearCache } from '../../registry.js'
import '../../modules/biometrics.web.js'
let mod: any
beforeEach(() => {
  Object.defineProperty(globalThis, 'navigator', { configurable: true, value: {} })
  ;(globalThis as any).window = { addEventListener: jest.fn(), removeEventListener: jest.fn(), location: { hostname: 'example.test' } }
  ;(globalThis as any).document = {}
  clearCache()
  mod = getModule('NeutronBiometrics')
  expect(mod).not.toBeNull()
})
afterEach(() => { jest.useRealTimers() })
it('unsupported browser cannot authenticate', async () => { expect(await mod.authenticate()).toMatchObject({ success: false }); expect(await mod.isAvailable()).toEqual({ available: false, biometryType: 'none' }) })
it('actual canceled WebAuthn request reports failure', async () => { (window as any).PublicKeyCredential = {}; (navigator as any).credentials = { get: jest.fn(async () => { throw new Error('cancel') }) }; expect(await mod.authenticate()).toMatchObject({ success: false, error: 'cancel' }); expect(navigator.credentials.get).toHaveBeenCalledWith(expect.objectContaining({ publicKey: expect.objectContaining({ userVerification: 'required' }) })) })
