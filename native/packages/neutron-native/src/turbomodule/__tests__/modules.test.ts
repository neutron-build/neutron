/**
 * Tests for TurboModule device modules — camera, location, haptics, clipboard,
 * notifications, biometrics, async-storage, net-info, device-info, permissions.
 *
 * Each module registers a factory and provides a useXxx hook.
 */

describe('TurboModule Device Modules', () => {
  beforeEach(() => {
    jest.resetModules()
    // Clear any cached modules
    const registry = require('../registry')
    registry.clearCache()
  })

  describe('camera module', () => {
    it('registers NeutronCamera module', () => {
      require('../modules/camera')
      const registry = require('../registry')
      expect(registry.hasModule('NeutronCamera')).toBe(true)
    })

    it('useCamera returns module with capture method', () => {
      const { useCamera } = require('../modules/camera')
      const camera = useCamera()
      expect(camera).toBeDefined()
      expect(typeof camera.capture).toBe('function')
      expect(typeof camera.pickFromGallery).toBe('function')
      expect(typeof camera.isAvailable).toBe('function')
      expect(typeof camera.checkPermission).toBe('function')
    })

    it('stub capture returns error result', async () => {
      const { useCamera } = require('../modules/camera')
      const camera = useCamera()
      const result = await camera.capture()
      expect(result.ok).toBe(false)
    })

    it('stub isAvailable returns false', () => {
      const { useCamera } = require('../modules/camera')
      const camera = useCamera()
      expect(camera.isAvailable()).toBe(false)
    })
  })

  describe('location module', () => {
    it('registers NeutronLocation module', () => {
      require('../modules/location')
      const registry = require('../registry')
      expect(registry.hasModule('NeutronLocation')).toBe(true)
    })

    it('useLocation returns module', () => {
      const { useLocation } = require('../modules/location')
      const location = useLocation()
      expect(location).toBeDefined()
      expect(location.moduleName).toBe('NeutronLocation')
    })
  })

  describe('haptics module', () => {
    it('registers NeutronHaptics module', () => {
      require('../modules/haptics')
      const registry = require('../registry')
      expect(registry.hasModule('NeutronHaptics')).toBe(true)
    })

    it('useHaptics returns module', () => {
      const { useHaptics } = require('../modules/haptics')
      const haptics = useHaptics()
      expect(haptics).toBeDefined()
      expect(haptics.moduleName).toBe('NeutronHaptics')
    })
  })

  describe('clipboard module', () => {
    it('registers NeutronClipboard module', () => {
      require('../modules/clipboard')
      const registry = require('../registry')
      expect(registry.hasModule('NeutronClipboard')).toBe(true)
    })

    it('useClipboard returns module', () => {
      const { useClipboard } = require('../modules/clipboard')
      const clipboard = useClipboard()
      expect(clipboard).toBeDefined()
      expect(clipboard.moduleName).toBe('NeutronClipboard')
    })
  })

  describe('notifications module', () => {
    it('registers NeutronNotifications module', () => {
      require('../modules/notifications')
      const registry = require('../registry')
      expect(registry.hasModule('NeutronNotifications')).toBe(true)
    })

    it('useNotifications returns module', () => {
      const { useNotifications } = require('../modules/notifications')
      const notifications = useNotifications()
      expect(notifications).toBeDefined()
      expect(notifications.moduleName).toBe('NeutronNotifications')
    })
  })

  describe('biometrics module', () => {
    it('registers NeutronBiometrics module', () => {
      require('../modules/biometrics')
      const registry = require('../registry')
      expect(registry.hasModule('NeutronBiometrics')).toBe(true)
    })

    it('useBiometrics returns module', () => {
      const { useBiometrics } = require('../modules/biometrics')
      const biometrics = useBiometrics()
      expect(biometrics).toBeDefined()
      expect(biometrics.moduleName).toBe('NeutronBiometrics')
    })
  })

  describe('async-storage module', () => {
    it('registers NeutronAsyncStorage module', () => {
      require('../modules/async-storage')
      const registry = require('../registry')
      expect(registry.hasModule('NeutronAsyncStorage')).toBe(true)
    })

    it('useAsyncStorage returns module', () => {
      const { useAsyncStorage } = require('../modules/async-storage')
      const storage = useAsyncStorage()
      expect(storage).toBeDefined()
      expect(storage.moduleName).toBe('NeutronAsyncStorage')
    })

    it('in-memory fallback getItem/setItem works', async () => {
      const { useAsyncStorage } = require('../modules/async-storage')
      const storage = useAsyncStorage()
      await storage.setItem('key1', 'value1')
      const result = await storage.getItem('key1')
      expect(result).toBe('value1')
    })

    it('in-memory fallback returns null for missing keys', async () => {
      const { useAsyncStorage } = require('../modules/async-storage')
      const storage = useAsyncStorage()
      const result = await storage.getItem('nonexistent')
      expect(result).toBeNull()
    })
  })

  describe('net-info module', () => {
    it('registers NeutronNetInfo module', () => {
      require('../modules/net-info')
      const registry = require('../registry')
      expect(registry.hasModule('NeutronNetInfo')).toBe(true)
    })

    it('useNetInfo returns module', () => {
      const { useNetInfo } = require('../modules/net-info')
      const netInfo = useNetInfo()
      expect(netInfo).toBeDefined()
      expect(netInfo.moduleName).toBe('NeutronNetInfo')
    })
  })

  describe('device-info module', () => {
    it('registers NeutronDeviceInfo module', () => {
      require('../modules/device-info')
      const registry = require('../registry')
      expect(registry.hasModule('NeutronDeviceInfo')).toBe(true)
    })

    it('useDeviceInfo returns module with stub data', () => {
      const { useDeviceInfo } = require('../modules/device-info')
      const info = useDeviceInfo()
      expect(info).toBeDefined()
      expect(info.moduleName).toBe('NeutronDeviceInfo')
    })
  })

  describe('permissions module', () => {
    it('registers NeutronPermissions module', () => {
      require('../modules/permissions')
      const registry = require('../registry')
      expect(registry.hasModule('NeutronPermissions')).toBe(true)
    })

    it('usePermissions returns module', () => {
      const { usePermissions } = require('../modules/permissions')
      const permissions = usePermissions()
      expect(permissions).toBeDefined()
      expect(permissions.moduleName).toBe('NeutronPermissions')
    })
  })
})
