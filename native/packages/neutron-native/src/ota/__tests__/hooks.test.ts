/**
 * Tests for OTA hooks — initOTA, useOTA.
 */

describe('OTA hooks', () => {
  beforeEach(() => {
    jest.resetModules()
    // Mock fetch for OTA check
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      status: 304,
    })
  })

  afterEach(() => {
    delete (global as any).fetch
  })

  it('initOTA creates and starts an OTA client', () => {
    const { initOTA } = require('../hooks')
    initOTA({
      endpoint: 'https://ota.example.com',
      channel: 'production',
      checkInterval: 0,
      updateStrategy: 'next-launch',
    })
    // Should not throw
  })

  it('useOTA returns signal-backed state', () => {
    const { initOTA, useOTA } = require('../hooks')
    initOTA({
      endpoint: 'https://ota.example.com',
      channel: 'production',
      checkInterval: 0,
      updateStrategy: 'next-launch',
    })
    const ota = useOTA()
    expect(ota).toBeDefined()
  })

  it('exports isUpdateAvailable and isDownloading computed signals', () => {
    const hooks = require('../hooks')
    expect(hooks.isUpdateAvailable).toBeDefined()
    expect(hooks.isDownloading).toBeDefined()
    expect(hooks.downloadProgress).toBeDefined()
  })
})
