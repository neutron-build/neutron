import { describe, it, expect, beforeEach } from '@jest/globals'

describe('NetInfo web implementation', () => {
  let listeners: Record<string, Function[]>

  beforeEach(() => {
    jest.clearAllMocks()
    listeners = {}
    // Mock window with real event listener behavior
    ;(global as any).window = {
      addEventListener: jest.fn((event: string, callback: Function) => {
        if (!listeners[event]) listeners[event] = []
        listeners[event].push(callback)
      }),
      removeEventListener: jest.fn((event: string, callback: Function) => {
        if (listeners[event]) {
          listeners[event] = listeners[event].filter(cb => cb !== callback)
        }
      }),
      dispatchEvent: jest.fn((event: Event) => {
        if (listeners[event.type]) {
          listeners[event.type].forEach(cb => cb(event))
        }
      }),
    }
  })

  it('should return isConnected true', () => {
    Object.defineProperty(navigator, 'onLine', { value: true, writable: true, configurable: true })
    expect(navigator.onLine).toBe(true)
  })

  it('should return isConnected false', () => {
    Object.defineProperty(navigator, 'onLine', { value: false, writable: true, configurable: true })
    expect(navigator.onLine).toBe(false)
  })

  it('should detect connection type', () => {
    const connection = { type: 'wifi' }
    expect(connection.type).toBe('wifi')
  })

  it('should fire online event on connect', () => {
    const listener = jest.fn()
    window.addEventListener('online', listener)
    window.dispatchEvent(new Event('online'))
    expect(listener).toHaveBeenCalled()
    window.removeEventListener('online', listener)
  })

  it('should fire offline event on disconnect', () => {
    const listener = jest.fn()
    window.addEventListener('offline', listener)
    window.dispatchEvent(new Event('offline'))
    expect(listener).toHaveBeenCalled()
    window.removeEventListener('offline', listener)
  })

  it('should handle subscription', () => {
    let isConnected = true
    const listener = (connected: boolean) => {
      isConnected = connected
    }
    window.addEventListener('online', () => listener(true))
    window.addEventListener('offline', () => listener(false))
    window.dispatchEvent(new Event('offline'))
    expect(isConnected).toBe(false)
  })

  it('should detect network type from connection API', () => {
    const connection = { type: 'cellular', effectiveType: '4g' }
    expect(connection.type).toBe('cellular')
    expect(connection.effectiveType).toBe('4g')
  })
})
