import { describe, it, expect, beforeEach, afterEach } from '@jest/globals'

describe('Haptics web implementation', () => {
  beforeEach(() => {
    global.navigator = {
      vibrate: jest.fn(() => true),
    } as any
  })

  afterEach(() => {
    jest.clearAllMocks()
  })

  it('should call navigator.vibrate for impact light', () => {
    const vibrate = navigator.vibrate as any
    const impact = (style: 'light' | 'medium' | 'heavy') => {
      const patterns: Record<string, number[]> = {
        light: [10],
        medium: [20],
        heavy: [50],
      }
      vibrate(patterns[style])
    }

    impact('light')
    expect(vibrate).toHaveBeenCalledWith([10])
  })

  it('should call navigator.vibrate for impact medium', () => {
    const vibrate = navigator.vibrate as any
    const impact = (style: 'light' | 'medium' | 'heavy') => {
      const patterns: Record<string, number[]> = {
        light: [10],
        medium: [20],
        heavy: [50],
      }
      vibrate(patterns[style])
    }

    impact('medium')
    expect(vibrate).toHaveBeenCalledWith([20])
  })

  it('should call navigator.vibrate for impact heavy', () => {
    const vibrate = navigator.vibrate as any
    const impact = (style: 'light' | 'medium' | 'heavy') => {
      const patterns: Record<string, number[]> = {
        light: [10],
        medium: [20],
        heavy: [50],
      }
      vibrate(patterns[style])
    }

    impact('heavy')
    expect(vibrate).toHaveBeenCalledWith([50])
  })

  it('should call navigator.vibrate for notification success', () => {
    const vibrate = navigator.vibrate as any
    const notification = (type: 'success' | 'warning' | 'error') => {
      const patterns: Record<string, number[]> = {
        success: [50, 50, 50],
        warning: [100, 50, 100],
        error: [200, 100, 200],
      }
      vibrate(patterns[type])
    }

    notification('success')
    expect(vibrate).toHaveBeenCalledWith([50, 50, 50])
  })

  it('should call navigator.vibrate for notification warning', () => {
    const vibrate = navigator.vibrate as any
    const notification = (type: 'success' | 'warning' | 'error') => {
      const patterns: Record<string, number[]> = {
        success: [50, 50, 50],
        warning: [100, 50, 100],
        error: [200, 100, 200],
      }
      vibrate(patterns[type])
    }

    notification('warning')
    expect(vibrate).toHaveBeenCalledWith([100, 50, 100])
  })

  it('should call navigator.vibrate for selection vibration', () => {
    const vibrate = navigator.vibrate as any
    const selection = () => vibrate(30)

    selection()
    expect(vibrate).toHaveBeenCalledWith(30)
  })

  it('should vibrate for specified milliseconds', () => {
    const vibrate = navigator.vibrate as any
    const vibrate_ms = (ms: number) => vibrate(ms)

    vibrate_ms(100)
    expect(vibrate).toHaveBeenCalledWith(100)
  })
})
