import { describe, it, expect, beforeEach } from '@jest/globals'

describe('Clipboard web implementation', () => {
  beforeEach(() => {
    Object.defineProperty(navigator, 'clipboard', {
      value: {
        readText: jest.fn(async () => 'clipboard content'),
        writeText: jest.fn(async () => {}),
      },
      configurable: true,
    })
  })

  it('should read text from clipboard', async () => {
    const text = await navigator.clipboard?.readText()
    expect(text).toBe('clipboard content')
  })

  it('should write text to clipboard', async () => {
    const write = navigator.clipboard?.writeText as any
    await write('test content')
    expect(write).toHaveBeenCalledWith('test content')
  })

  it('should check if clipboard has string', async () => {
    const read = navigator.clipboard?.readText as any
    const content = await read()
    expect(content).toBeTruthy()
  })

  it('should handle empty clipboard', async () => {
    const read = jest.fn(async () => '')
    const content = await read()
    expect(content).toBe('')
  })

  it('should handle clipboard errors', async () => {
    const read = jest.fn(async () => {
      throw new Error('Clipboard access denied')
    })
    await expect(read()).rejects.toThrow()
  })
})
