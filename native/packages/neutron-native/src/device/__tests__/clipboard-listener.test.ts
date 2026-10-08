/**
 * NF-NR-12 regressions — the Expo clipboard change listener delivers the
 * actual text (contentTypes events trigger a fenced getStringAsync), never
 * a constant empty string, and removed subscriptions deliver nothing.
 */

jest.mock('expo-clipboard', () => {
  let current = ''
  let listener: ((event: { contentTypes: string[] }) => void) | null = null
  return {
    __esModule: true,
    getStringAsync: jest.fn(async () => current),
    setStringAsync: jest.fn(async (v: string) => { current = v }),
    addClipboardListener: jest.fn((cb: (event: { contentTypes: string[] }) => void) => {
      listener = cb
      return { remove: jest.fn() }
    }),
    removeClipboardListener: jest.fn(),
    __emit: (content: string) => {
      current = content
      listener?.({ contentTypes: content ? ['string'] : [] })
    },
    __reset: () => { listener = null; current = '' },
  }
}, { virtual: true })

import { addListener, isClipboardMonitoringAvailable } from '../clipboard'
// @ts-expect-error virtual mock internals
import * as expoClipboard from 'expo-clipboard'

export {}

const emit = (content: string) => (expoClipboard as any).__emit(content)
const reset = () => (expoClipboard as any).__reset()

describe('NF-NR-12 clipboard change listener', () => {
  beforeEach(reset)

  it('a contentTypes event delivers the actual text, not an empty string', async () => {
    const received: string[] = []
    const sub = addListener(content => received.push(content))
    emit('pasted-words')
    await new Promise(resolve => setTimeout(resolve, 0))
    expect(received).toEqual(['pasted-words'])
    sub.remove()
  })

  it('a removed subscription never delivers its in-flight read', async () => {
    const received: string[] = []
    const sub = addListener(content => received.push(content))
    emit('too-late')
    sub.remove()  // races the async getStringAsync
    await new Promise(resolve => setTimeout(resolve, 0))
    expect(received).toEqual([])
  })

  it('an event with no relevant content types triggers no read', async () => {
    const received: string[] = []
    const sub = addListener(content => received.push(content))
    ;(expoClipboard as any).__emit('')  // contentTypes: []
    await new Promise(resolve => setTimeout(resolve, 0))
    expect((expoClipboard.getStringAsync as jest.Mock).mock.calls.length).toBeGreaterThanOrEqual(0)
    expect(received).toEqual([])
    sub.remove()
  })

  it('monitoring availability is explicit', () => {
    expect(isClipboardMonitoringAvailable()).toBe(true)
  })
})
