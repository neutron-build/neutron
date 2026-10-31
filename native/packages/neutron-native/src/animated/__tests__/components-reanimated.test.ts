/**
 * NF-NR-04 regressions — with the Reanimated peer installed, the exported
 * Animated hosts ARE the provider's hosts, so styles produced by the
 * provider's useAnimatedStyle land on a host that understands them.
 */

jest.mock('react-native-reanimated', () => {
  const host = (name: string) => (props: Record<string, unknown>) => ({ type: `Reanimated${name}`, props })
  return {
    __esModule: true,
    default: {
      Animated: {
        View: host('View'),
        Text: host('Text'),
        Image: host('Image'),
        ScrollView: host('ScrollView'),
      },
    },
  }
  // Virtual: the peer is not installed in this workspace; the mock stands in.
}, { virtual: true })

import { Animated, usingReanimatedHosts } from '../components'

export {}

describe('NF-NR-04 Reanimated-backed Animated hosts', () => {
  it('reports the Reanimated provider', () => {
    expect(usingReanimatedHosts()).toBe(true)
  })

  it('exports the provider hosts for View/Text/Image/ScrollView', () => {
    // Mock createElement keeps the component reference: invoking it is the
    // provider host, whose element identifies itself.
    const view = (Animated.View as any)({ style: { opacity: 1 }, entering: {} })
    expect(view.type(view.props).type).toBe('ReanimatedView')
    expect((Animated.Text as any)({}).type({}).type).toBe('ReanimatedText')
    expect((Animated.Image as any)({ source: 'https://x/i.png' }).props.source).toEqual({ uri: 'https://x/i.png' })
    expect((Animated.ScrollView as any)({}).type({}).type).toBe('ReanimatedScrollView')
  })

  it('accepts entering/exiting/layout on the provider host', () => {
    expect(() => (Animated.View as any)({ entering: { type: 'fade-in' } })).not.toThrow()
  })
})
