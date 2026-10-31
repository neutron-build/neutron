/**
 * NF-NR-04 regressions — Animated hosts on the fallback (no Reanimated)
 * provider: they must be RN's Animated hosts, not plain views, and layout
 * transitions must fail loudly instead of no-oping.
 */

import { Animated, usingReanimatedHosts } from '../components'
import { Animated as RNAnimated, View } from 'react-native'

export {}

describe('NF-NR-04 fallback Animated hosts (no Reanimated peer)', () => {
  it('reports the fallback provider', () => {
    expect(usingReanimatedHosts()).toBe(false)
  })

  it('renders through RN Animated hosts, not plain View/Text', () => {
    // The mock react's forwardRef is the identity, so calling the component
    // invokes the render and mock createElement returns { type, props }.
    const view = (Animated.View as any)({ style: { opacity: 1 }, testID: 'a' })
    expect(view).toBeTruthy()
    expect(view.type).toBe(RNAnimated.View)     // RN's Animated host…
    expect(view.type).not.toBe(View)            // …never the plain View
    expect(view.props.style).toEqual({ opacity: 1 })
    const text = (Animated.Text as any)({ testID: 't' })
    expect(text.type).toBe(RNAnimated.Text)
    const image = (Animated.Image as any)({ source: 'https://x/img.png' })
    expect(image.type).toBe(RNAnimated.Image)
    expect(image.props.source).toEqual({ uri: 'https://x/img.png' })
    const scroll = (Animated.ScrollView as any)({ testID: 's' })
    expect(scroll.type).toBe(RNAnimated.ScrollView)
  })

  it('throws on entering/exiting/layout — never a silent no-op', () => {
    expect(() => (Animated.View as any)({ entering: { type: 'fade-in' } })).toThrow(/react-native-reanimated/)
    expect(() => (Animated.View as any)({ exiting: {} })).toThrow(/layout transitions/)
    expect(() => (Animated.View as any)({ layout: {} })).toThrow(/layout transitions/)
  })
})
