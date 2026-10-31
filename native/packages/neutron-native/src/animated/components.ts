/**
 * Animated component wrappers.
 *
 * HOSTS AND HOOKS STAY ON THE SAME PROVIDER (NF-NR-04): when
 * react-native-reanimated is installed, `Animated.View` & co. are the
 * Reanimated hosts — the only hosts that understand the styles produced by
 * `useAnimatedStyle`. Without the peer, the exported components are React
 * Native's own JS-driven `Animated` hosts (they accept `Animated.Value`
 * styles from the fallback hook layer) — NOT plain views pretending.
 *
 * `entering` / `exiting` / `layout` are Reanimated-only layout transitions;
 * using them without the peer throws instead of silently no-oping.
 *
 * Usage:
 *   import { Animated } from '@neutron-build/native/animated'
 *   <Animated.View style={animatedStyle}>...</Animated.View>
 */

import React, { forwardRef, type ReactNode } from 'react'
import { Animated as RNAnimated } from 'react-native'
import type { NativeStyleProp } from '../types.js'

interface AnimatedViewProps {
  style?: NativeStyleProp | NativeStyleProp[]
  className?: string
  testID?: string
  children?: ReactNode
  entering?: unknown
  exiting?: unknown
  layout?: unknown
}

interface AnimatedTextProps {
  style?: NativeStyleProp | NativeStyleProp[]
  className?: string
  testID?: string
  numberOfLines?: number
  children?: ReactNode
}

interface AnimatedImageProps {
  style?: NativeStyleProp | NativeStyleProp[]
  source: string | number | { uri: string }
  resizeMode?: 'cover' | 'contain' | 'stretch' | 'repeat' | 'center'
  testID?: string
}

interface AnimatedScrollViewProps {
  style?: NativeStyleProp | NativeStyleProp[]
  contentContainerStyle?: NativeStyleProp
  horizontal?: boolean
  onScroll?: (event: unknown) => void
  scrollEventThrottle?: number
  testID?: string
  children?: ReactNode
}

let _reanimatedHosts: Record<string, unknown> | null | undefined

/** Resolve the Reanimated host components, or null when the peer is absent. */
function getReanimatedHosts(): Record<string, unknown> | null {
  if (_reanimatedHosts === undefined) {
    _reanimatedHosts = null
    try {
      // Lazy peer resolution — never at module load.
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const mod: any = require('react-native-reanimated')
      const animated = mod?.default?.Animated ?? mod?.Animated
      if (animated && typeof animated.View === 'function') _reanimatedHosts = animated
    } catch {
      _reanimatedHosts = null
    }
  }
  return _reanimatedHosts ?? null
}

/** True when the exported hosts are Reanimated's (worklet-compatible). */
export function usingReanimatedHosts(): boolean {
  return getReanimatedHosts() !== null
}

function assertNoLayoutTransitions(props: { entering?: unknown; exiting?: unknown; layout?: unknown }): void {
  const used = (['entering', 'exiting', 'layout'] as const).filter(key => props[key] !== undefined)
  if (used.length > 0) {
    throw new Error(
      `Animated <View> received ${used.join(', ')} — layout transitions require react-native-reanimated, ` +
      'which is not installed. The no-peer components support Animated.Value styles only.',
    )
  }
}

/**
 * Animated.View — View whose host matches the animation provider:
 * Reanimated's View when installed, RN's Animated.View otherwise.
 */
const AnimatedView = forwardRef<unknown, AnimatedViewProps>(function AnimatedView(props, ref) {
  const hosts = getReanimatedHosts()
  if (hosts) {
    return React.createElement((hosts as any).View, { ...props, ref })
  }
  assertNoLayoutTransitions(props)
  const { children, style, testID, ...rest } = props
  return React.createElement(RNAnimated.View, { style: style as any, testID, ...(rest as any), ref })
})

/** Animated.Text — Text on the active animation provider's host. */
const AnimatedText = forwardRef<unknown, AnimatedTextProps>(function AnimatedText(props, ref) {
  const hosts = getReanimatedHosts()
  if (hosts) {
    return React.createElement((hosts as any).Text, { ...props, ref })
  }
  const { children, style, testID, ...rest } = props
  return React.createElement(RNAnimated.Text, { style: style as any, testID, ...(rest as any), ref })
})

/** Animated.Image — Image on the active animation provider's host. */
const AnimatedImage = forwardRef<unknown, AnimatedImageProps>(function AnimatedImage(props, ref) {
  const hosts = getReanimatedHosts()
  if (hosts) {
    return React.createElement((hosts as any).Image, { ...props, source: normalizeSource(props.source), ref })
  }
  const { style, source, testID, ...rest } = props
  return React.createElement(RNAnimated.Image, { style: style as any, source: normalizeSource(source) as any, testID, ...(rest as any), ref })
})

/** Animated.ScrollView — ScrollView on the active animation provider's host. */
const AnimatedScrollView = forwardRef<unknown, AnimatedScrollViewProps>(function AnimatedScrollView(props, ref) {
  const hosts = getReanimatedHosts()
  if (hosts) {
    return React.createElement((hosts as any).ScrollView, { ...props, ref })
  }
  const { children, style, testID, ...rest } = props
  return React.createElement(RNAnimated.ScrollView, { style: style as any, testID, ...(rest as any), ref })
})

function normalizeSource(source: string | number | { uri: string }): string | number | { uri: string } {
  return typeof source === 'string' ? { uri: source } : source
}

/**
 * Animated namespace. The hosts and the hooks (useAnimatedStyle etc.) are
 * resolved from the SAME provider, so a style produced by the hook layer is
 * always understood by the host that receives it.
 */
export const Animated = {
  View: AnimatedView,
  Text: AnimatedText,
  Image: AnimatedImage,
  ScrollView: AnimatedScrollView,
} as const
