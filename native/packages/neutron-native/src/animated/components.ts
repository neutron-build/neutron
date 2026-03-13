/**
 * Animated component wrappers — apply animated styles via JSI worklets.
 *
 * Usage:
 *   import { Animated } from '@neutron/native/animated'
 *   <Animated.View style={animatedStyle}>...</Animated.View>
 */

import React, { type ReactNode } from 'react'
import { View, Text, Image, ScrollView } from 'react-native'
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

/**
 * Animated.View — View with animated style support.
 * The style prop can include values driven by shared values and worklets.
 */
function AnimatedView({ children, style, testID, ...rest }: AnimatedViewProps) {
  // In production: register a NativeAnimatedModule observer that
  // updates this view's props on each animation frame via JSI,
  // bypassing the JS bridge entirely.
  return React.createElement(View, { style: style as any, testID, ...rest }, children)
}

/** Animated.Text — Text with animated style support. */
function AnimatedText({ children, style, testID, ...rest }: AnimatedTextProps) {
  return React.createElement(Text, { style: style as any, testID, ...rest }, children)
}

/** Animated.Image — Image with animated style support. */
function AnimatedImage({ style, source, testID, ...rest }: AnimatedImageProps) {
  const src = typeof source === 'string' ? { uri: source } : source
  return React.createElement(Image, { style: style as any, source: src as any, testID, ...rest })
}

/** Animated.ScrollView — ScrollView with animated scroll events. */
function AnimatedScrollView({ children, style, testID, ...rest }: AnimatedScrollViewProps) {
  return React.createElement(ScrollView, { style: style as any, testID, ...rest }, children)
}

/**
 * Animated namespace — mirrors react-native-reanimated API.
 *
 * @example
 * import { Animated } from '@neutron/native/animated'
 *
 * function FadeIn({ children }) {
 *   const opacity = useSharedValue(0)
 *   const style = useAnimatedStyle(() => ({ opacity: opacity.value }))
 *
 *   useEffect(() => { opacity.value = withTiming(1) }, [])
 *
 *   return <Animated.View style={style}>{children}</Animated.View>
 * }
 */
export const Animated = {
  View: AnimatedView,
  Text: AnimatedText,
  Image: AnimatedImage,
  ScrollView: AnimatedScrollView,
} as const
