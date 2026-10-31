import { Text, Pressable, Linking } from 'react-native'
import { navigate } from '../router/navigator.js'
import type { ReactNode } from 'react'
import type { NativeStyleProp, NativeTextStyleProp } from '../types.js'

export interface LinkProps {
  href: string
  children?: ReactNode
  style?: NativeTextStyleProp
  pressableStyle?: NativeStyleProp
  replace?: boolean
  params?: Record<string, string>
  /** Render a plain anchor (web) / open through Linking (native) and let
   * the system handle the navigation — for absolute external URLs. */
  external?: boolean
  disabled?: boolean
  accessibilityLabel?: string
  testID?: string
}

export function Link({
  href,
  children,
  style,
  pressableStyle,
  replace,
  params,
  external,
  disabled,
  accessibilityLabel,
  testID,
}: LinkProps) {
  return (
    <Pressable
      style={pressableStyle}
      // External native links go through Linking.openURL (NF-NR-14):
      // the signal router only understands in-app routes.
      onPress={() => {
        if (external) {
          void Linking.openURL(href)
          return
        }
        navigate(href, { replace, params })
      }}
      disabled={disabled}
      accessibilityLabel={accessibilityLabel}
      accessibilityRole="link"
      testID={testID}
    >
      <Text style={style}>{children}</Text>
    </Pressable>
  )
}
