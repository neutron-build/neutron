import { Text, Pressable } from 'react-native'
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
  /** Web only: render a plain anchor and let the browser handle the navigation. */
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
  disabled,
  accessibilityLabel,
  testID,
}: LinkProps) {
  return (
    <Pressable
      style={pressableStyle}
      onPress={() => navigate(href, { replace, params })}
      disabled={disabled}
      accessibilityLabel={accessibilityLabel}
      testID={testID}
    >
      <Text style={style}>{children}</Text>
    </Pressable>
  )
}
