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
  testID?: string
}

export function Link({ href, children, style, pressableStyle, testID }: LinkProps) {
  return (
    <Pressable
      style={pressableStyle}
      onPress={() => navigate(href)}
      testID={testID}
    >
      <Text style={style}>{children}</Text>
    </Pressable>
  )
}
