import { KeyboardAvoidingView as RNKeyboardAvoidingView, Platform } from 'react-native'
import type { ReactNode } from 'react'
import type { NativeStyleProp } from '../types.js'

export interface KeyboardAvoidingViewProps {
  behavior?: 'height' | 'position' | 'padding'
  keyboardVerticalOffset?: number
  style?: NativeStyleProp
  contentContainerStyle?: NativeStyleProp
  enabled?: boolean
  testID?: string
  children?: ReactNode
}

export function KeyboardAvoidingView({
  behavior = Platform.OS === 'ios' ? 'padding' : 'height',
  children, ...rest
}: KeyboardAvoidingViewProps) {
  return (
    <RNKeyboardAvoidingView behavior={behavior} {...rest}>
      {children}
    </RNKeyboardAvoidingView>
  )
}
