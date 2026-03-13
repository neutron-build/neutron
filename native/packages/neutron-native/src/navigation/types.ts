import type { ComponentType, ReactNode } from 'react'
import type { NativeStyleProp } from '../types.js'

export interface ScreenConfig {
  /** Route name / segment */
  name: string
  /** Component to render */
  component: ComponentType
  /** Options for this screen */
  options?: ScreenOptions
}

export interface ScreenOptions {
  title?: string
  /** Hide the header entirely */
  headerShown?: boolean
  headerStyle?: NativeStyleProp
  headerTintColor?: string
  headerTitleStyle?: NativeStyleProp
  /** Tab-specific */
  tabBarLabel?: string
  tabBarIcon?: ComponentType<{ focused: boolean; color: string; size: number }>
  tabBarBadge?: string | number
  /** Prevent going back (e.g. after logout) */
  gestureEnabled?: boolean
}

export interface NavigatorProps {
  children?: ReactNode
  initialRouteName?: string
  screenOptions?: ScreenOptions
}
