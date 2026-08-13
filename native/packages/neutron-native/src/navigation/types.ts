import type { ComponentType, ReactNode } from 'react'
import type { TextStyle, ViewStyle } from 'react-native'

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
  /** Merged into the header bar's style object, so it must be a plain style — not an array. */
  headerStyle?: ViewStyle
  headerTintColor?: string
  /** Merged into the header title's style object, so it must be a plain style — not an array. */
  headerTitleStyle?: TextStyle
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
