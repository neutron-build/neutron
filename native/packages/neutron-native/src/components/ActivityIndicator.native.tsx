import { ActivityIndicator as RNActivityIndicator } from 'react-native'
import type { NativeStyleProp } from '../types.js'

export interface ActivityIndicatorProps {
  size?: 'small' | 'large' | number
  color?: string
  animating?: boolean
  style?: NativeStyleProp
  testID?: string
}

export function ActivityIndicator({ size = 'small', color, animating = true, style, testID }: ActivityIndicatorProps) {
  return <RNActivityIndicator size={size} color={color} animating={animating} style={style} testID={testID} />
}
