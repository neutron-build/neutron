import { ActivityIndicator as RNActivityIndicator } from 'react-native'

export interface ActivityIndicatorProps {
  size?: 'small' | 'large' | number
  color?: string
  animating?: boolean
  testID?: string
}

export function ActivityIndicator({ size = 'small', color, animating = true, testID }: ActivityIndicatorProps) {
  return <RNActivityIndicator size={size} color={color} animating={animating} testID={testID} />
}
