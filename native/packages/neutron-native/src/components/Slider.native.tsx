import { View } from 'react-native'

export interface SliderProps {
  value?: number
  minimumValue?: number
  maximumValue?: number
  step?: number
  onValueChange?: (value: number) => void
  onSlidingStart?: (value: number) => void
  onSlidingComplete?: (value: number) => void
  minimumTrackTintColor?: string
  maximumTrackTintColor?: string
  thumbTintColor?: string
  disabled?: boolean
  testID?: string
}

/**
 * Slider — use @react-native-community/slider for production.
 * This is a placeholder that renders an empty view.
 */
export function Slider({ testID }: SliderProps) {
  return <View testID={testID} />
}
