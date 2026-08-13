import { Switch as RNSwitch } from 'react-native'
import type { NativeStyleProp } from '../types.js'

export interface SwitchProps {
  value?: boolean
  onValueChange?: (value: boolean) => void
  disabled?: boolean
  trackColor?: { false?: string; true?: string }
  thumbColor?: string
  ios_backgroundColor?: string
  style?: NativeStyleProp
  testID?: string
}

export function Switch(props: SwitchProps) {
  return <RNSwitch {...props} />
}
