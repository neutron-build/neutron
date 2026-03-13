import { Switch as RNSwitch } from 'react-native'

export interface SwitchProps {
  value?: boolean
  onValueChange?: (value: boolean) => void
  disabled?: boolean
  trackColor?: { false?: string; true?: string }
  thumbColor?: string
  ios_backgroundColor?: string
  testID?: string
}

export function Switch(props: SwitchProps) {
  return <RNSwitch {...props} />
}
