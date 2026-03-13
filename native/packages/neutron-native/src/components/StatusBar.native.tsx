import { StatusBar as RNStatusBar } from 'react-native'
import type { StatusBarProps } from '../types.js'

export function StatusBar(props: StatusBarProps) {
  return <RNStatusBar {...props} />
}
