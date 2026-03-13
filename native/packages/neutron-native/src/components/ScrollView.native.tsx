import { ScrollView as RNScrollView } from 'react-native'
import type { ScrollViewProps } from '../types.js'

export function ScrollView({ children, className: _className, ...props }: ScrollViewProps) {
  return <RNScrollView {...props}>{children}</RNScrollView>
}
