import { SafeAreaView as RNSafeAreaView } from 'react-native'
import type { ViewProps } from '../types.js'

export function SafeAreaView({ children, style, className: _className, ...rest }: ViewProps) {
  return <RNSafeAreaView style={style} {...rest}>{children}</RNSafeAreaView>
}
