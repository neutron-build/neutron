import { View as RNView } from 'react-native'
import type { ViewProps } from '../types.js'

export function View({ children, style, className: _className, ...rest }: ViewProps) {
  return <RNView style={style} {...rest}>{children}</RNView>
}
