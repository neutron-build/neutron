import { Text as RNText } from 'react-native'
import type { TextProps } from '../types.js'

export function Text({ children, style, numberOfLines, ellipsizeMode, onPress, selectable, className: _className, ...rest }: TextProps) {
  return (
    <RNText
      style={style}
      numberOfLines={numberOfLines}
      ellipsizeMode={ellipsizeMode}
      onPress={onPress}
      selectable={selectable}
      {...rest}
    >
      {children}
    </RNText>
  )
}
