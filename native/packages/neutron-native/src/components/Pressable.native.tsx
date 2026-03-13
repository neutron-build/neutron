import { useState, useCallback } from 'react'
import { Pressable as RNPressable } from 'react-native'
import type { PressableProps } from '../types.js'

export function Pressable({
  children, onPress, onPressIn, onPressOut, onLongPress,
  style, disabled, hitSlop, className: _className, ...rest
}: PressableProps) {
  const [pressed, setPressed] = useState(false)

  const handlePressIn = useCallback((e: unknown) => {
    if (disabled) return
    setPressed(true)
    onPressIn?.(e)
  }, [disabled, onPressIn])

  const handlePressOut = useCallback((e: unknown) => {
    if (disabled) return
    setPressed(false)
    onPressOut?.(e)
  }, [disabled, onPressOut])

  const handlePress = useCallback((e: unknown) => {
    if (disabled) return
    onPress?.(e)
  }, [disabled, onPress])

  const resolvedStyle = typeof style === 'function' ? style({ pressed }) : style
  const resolvedChildren = typeof children === 'function' ? children({ pressed }) : children

  return (
    <RNPressable
      style={resolvedStyle}
      onPress={handlePress}
      onPressIn={handlePressIn}
      onPressOut={handlePressOut}
      onLongPress={onLongPress}
      disabled={disabled}
      hitSlop={hitSlop}
      {...rest}
    >
      {resolvedChildren}
    </RNPressable>
  )
}
