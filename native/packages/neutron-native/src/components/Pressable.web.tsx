import { useSignal } from '@preact/signals'
import type { PressableProps, NativeStyleProp } from '../types.js'
import { styleToCSS } from '../web-compat/style.js'

export function Pressable({
  children,
  onPress,
  onPressIn,
  onPressOut,
  style,
  disabled,
  testID,
  accessibilityLabel,
  accessibilityRole,
  ...rest
}: PressableProps) {
  const pressed = useSignal(false)

  const resolvedStyleRaw = typeof style === 'function' ? style({ pressed: pressed.value }) : style
  const resolvedStyle = Array.isArray(resolvedStyleRaw) ? resolvedStyleRaw[0] : resolvedStyleRaw as NativeStyleProp | undefined
  const resolvedChildren = typeof children === 'function' ? children({ pressed: pressed.value }) : children

  return (
    <button
      data-testid={testID}
      aria-label={accessibilityLabel}
      role={accessibilityRole as preact.JSX.AriaRole}
      disabled={disabled}
      style={{
        background: 'none',
        border: 'none',
        padding: 0,
        cursor: disabled ? 'not-allowed' : 'pointer',
        display: 'flex',
        ...styleToCSS(resolvedStyle),
      } as preact.JSX.CSSProperties}
      onMouseDown={() => { pressed.value = true; onPressIn?.({}) }}
      onMouseUp={() => { pressed.value = false; onPressOut?.({}) }}
      onMouseLeave={() => { pressed.value = false }}
      onClick={onPress ? () => onPress({}) : undefined}
      {...(rest as Record<string, unknown>)}
    >
      {resolvedChildren}
    </button>
  )
}
