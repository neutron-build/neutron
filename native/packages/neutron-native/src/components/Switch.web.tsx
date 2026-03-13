import type { SwitchProps } from './Switch.native.js'
import { styleToCSS } from '../web-compat/style.js'

export function Switch({ value, onValueChange, disabled, trackColor, thumbColor, style, testID }: SwitchProps) {
  const track = value ? (trackColor?.true ?? '#34c759') : (trackColor?.false ?? '#e5e5ea')

  return (
    <div
      data-testid={testID}
      role="switch"
      aria-checked={value}
      onClick={disabled ? undefined : () => onValueChange?.(!value)}
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        width: 51,
        height: 31,
        borderRadius: 16,
        backgroundColor: track,
        cursor: disabled ? 'not-allowed' : 'pointer',
        opacity: disabled ? 0.4 : 1,
        transition: 'background 0.2s',
        padding: 2,
        ...styleToCSS(style),
      } as preact.JSX.CSSProperties}
    >
      <div style={{
        width: 27,
        height: 27,
        borderRadius: '50%',
        backgroundColor: thumbColor ?? '#fff',
        boxShadow: '0 1px 3px rgba(0,0,0,0.3)',
        transform: value ? 'translateX(20px)' : 'translateX(0)',
        transition: 'transform 0.2s',
      }} />
    </div>
  )
}
