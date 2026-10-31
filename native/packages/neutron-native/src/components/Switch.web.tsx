/** @jsxImportSource preact */
import type { SwitchProps } from './Switch.native.js'
import { styleToCSS } from '../web-compat/style.js'

export function Switch({ value, onValueChange, disabled, trackColor, thumbColor, style, testID }: SwitchProps) {
  const track = value ? (trackColor?.true ?? '#34c759') : (trackColor?.false ?? '#e5e5ea')

  function handleKey(e: KeyboardEvent) {
    if (disabled) return
    // A switch is operable from the keyboard (NF-NR-14): Space and Enter
    // toggle, matching the WAI-ARIA switch pattern.
    if (e.key === ' ' || e.key === 'Enter') {
      e.preventDefault()
      onValueChange?.(!value)
    }
  }

  return (
    <div
      data-testid={testID}
      role="switch"
      aria-checked={value}
      aria-disabled={disabled}
      aria-label={testID}
      tabIndex={disabled ? -1 : 0}
      onClick={disabled ? undefined : () => onValueChange?.(!value)}
      onKeyDown={handleKey}
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