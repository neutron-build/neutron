import type { ViewProps } from '../types.js'
import { styleToCSS } from '../web-compat/style.js'

/**
 * View — web implementation renders a <div> with converted inline styles.
 */
export function View({ children, style, testID, accessible, accessibilityLabel, pointerEvents, ...rest }: ViewProps) {
  return (
    <div
      data-testid={testID}
      aria-label={accessibilityLabel}
      role={accessible ? 'region' : undefined}
      style={{ display: 'flex', flexDirection: 'column', ...styleToCSS(style), pointerEvents } as preact.JSX.CSSProperties}
      {...(rest as Record<string, unknown>)}
    >
      {children}
    </div>
  )
}
