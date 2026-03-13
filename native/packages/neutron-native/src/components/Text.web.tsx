import type { TextProps } from '../types.js'
import { styleToCSS } from '../web-compat/style.js'

/**
 * Text — web implementation renders a <span>.
 * numberOfLines maps to CSS line-clamp.
 */
export function Text({ children, style, numberOfLines, onPress, testID, selectable, ...rest }: TextProps) {
  const clampStyle = numberOfLines != null
    ? { display: '-webkit-box', WebkitLineClamp: numberOfLines, WebkitBoxOrient: 'vertical', overflow: 'hidden' }
    : {}

  return (
    <span
      data-testid={testID}
      style={{
        userSelect: selectable === false ? 'none' : undefined,
        ...styleToCSS(style),
        ...clampStyle,
      } as preact.JSX.CSSProperties}
      onClick={onPress ? (e: MouseEvent) => onPress(e) : undefined}
      {...(rest as Record<string, unknown>)}
    >
      {children}
    </span>
  )
}
