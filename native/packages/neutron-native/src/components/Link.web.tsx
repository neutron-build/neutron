/** @jsxImportSource preact */
import type { LinkProps } from './Link.native.js'
import { navigate } from '../router/navigator.js'
import { styleToCSS } from '../web-compat/style.js'

/**
 * Link — web implementation renders a semantic <a> tag.
 * Intercepts click to use the signal-based router (no full page load).
 * External links open in a new tab.
 */
export function Link({
  href,
  params,
  replace: shouldReplace,
  external,
  children,
  style,
  disabled,
  testID,
  accessibilityLabel,
}: LinkProps) {
  function handleClick(e: MouseEvent) {
    if (disabled) {
      // Block the browser's own navigation too — returning without
      // preventDefault still follows the href (NF-NR-14).
      e.preventDefault()
      return
    }
    if (external) return  // let browser handle it naturally

    // Don't intercept modifier-key clicks (open in new tab, etc.)
    if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return

    e.preventDefault()
    navigate(href, { replace: shouldReplace, params })
  }

  return (
    <a
      href={href}
      target={external ? '_blank' : undefined}
      rel={external ? 'noopener noreferrer' : undefined}
      data-testid={testID}
      aria-label={accessibilityLabel}
      aria-disabled={disabled}
      role="link"
      onClick={handleClick}
      style={{
        textDecoration: 'none',
        color: 'inherit',
        display: 'contents',
        // Whole style arrays convert through the bridge with RN's
        // later-wins flattening — not just the first entry (NF-NR-14).
        ...styleToCSS(style),
      } as preact.JSX.CSSProperties}
    >
      {children}
    </a>
  )
}