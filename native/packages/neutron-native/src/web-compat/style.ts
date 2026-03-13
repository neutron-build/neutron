/**
 * RN StyleSheet → CSS inline style converter (web target only).
 *
 * React Native style props use camelCase matching CSS, so most pass through
 * directly. We only need to handle the RN-specific properties that don't
 * have a 1:1 CSS equivalent.
 */

import type { NativeStyleProp } from '../types.js'

type CSSObject = Record<string, string | number | undefined>

const SHADOW_SKIP = new Set(['shadowColor', 'shadowOffset', 'shadowOpacity', 'shadowRadius'])

/**
 * Convert a React Native style object (or array) to a plain CSS object
 * suitable for use as a Preact inline style.
 */
export function styleToCSS(style: NativeStyleProp | NativeStyleProp[] | undefined | null): CSSObject {
  if (!style) return {}
  if (Array.isArray(style)) {
    return style.reduce<CSSObject>((acc, s) => ({ ...acc, ...styleToCSS(s) }), {})
  }

  const css: CSSObject = {}

  for (const [key, value] of Object.entries(style as Record<string, unknown>)) {
    // Skip native-only shadow props (web uses boxShadow separately)
    if (SHADOW_SKIP.has(key)) continue

    // RN elevation → CSS box-shadow approximation
    if (key === 'elevation') {
      const el = Number(value)
      css.boxShadow = `0 ${el}px ${el * 2}px rgba(0,0,0,0.2)`
      continue
    }

    // RN uses numeric font weights; CSS accepts them directly
    css[key] = value as string | number
  }

  return css
}
