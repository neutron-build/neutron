/**
 * RN StyleSheet → CSS inline style converter (web target only).
 *
 * React Native style props use camelCase matching CSS, so most pass through
 * directly. The RN-specific properties without a 1:1 CSS equivalent get a
 * TESTED mapping; anything the converter does not knowingly support is
 * dropped with a console warning rather than leaking native-only values
 * into CSS (NF-NR-13). Arrays pass through the converter element-wise,
 * preserving RN's later-wins flattening order.
 */

import type { NativeStyleProp, NativeTextStyleProp, NativeImageStyleProp } from '../types.js'

type CSSObject = Record<string, string | number | undefined>

/**
 * Anything React Native accepts as a style prop, including the arbitrarily
 * nested (and possibly readonly) arrays that `StyleProp<T>` permits.
 */
export type StyleInput =
  | NativeStyleProp
  | NativeTextStyleProp
  | NativeImageStyleProp
  | readonly StyleInput[]

/** RN-only properties with no direct CSS meaning on this bridge. */
const NATIVE_ONLY = new Set([
  'shadowColor', 'shadowOffset', 'shadowOpacity', 'shadowRadius', // boxShadow is authored directly
  'paddingHorizontal', 'paddingVertical',
  'marginHorizontal', 'marginVertical',
])

/** Axis shorthands expand to their physical CSS pairs (order: X then Y). */
function axisShorthand(key: string, value: unknown): Array<[string, string | number]> | null {
  switch (key) {
    case 'paddingHorizontal': return [['paddingLeft', cssValue(value)], ['paddingRight', cssValue(value)]]
    case 'paddingVertical': return [['paddingTop', cssValue(value)], ['paddingBottom', cssValue(value)]]
    case 'marginHorizontal': return [['marginLeft', cssValue(value)], ['marginRight', cssValue(value)]]
    case 'marginVertical': return [['marginTop', cssValue(value)], ['marginBottom', cssValue(value)]]
    default: return null
  }
}

/** Length-unit semantics: unitless RN numbers are px in CSS. */
function cssValue(value: unknown): string | number {
  return typeof value === 'number' ? `${value}px` : (value as string)
}

/** transform arrays ({ perspective, translateX, ... }) → CSS transform string. */
function transformToCSS(value: unknown): string | undefined {
  if (!Array.isArray(value)) return typeof value === 'string' ? value : undefined
  const parts: string[] = []
  for (const entry of value) {
    if (!entry || typeof entry !== 'object') continue
    for (const [fn, arg] of Object.entries(entry as Record<string, unknown>)) {
      if (typeof arg === 'string') { parts.push(`${fn}(${arg})`); continue }
      parts.push(`${fn}(${arg}${transformUnit(fn)})`)
    }
  }
  return parts.join(' ')
}

/** Per-function units: translations are px, rotations are degrees,
 * scales/skews are unitless (NF-NR-13). */
function transformUnit(fn: string): string {
  if (fn.startsWith('translate') || fn === 'perspective') return 'px'
  if (fn.startsWith('rotate')) return 'deg'
  return ''
}

const warned = new Set<string>()
function warnOnce(key: string): void {
  if (process.env.NODE_ENV !== 'production' && !warned.has(key)) {
    warned.add(key)
    console.warn(`[neutron-native/web-compat] style property "${key}" has no CSS mapping on web and was dropped`)
  }
}

/**
 * Convert a React Native style object (or array) to a plain CSS object
 * suitable for use as a Preact inline style.
 */
export function styleToCSS(style: StyleInput): CSSObject {
  if (!style) return {}
  if (Array.isArray(style)) {
    // Whole arrays convert element-wise: RN flattens with later entries
    // winning, and so does this merge.
    return style.reduce<CSSObject>((acc, s) => ({ ...acc, ...styleToCSS(s) }), {})
  }

  const css: CSSObject = {}

  for (const [key, value] of Object.entries(style as Record<string, unknown>)) {
    if (value === undefined || value === null) continue

    if (key === 'elevation') {
      const el = Number(value)
      css.boxShadow = `0 ${el}px ${el * 2}px rgba(0,0,0,0.2)`
      continue
    }

    if (key === 'transform') {
      const transform = transformToCSS(value)
      if (transform) css.transform = transform
      continue
    }

    if (key === 'transformMatrix') {
      warnOnce(key)
      continue
    }

    const shorthand = axisShorthand(key, value)
    if (shorthand) {
      for (const [prop, val] of shorthand) css[prop] = val
      continue
    }

    if (NATIVE_ONLY.has(key)) {
      warnOnce(key)
      continue
    }

    // Dimensional CSS properties need units from bare RN numbers; RN-style
    // string values (percentages, 'auto') pass through untouched.
    if (typeof value === 'number' && DIMENSIONS.has(key)) {
      css[key] = cssValue(value)
      continue
    }

    css[key] = value as string | number
  }

  return css
}

/** CSS properties whose bare numbers mean pixels. */
const DIMENSIONS = new Set([
  'width', 'height', 'minWidth', 'maxWidth', 'minHeight', 'maxHeight',
  'top', 'right', 'bottom', 'left',
  'marginTop', 'marginRight', 'marginBottom', 'marginLeft',
  'paddingTop', 'paddingRight', 'paddingBottom', 'paddingLeft',
  'gap', 'rowGap', 'columnGap',
  'borderWidth', 'borderTopWidth', 'borderRightWidth', 'borderBottomWidth', 'borderLeftWidth',
  'borderRadius', 'borderTopLeftRadius', 'borderTopRightRadius', 'borderBottomLeftRadius', 'borderBottomRightRadius',
  'fontSize', 'lineHeight', 'letterSpacing',
  'flexBasis', 'zIndex',
])
