/**
 * NeutronWind device-aware runtime resolver.
 *
 * This is the ONLY part of the styling package that may touch React Native,
 * and it does so lazily: the module is importable in Node (build tools,
 * SSR, tests) without an RN runtime — RN APIs are required inside the
 * resolver call, never at module load (NF-NR-02).
 *
 * The Babel plugin emits calls into this module for anything it cannot
 * resolve statically: dynamic class expressions, unknown tokens, screen-size
 * tokens and unpaired leading-* multipliers. The import is always explicit
 * and bound — the plugin never emits a free helper identifier (NF-NR-01).
 */

import { resolveStaticClasses, resolvePlatformClass, RUNTIME_ONLY_TOKENS, type Platform } from './resolve.js'

type StyleLike = Record<string, unknown>

/** Screen-size style values for the runtime-only tokens (NF-NR-03). */
function screenStyle(cls: string): StyleLike | null {
  let dimensions: { width: number; height: number } | null = null
  try {
    // Lazy require: absent in Node/build environments — that is fine, the
    // caller only reaches here when running inside an app.
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const { Dimensions } = require('react-native')
    const window = Dimensions.get('window')
    dimensions = { width: window.width, height: window.height }
  } catch {
    return null
  }
  switch (cls) {
    case 'w-screen': return { width: dimensions.width }
    case 'h-screen': return { height: dimensions.height }
    case 'max-w-screen': return { maxWidth: dimensions.width }
    case 'max-h-screen': return { maxHeight: dimensions.height }
    default: return null
  }
}

function detectPlatform(): Platform {
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const { Platform: RNPlatform } = require('react-native')
    return RNPlatform.OS === 'ios' ? 'ios' : RNPlatform.OS === 'android' ? 'android' : 'all'
  } catch {
    return 'all'
  }
}

/**
 * Resolve a class value on the device. Accepts the exact expression the
 * element carried (string, or any dynamic value evaluating to a string).
 * Non-string, non-null values pass through untouched — the caller may pipe
 * style objects through the same expression.
 */
export function resolveClassName(value: unknown, platform: Platform | undefined = undefined): StyleLike {
  if (typeof value !== 'string') return (value ?? {}) as StyleLike
  const effectivePlatform = platform ?? detectPlatform()

  const classes = value.trim().split(/\s+/).filter(Boolean)
  if (classes.length === 0) return {}

  const { styles } = resolveStaticClasses(value, effectivePlatform)
  let fontSize: number | undefined

  // Second pass for runtime-only tokens and a leading-* that had no static
  // text-* pair (an arbitrary text-[18px] size also pairs here).
  for (const cls of classes) {
    const { base, applies } = resolvePlatformClass(cls, effectivePlatform)
    if (!applies) continue
    if (RUNTIME_ONLY_TOKENS.has(base)) {
      Object.assign(styles, screenStyle(base) ?? {})
      continue
    }
    if (typeof styles.fontSize === 'number') fontSize = styles.fontSize
  }

  // Pair an unpaired leading-* against any font size we ended up with.
  const leading = classes
    .map(cls => resolvePlatformClass(cls, effectivePlatform).base)
    .find(base => base.startsWith('leading-'))
  if (leading && fontSize !== undefined && styles.lineHeight === undefined) {
    styles.lineHeight = fontSize * LEADING_FACTOR(leading)
  }

  return styles
}

function LEADING_FACTOR(cls: string): number {
  switch (cls) {
    case 'leading-none': return 1
    case 'leading-tight': return 1.25
    case 'leading-snug': return 1.375
    case 'leading-normal': return 1.5
    case 'leading-relaxed': return 1.625
    case 'leading-loose': return 2
    default: return 0
  }
}

/**
 * Merge NeutronWind-resolved styles with a caller-provided style prop.
 *
 * Precedence: the caller's style wins (the same order as the web, where an
 * inline style beats a class). Function-valued styles — Pressable's
 * `(state) => style` callback form — are preserved: the merge returns a
 * function that evaluates the callback per interaction state and flattens
 * the pair, with the caller's entry still winning (NF-NR-01).
 */
export function mergeStyles(
  resolved: unknown,
  existing: unknown,
): Array<unknown> | ((state: unknown) => Array<unknown>) {
  if (typeof existing === 'function') {
    return function mergedPressableStyle(state: unknown) {
      const fromCaller = existing(state)
      return flattenPair(resolved, fromCaller)
    }
  }
  return flattenPair(resolved, existing)
}

function flattenPair(resolved: unknown, existing: unknown): Array<unknown> {
  const pair: Array<unknown> = []
  if (resolved !== undefined && resolved !== null) pair.push(resolved)
  if (existing !== undefined && existing !== null) pair.push(existing)
  return pair
}
