/**
 * Node-safe class resolution shared by the Babel plugin and the runtime.
 *
 * NOTHING in this module may import react-native or touch device globals:
 * it executes inside build processes (Babel, Rspack, Node CI) where no RN
 * runtime exists. Dimension-dependent tokens (w-screen, h-screen, ...) are
 * deliberately absent here — they are runtime-only and resolved by
 * ./runtime.ts on the device (NF-NR-02/NF-NR-03).
 */

import { ALL_TOKENS, parseArbitraryValue, parseOpacityModifier } from './tokens.js'

export type Platform = 'ios' | 'android' | 'all'

export interface StaticResolution {
  /** Styles that could be resolved without a device. */
  styles: Record<string, unknown>
  /** True when at least one class needs the device-aware runtime resolver
   * (unknown token, screen-size token, or an unpaired leading-* multiplier). */
  needsRuntime: boolean
}

const PLATFORM_PREFIXES = ['ios', 'android'] as const

/** Tokens whose values depend on device dimensions — never static. */
export const RUNTIME_ONLY_TOKENS = new Set([
  'w-screen', 'h-screen', 'max-w-screen', 'max-h-screen',
])

/** CSS unitless line-height multipliers. On native, lineHeight must be an
 * absolute points value: multiplier × fontSize (NF-NR-03). Only meaningful
 * when the same class list pins the font size. */
export const LEADING_MULTIPLIERS: Record<string, number> = {
  'leading-none': 1,
  'leading-tight': 1.25,
  'leading-snug': 1.375,
  'leading-normal': 1.5,
  'leading-relaxed': 1.625,
  'leading-loose': 2,
}

/** Strip a platform prefix (ios:/android:) if present. */
export function resolvePlatformClass(cls: string, platform: Platform): { base: string; applies: boolean } {
  for (const prefix of PLATFORM_PREFIXES) {
    if (cls.startsWith(`${prefix}:`)) {
      const base = cls.slice(prefix.length + 1)
      return { base, applies: platform === prefix || platform === 'all' }
    }
  }
  return { base: cls, applies: true }
}

/** Resolve one bare token (no platform prefix) to a style object, or null. */
export function resolveToken(cls: string): Record<string, unknown> | null {
  return ALL_TOKENS[cls] ?? parseArbitraryValue(cls) ?? parseOpacityModifier(cls) ?? null
}

/**
 * Resolve a full space-separated class string for a known platform.
 *
 * `leading-*` multipliers pair with a `text-*` font size in the SAME string:
 * the pair is folded into an absolute `lineHeight` (native semantics). An
 * unpaired `leading-*` sets needsRuntime — the runtime resolver applies the
 * same rule against any text size present, or drops it when there is none.
 */
export function resolveStaticClasses(raw: string, platform: Platform): StaticResolution {
  const styles: Record<string, unknown> = {}
  let needsRuntime = false
  let fontSize: number | undefined
  let pendingLeading: number | undefined

  const classes = raw.trim().split(/\s+/).filter(Boolean)
  for (const cls of classes) {
    const { base, applies } = resolvePlatformClass(cls, platform)
    if (!applies) continue

    if (RUNTIME_ONLY_TOKENS.has(base)) {
      needsRuntime = true
      continue
    }

    if (base in LEADING_MULTIPLIERS) {
      pendingLeading = LEADING_MULTIPLIERS[base]
      continue
    }

    const token = resolveToken(base)
    if (!token) {
      // Unknown token: preserve deliberately via the runtime resolver,
      // never silently drop it (NF-NR-01).
      needsRuntime = true
      continue
    }
    Object.assign(styles, token)
    if (typeof token.fontSize === 'number') fontSize = token.fontSize
  }

  if (pendingLeading !== undefined) {
    if (fontSize !== undefined) {
      styles.lineHeight = Math.round(pendingLeading * fontSize)
    } else {
      // No font size in this string: the runtime may still pair it with a
      // text-[...] arbitrary size or a theme context. Mark for runtime.
      needsRuntime = true
    }
  }

  return { styles, needsRuntime }
}
