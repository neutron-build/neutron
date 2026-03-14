/**
 * NeutronWind Babel plugin.
 *
 * Transforms JSX className props into inline style objects at build time.
 * This runs during the Re.Pack/Rspack compilation step so there is
 * zero runtime cost — no className string parsing on device.
 *
 * Input:
 *   <View className="flex-1 bg-slate-900 p-4 ios:shadow-lg android:elevation-4" />
 *
 * Output (when platform === 'ios'):
 *   <View style={{"flex":1,"backgroundColor":"#0f172a","padding":16,"shadowColor":"#000",...}} />
 *
 * Platform variants:
 *   ios:shadow-lg       → only applied when bundling for iOS
 *   android:elevation-4 → only applied when bundling for Android
 *
 * Arbitrary values and opacity modifiers are also resolved at build time:
 *   w-[42px]           → { width: 42 }
 *   bg-blue-500/50     → { backgroundColor: 'rgba(59,130,246,0.5)' }
 *
 * If className contains dynamic values (template literals, variables),
 * the plugin falls back to a runtime resolveClassName() call.
 */

import type { NodePath, PluginObj } from '@babel/core'
import type * as BabelTypes from '@babel/types'
import { ALL_TOKENS, parseArbitraryValue, parseOpacityModifier } from './tokens.js'
import type { StyleProp } from './tokens.js'

type Platform = 'ios' | 'android' | 'all'

interface PluginOptions {
  /** Target platform — filters platform-specific class variants */
  platform?: Platform
}

interface BabelAPI {
  types: typeof BabelTypes
}

const PLATFORM_PREFIXES = ['ios', 'android'] as const

/**
 * Strip platform prefix from a class and check if it applies.
 * Returns { base, applies } — base is the token name, applies is whether
 * this class should be included for the given platform.
 */
function resolvePlatformClass(cls: string, platform: Platform): { base: string; applies: boolean } {
  for (const prefix of PLATFORM_PREFIXES) {
    if (cls.startsWith(`${prefix}:`)) {
      const base = cls.slice(prefix.length + 1)
      return { base, applies: platform === prefix || platform === 'all' }
    }
  }
  return { base: cls, applies: true }
}

/**
 * Resolve a single class token to a style object. Checks the static
 * token map first, then falls back to arbitrary value / opacity modifier parsing.
 */
function resolveToken(cls: string): StyleProp | null {
  return ALL_TOKENS[cls] ?? parseArbitraryValue(cls) ?? parseOpacityModifier(cls) ?? null
}

export default function neutronWindPlugin({ types: t }: BabelAPI, options: PluginOptions = {}): PluginObj {
  const platform = options.platform ?? 'all'

  return {
    name: 'neutron-wind',
    visitor: {
      JSXAttribute(path: NodePath<BabelTypes.JSXAttribute>) {
        // Only handle className attributes
        if (!t.isJSXIdentifier(path.node.name, { name: 'className' })) return

        const value = path.node.value
        if (!value) return

        // Static string literal — resolve at compile time
        if (t.isStringLiteral(value)) {
          const classes = value.value.trim().split(/\s+/)
          const merged: Record<string, unknown> = {}
          const unresolved: string[] = []

          for (const cls of classes) {
            const { base, applies } = resolvePlatformClass(cls, platform)
            if (!applies) continue  // wrong platform

            const token = resolveToken(base)
            if (token) {
              Object.assign(merged, token)
            } else {
              unresolved.push(base)
            }
          }

          if (unresolved.length === 0 && Object.keys(merged).length > 0) {
            // Fully resolved — emit static object
            path.node.name = t.jsxIdentifier('style')
            path.node.value = t.jsxExpressionContainer(
              _objectToASTExpression(merged, t)
            )
          } else if (Object.keys(merged).length > 0) {
            // Partial — warn and fall back to runtime for unresolved
            path.node.name = t.jsxIdentifier('style')
            path.node.value = t.jsxExpressionContainer(
              t.callExpression(
                t.memberExpression(t.identifier('__nw'), t.identifier('resolveClassName')),
                [t.stringLiteral(unresolved.join(' '))]
              )
            )
          }
          return
        }

        // JSX expression — resolve static template literals
        if (t.isJSXExpressionContainer(value)) {
          const expr = value.expression
          if (t.isTemplateLiteral(expr) && expr.quasis.every(q => q.type === 'TemplateElement')) {
            const raw = expr.quasis.map(q => q.value.cooked ?? '').join('')
            const classes = raw.trim().split(/\s+/)
            const merged: Record<string, unknown> = {}
            for (const cls of classes) {
              const { base, applies } = resolvePlatformClass(cls, platform)
              if (!applies) continue
              const token = resolveToken(base)
              if (token) Object.assign(merged, token)
            }
            if (Object.keys(merged).length > 0) {
              path.node.name = t.jsxIdentifier('style')
              path.node.value = t.jsxExpressionContainer(_objectToASTExpression(merged, t))
            }
          }
          // Dynamic expressions fall through unchanged
        }
      },
    },
  }
}

function _objectToASTExpression(
  obj: Record<string, unknown>,
  t: typeof BabelTypes,
): BabelTypes.ObjectExpression {
  const properties = Object.entries(obj).map(([key, value]) => {
    const val = _valueToAST(value, t)
    return t.objectProperty(t.stringLiteral(key), val)
  })
  return t.objectExpression(properties)
}

function _valueToAST(value: unknown, t: typeof BabelTypes): BabelTypes.Expression {
  if (typeof value === 'number') return t.numericLiteral(value)
  if (typeof value === 'string') return t.stringLiteral(value)
  if (typeof value === 'boolean') return t.booleanLiteral(value)
  if (value === null) return t.nullLiteral()
  if (typeof value === 'object' && !Array.isArray(value)) {
    return _objectToASTExpression(value as Record<string, unknown>, t)
  }
  return t.stringLiteral(String(value))
}

// Export token map for external tooling (IDE plugins, etc.)
export { ALL_TOKENS }
export type { StyleProp, Platform }
