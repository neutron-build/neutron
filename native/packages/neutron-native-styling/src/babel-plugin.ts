/**
 * NeutronWind Babel plugin.
 *
 * Transforms JSX className props into inline style objects at build time.
 * This runs during the Re.Pack/Rspack compilation step so static classes
 * cost nothing at runtime — no className string parsing on device.
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
 * Dynamic values (template literals with expressions, identifiers, ternaries)
 * and anything not statically resolvable (unknown tokens, screen sizes,
 * unpaired leading-*) are routed through ONE explicitly imported
 * `resolveClassName` call from 'neutron-native-styling/runtime' — never a
 * free/unbound helper identifier (NF-NR-01). The full original expression is
 * passed to the resolver so every interpolated expression still evaluates
 * exactly once. An existing style prop on the same element is merged (caller
 * style wins; function-valued Pressable styles preserved) instead of being
 * duplicated.
 */

import type { NodePath, PluginObj } from '@babel/core'
import type * as BabelTypes from '@babel/types'
import { resolveStaticClasses } from './resolve.js'
import { ALL_TOKENS } from './tokens.js'

export type Platform = 'ios' | 'android' | 'all'

interface PluginOptions {
  /** Target platform — filters platform-specific class variants */
  platform?: Platform
  /** Module specifier for the runtime resolver (tests/overrides). */
  runtimeModule?: string
}

interface BabelAPI {
  types: typeof BabelTypes
  /**
   * Babel ≥7.17 hands plugins an `availableHelper`-adjacent scope API; the
   * loader passes the real one. We only need generateUidIdentifier, which
   * plain @babel/traverse scopes provide.
   */
}

const RUNTIME_MODULE = 'neutron-native-styling/runtime'

export default function neutronWindPlugin({ types: t }: BabelAPI, options: PluginOptions = {}): PluginObj {
  const platform = options.platform ?? 'all'
  const runtimeModule = options.runtimeModule ?? RUNTIME_MODULE

  // Per-program import bookkeeping: bindings are generated lazily on first
  // use so a file with no dynamic classes never gets an unused import.
  let resolveBinding: BabelTypes.Identifier | null = null
  let mergeBinding: BabelTypes.Identifier | null = null
  let programPath: NodePath<BabelTypes.Program> | null = null

  function ensureResolveImport(): BabelTypes.Identifier {
    if (!resolveBinding) {
      if (!programPath) throw new Error('neutron-wind: program not registered')
      resolveBinding = programPath.scope.generateUidIdentifier('neutronWindResolve')
      programPath.node.body.unshift(
        t.importDeclaration(
          [t.importSpecifier(resolveBinding, t.identifier('resolveClassName'))],
          t.stringLiteral(runtimeModule),
        ),
      )
      programPath.scope.registerDeclaration(programPath.get('body.0') as NodePath<BabelTypes.ImportDeclaration>)
    }
    return resolveBinding
  }

  function ensureMergeImport(): BabelTypes.Identifier {
    if (!mergeBinding) {
      if (!programPath) throw new Error('neutron-wind: program not registered')
      mergeBinding = programPath.scope.generateUidIdentifier('neutronWindMerge')
      programPath.node.body.unshift(
        t.importDeclaration(
          [t.importSpecifier(mergeBinding, t.identifier('mergeStyles'))],
          t.stringLiteral(runtimeModule),
        ),
      )
      programPath.scope.registerDeclaration(programPath.get('body.0') as NodePath<BabelTypes.ImportDeclaration>)
    }
    return mergeBinding
  }

  return {
    name: 'neutron-wind',
    visitor: {
      Program(path: NodePath<BabelTypes.Program>) {
        programPath = path
      },
      JSXAttribute(path: NodePath<BabelTypes.JSXAttribute>) {
        if (!t.isJSXIdentifier(path.node.name, { name: 'className' })) return
        const value = path.node.value
        if (!value) return

        const element = path.parentPath
        if (!element || !t.isJSXOpeningElement(element.node)) return

        // Existing style attribute (either side of className) — merged, never
        // duplicated.
        const styleAttr = element.node.attributes.find(
          attr => t.isJSXAttribute(attr) && t.isJSXIdentifier(attr.name, { name: 'style' }),
        ) as BabelTypes.JSXAttribute | undefined

        let resolvedExpr: BabelTypes.Expression | null = null

        if (t.isStringLiteral(value)) {
          resolvedExpr = resolveStaticString(value.value)
        } else if (t.isJSXExpressionContainer(value)) {
          const expr = value.expression
          if (t.isTemplateLiteral(expr)) {
            // Only fold templates with ZERO expressions: joining the quasis
            // of a template that interpolates anything silently discards the
            // interpolated values (NF-NR-01).
            if (expr.expressions.length === 0) {
              const raw = expr.quasis.map(q => q.value.cooked ?? '').join('')
              resolvedExpr = resolveStaticString(raw)
            } else {
              resolvedExpr = callResolver(expr)
            }
          } else if (t.isExpression(expr)) {
            // Any other dynamic expression: one resolver call over the full
            // original expression — evaluation count preserved.
            resolvedExpr = callResolver(expr)
          }
        }

        if (!resolvedExpr) return

        const finalExpr = styleAttr
          ? t.callExpression(ensureMergeImport(), [resolvedExpr, unwrapStyleValue(styleAttr)])
          : resolvedExpr

        if (styleAttr) {
          // Rewrite the existing style attribute; drop className entirely.
          styleAttr.value = t.jsxExpressionContainer(finalExpr)
          path.remove()
        } else {
          path.node.name = t.jsxIdentifier('style')
          path.node.value = t.jsxExpressionContainer(finalExpr)
        }
        return

        function callResolver(arg: BabelTypes.Expression): BabelTypes.Expression {
          return t.callExpression(ensureResolveImport(), [arg])
        }

        function resolveStaticString(raw: string): BabelTypes.Expression | null {
          const { styles, needsRuntime } = resolveStaticClasses(raw, platform)
          const staticEntries = Object.keys(styles).length
          if (!needsRuntime && staticEntries > 0) {
            return _objectToASTExpression(styles, t)
          }
          if (needsRuntime) {
            // Unknown/screen/unpaired tokens: hand the FULL original string
            // to the runtime resolver (it keeps platform handling and merges
            // everything, including what we resolved statically here).
            return callResolver(t.stringLiteral(raw))
          }
          // Whitespace-only or empty: no style at all. Leave the attribute
          // alone — an explicit, boring contract (NF-NR-01).
          return null
        }

        function unwrapStyleValue(attr: BabelTypes.JSXAttribute): BabelTypes.Expression {
          const v = attr.value
          if (!v) return t.nullLiteral()
          if (t.isStringLiteral(v)) return v
          if (t.isJSXExpressionContainer(v) && t.isExpression(v.expression)) return v.expression
          return t.nullLiteral()
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
export type { StyleProp } from './tokens.js'
