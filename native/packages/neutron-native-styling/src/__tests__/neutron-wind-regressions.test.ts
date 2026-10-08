/**
 * NF-NR-01/02/03 regression suite — NeutronWind dynamic/mixed classes,
 * build entry points, and native typography/screen token semantics.
 */

import { transformSync } from '@babel/core'
import neutronWindPlugin from '../babel-plugin'
import { resolveStaticClasses, LEADING_MULTIPLIERS, RUNTIME_ONLY_TOKENS } from '../resolve'
import { ALL_TOKENS, resolveClassName as staticResolve } from '../tokens'
import { resolveClassName as runtimeResolve, mergeStyles } from '../runtime'

export {}

function transform(code: string, platform = 'all'): string {
  const result = transformSync(code, {
    plugins: [
      [require('@babel/plugin-syntax-jsx'), {}],
      [neutronWindPlugin, { platform, runtimeModule: 'neutron-native-styling/runtime' }],
    ],
    filename: 'test.tsx',
    configFile: false,
    babelrc: false,
  })
  return result?.code ?? ''
}

function evaluateReturnedStyle(code: string): unknown {
  // Extract the generated import specifier + expression and run them.
  const importMatch = code.match(/import\s*\{([^}]+)\}\s*from\s*"neutron-native-styling\/runtime"/)
  const styleMatch = code.match(/style=\{(.+)\}/)
  expect(styleMatch).toBeTruthy()
  const runtimeStub = { resolveClassName: (v: unknown) => ({ __resolved: v }), mergeStyles: (a: unknown, b: unknown) => ({ __merged: [a, b] }) }
  const factory = new Function(
    '__runtime',
    `${importMatch ? `const ${importMatch[1].split(',').map(s => s.trim().split(' as ').pop()).join(', ')} = __runtime;` : ''}
     const __nw_eval = (${styleMatch![1]});
     return typeof __nw_eval === 'function' ? __nw_eval({ pressed: true }) : __nw_eval;`,
  )
  return factory(runtimeStub)
}

describe('NF-NR-01 mixed and dynamic classes', () => {
  it('a mixed static list keeps resolved styles and never emits a free helper identifier', () => {
    const out = transform('<View className="p-4 definitely-not-a-token" />')
    // No unbound __nw — the resolver arrives through a generated import.
    expect(out).not.toMatch(/[^.\w]__nw\b/)
    expect(out).toMatch(/import \{ resolveClassName as _neutronWindResolve\d* \}/)
    expect(out).toMatch(/_neutronWindResolve\d*\("p-4 definitely-not-a-token"\)/)
    // Evaluating through the REAL runtime keeps the padding.
    expect(runtimeResolve('p-4 definitely-not-a-token')).toMatchObject({ padding: 16 })
  })

  it('a template with expressions keeps every interpolated value and evaluates each once', () => {
    let evaluations = 0
    const sideEffect = () => { evaluations += 1; return 'mt-2' }
    const out = transform('<View className={`p-4 ${sideEffect()} ${cond ? "m-1" : "m-2"}`} />')
    // The WHOLE template is passed — quasis were never joined behind the
    // interpolations' backs.
    expect(out).toMatch(/_neutronWindResolve\d*\(`p-4 \$\{sideEffect\(\)\} \$\{cond \? "m-1" : "m-2"\}`\)/)
    // Evaluate the exact expression shape the assertion above verified the
    // plugin emitted: the FULL template, every interpolation intact.
    const value = new Function('resolveClassName', 'sideEffect', 'cond',
      'return resolveClassName(`p-4 ${sideEffect()} ${cond ? "m-1" : "m-2"}`)')(runtimeResolve, sideEffect, true)
    expect(evaluations).toBe(1)
    expect(value).toMatchObject({ padding: 16, marginTop: 8, margin: 4 })
  })

  it('zero-expression templates still fold statically', () => {
    const out = transform('<View className={`p-4 m-2`} />')
    expect(out).toContain('padding')
    expect(out).not.toContain('resolveClassName')
  })

  it('merges with an existing style attribute in both orders, caller style winning', () => {
    const styleFirst = transform('<View style={{ padding: 32 }} className="p-4" />')
    const classFirst = transform('<View className="p-4" style={{ padding: 32 }} />')
    for (const out of [styleFirst, classFirst]) {
      expect(out).toMatch(/style=\{_neutronWindMerge\d*\(/)
      expect((out.match(/style=/g) ?? []).length).toBe(1) // never duplicated
    }
    // Runtime merge precedence: caller's 32 beats the class's 16.
    const arrayForm = mergeStyles({ padding: 16 }, { padding: 32 })
    expect(StyleSheetFlatten(arrayForm)).toEqual({ padding: 32 })
  })

  it('preserves function-valued Pressable styles through the merge', () => {
    const out = transform('<Pressable className="p-4" style={(state) => ({ opacity: state.pressed ? 0.5 : 1 })} />')
    expect(out).toMatch(/_neutronWindMerge\d*\(/)
    const fnForm = mergeStyles({ padding: 16 }, (state: { pressed: boolean }) => ({ opacity: state.pressed ? 0.5 : 1 })) as (state: unknown) => unknown[]
    expect(typeof fnForm).toBe('function')
    const flattened = StyleSheetFlatten(fnForm({ pressed: true }))
    expect(flattened).toEqual({ padding: 16, opacity: 0.5 })
  })

  it('unknown-only and whitespace class strings have an explicit contract', () => {
    // Whitespace-only: no rewrite at all — the attribute stays className.
    expect(transform('<View className="   " />')).toContain('className')
    // Unknown-only: runtime resolver call (the class survives, not dropped).
    const out = transform('<View className="totally-unknown" />')
    expect(out).toMatch(/_neutronWindResolve\d*\("totally-unknown"\)/)
  })
})

function StyleSheetFlatten(style: unknown): Record<string, unknown> {
  if (typeof style === 'function') throw new Error('unexpected function style')
  const array = Array.isArray(style) ? style : [style]
  return Object.assign({}, ...array.filter(Boolean))
}

describe('NF-NR-03 native typography and screen-size semantics', () => {
  it('leading-* pairs with a text size into an absolute lineHeight', () => {
    const out = transform('<Text className="text-base leading-tight">x</Text>')
    expect(out).toContain('lineHeight')
    // text-base = fontSize 16; leading-tight = ×1.25 → 20 on native.
    expect(resolveStaticClasses('text-base leading-tight', 'all').styles).toEqual({ fontSize: 16, lineHeight: 20 })
  })

  it('an unpaired leading-* is deferred to the runtime, never a bare multiplier', () => {
    const out = transform('<Text className="leading-loose">x</Text>')
    expect(out).not.toContain('lineHeight')
    expect(out).toMatch(/_neutronWindResolve\d*\("leading-loose"\)/)
    expect(LEADING_MULTIPLIERS['leading-loose']).toBe(2)
  })

  it('screen-size tokens are runtime-only and absent from the static map', () => {
    expect(ALL_TOKENS['w-screen']).toBeUndefined()
    expect(ALL_TOKENS['h-screen']).toBeUndefined()
    expect(ALL_TOKENS['max-w-screen']).toBeUndefined()
    expect([...RUNTIME_ONLY_TOKENS]).toEqual(expect.arrayContaining(['w-screen', 'h-screen']))
    const out = transform('<View className="w-screen" />')
    expect(out).toMatch(/_neutronWindResolve\d*\("w-screen"\)/)
  })

  it('numberOfLines is not a style token', () => {
    expect(ALL_TOKENS['text-ellipsis']).toBeUndefined()
  })

  it('the runtime resolver resolves screen tokens against device dimensions', () => {
    // jest's react-native mock: 375×812, Platform.OS ios.
    expect(runtimeResolve('w-screen h-screen')).toEqual({ width: 375, height: 812 })
  })

  it('the runtime resolver applies platform variants for the running OS', () => {
    expect(runtimeResolve('ios:p-4 android:m-2')).toEqual({ padding: 16 })
    expect(runtimeResolve('ios:p-4 android:m-2', 'android')).toEqual({ margin: 8 })
  })

  it('static resolveClassName stays usable in Node and unknown tokens are skipped', () => {
    expect(staticResolve('p-4 nope')).toEqual({ padding: 16 })
  })
})

describe('NF-NR-02 build entry points', () => {
  it('the loader transforms TSX with type annotations through the real pipeline', async () => {
    const loader = (await import('../rspack-loader')).default
    const source = 'export const Card = (props: { title: string }) => <View className="p-4">{props.title}</View>'
    let result: string | undefined
    loader.call(
      { resourcePath: 'Card.tsx', getOptions: () => ({}), callback: (_e: Error | null, r?: string) => { result = r } } as never,
      source,
    )
    expect(result).toBeTruthy()
    expect(result).toContain('padding')
    expect(result).not.toContain('className="p-4"')
    // Type annotations must survive for the downstream lowering stage
    // (Babel prints object types with its own spacing/semicolons).
    expect(result).toContain('title: string')
  })

  it('the loader leaves .jsx inputs untouched by the TS plugin path', async () => {
    const loader = (await import('../rspack-loader')).default
    const source = 'export const Card = (props) => <View className="p-4">{props.title}</View>'
    let result: string | undefined
    loader.call(
      { resourcePath: 'Card.jsx', getOptions: () => ({}), callback: (_e: Error | null, r?: string) => { result = r } } as never,
      source,
    )
    expect(result).toContain('padding')
  })

  it('importing the token table reads no device dimensions in Node', () => {
    // tokens.ts imports nothing from react-native anymore; this test file
    // importing it in a Node jest environment is itself the assertion.
    expect(ALL_TOKENS['p-4']).toEqual({ padding: 16 })
    expect(() => require('../tokens')).not.toThrow()
  })
})
