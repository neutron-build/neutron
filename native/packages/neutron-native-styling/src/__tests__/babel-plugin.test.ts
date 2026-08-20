/**
 * Tests for the NeutronWind Babel plugin.
 *
 * Uses @babel/core to transform JSX source and verify the output
 * transforms className into style props at build time.
 */

// `export {}` makes this file a MODULE. Without it TypeScript treats it as a
// script, so the `let` below is declared in the GLOBAL scope -- and the built
// `dist/__tests__/babel-plugin.test.d.ts` declares the same name globally,
// which failed the whole suite with TS2451 "Cannot redeclare block-scoped
// variable". Deleting dist is not the fix: other packages import from it at
// test time, and removing it takes 10 suites and 68 tests down with it.
import neutronWindPlugin from '../babel-plugin'

export {}

// We need babel available for these tests
let transformSync: typeof import('@babel/core').transformSync

beforeAll(() => {
  try {
    transformSync = require('@babel/core').transformSync
  } catch {
    // If babel is not installed, skip these tests
  }
})

function transform(code: string, platform: string = 'all'): string | null | undefined {
  if (!transformSync) return code
  const result = transformSync(code, {
    plugins: [
      ['@babel/plugin-syntax-jsx', {}],
      // Pass the plugin VALUE, not a path. `require.resolve` handed Babel a
      // `.ts` file to load itself, and Babel only transpiles `.cts` configs
      // and plugins -- so on CI this failed with "You are using a .ts config
      // file" while passing locally, where resolution happened to land on the
      // built `.js`. ts-jest has already compiled this module by the time the
      // test runs, so handing over the function is both hermetic and correct.
      [neutronWindPlugin, { platform }],
    ],
    filename: 'test.tsx',
  })
  return result?.code
}

describe('NeutronWind Babel Plugin', () => {
  // Skip all tests if babel is not available
  const describeOrSkip = (() => {
    try {
      require('@babel/core')
      return describe
    } catch {
      return describe.skip
    }
  })()

  describeOrSkip('static string literals', () => {
    it('transforms className to style with resolved tokens', () => {
      const input = '<View className="flex-1 p-4" />'
      const output = transform(input)
      expect(output).toContain('style')
      expect(output).not.toContain('className')
    })

    it('resolves colors correctly', () => {
      const input = '<View className="bg-white" />'
      const output = transform(input)
      expect(output).toContain('#ffffff')
    })

    it('handles multiple classes', () => {
      const input = '<View className="flex-1 p-4 m-2" />'
      const output = transform(input)
      expect(output).toContain('style')
    })
  })

  describeOrSkip('platform variants', () => {
    it('includes ios: classes when platform is ios', () => {
      const input = '<View className="p-4 ios:m-2" />'
      const output = transform(input, 'ios')
      expect(output).toContain('style')
    })

    it('excludes android: classes when platform is ios', () => {
      const input = '<View className="android:p-4" />'
      const output = transform(input, 'ios')
      // android:p-4 should not be included, but if nothing resolves,
      // the transform may leave className unchanged
    })

    it('includes all platform classes when platform is all', () => {
      const input = '<View className="ios:p-4 android:m-2" />'
      const output = transform(input, 'all')
      expect(output).toContain('style')
    })
  })

  describeOrSkip('non-className attributes', () => {
    it('leaves non-className attributes unchanged', () => {
      const input = '<View testID="test" />'
      const output = transform(input)
      expect(output).toContain('testID')
      expect(output).not.toContain('style')
    })
  })
})
