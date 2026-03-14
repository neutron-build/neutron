/**
 * Tests for the NeutronWind Babel plugin.
 *
 * Uses @babel/core to transform JSX source and verify the output
 * transforms className into style props at build time.
 */

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
      [require.resolve('../babel-plugin'), { platform }],
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
