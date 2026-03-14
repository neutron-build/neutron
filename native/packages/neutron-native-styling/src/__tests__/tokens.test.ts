/**
 * Tests for NeutronWind design tokens — resolveClassName, parseArbitraryValue,
 * parseOpacityModifier, hexToRgba, token maps.
 */

import {
  resolveClassName,
  clearClassNameCache,
  parseArbitraryValue,
  parseOpacityModifier,
  hexToRgba,
  ALL_TOKENS,
  FLEX_TOKENS,
  TEXT_TOKENS,
  COLOR_TOKENS,
  BORDER_TOKENS,
  LAYOUT_TOKENS,
  SIZE_TOKENS,
  MISC_TOKENS,
  PADDING_TOKENS,
  MARGIN_TOKENS,
} from '../tokens'

describe('Token maps', () => {
  it('PADDING_TOKENS contains expected keys', () => {
    expect(PADDING_TOKENS['p-4']).toEqual({ padding: 16 })
    expect(PADDING_TOKENS['px-2']).toEqual({ paddingHorizontal: 8 })
    expect(PADDING_TOKENS['py-1']).toEqual({ paddingVertical: 4 })
    expect(PADDING_TOKENS['pt-3']).toEqual({ paddingTop: 12 })
    expect(PADDING_TOKENS['pb-0']).toEqual({ paddingBottom: 0 })
    expect(PADDING_TOKENS['pl-0.5']).toEqual({ paddingLeft: 2 })
    expect(PADDING_TOKENS['pr-1.5']).toEqual({ paddingRight: 6 })
  })

  it('MARGIN_TOKENS contains expected keys', () => {
    expect(MARGIN_TOKENS['m-4']).toEqual({ margin: 16 })
    expect(MARGIN_TOKENS['mx-2']).toEqual({ marginHorizontal: 8 })
    expect(MARGIN_TOKENS['my-1']).toEqual({ marginVertical: 4 })
    expect(MARGIN_TOKENS['mt-3']).toEqual({ marginTop: 12 })
    expect(MARGIN_TOKENS['mb-0']).toEqual({ marginBottom: 0 })
  })

  it('FLEX_TOKENS contains flex utilities', () => {
    expect(FLEX_TOKENS['flex-1']).toEqual({ flex: 1 })
    expect(FLEX_TOKENS['flex-row']).toEqual({ flexDirection: 'row' })
    expect(FLEX_TOKENS['flex-col']).toEqual({ flexDirection: 'column' })
    expect(FLEX_TOKENS['flex-wrap']).toEqual({ flexWrap: 'wrap' })
    expect(FLEX_TOKENS['flex-none']).toEqual({ flex: 0 })
    expect(FLEX_TOKENS['items-center']).toEqual({ alignItems: 'center' })
    expect(FLEX_TOKENS['justify-between']).toEqual({ justifyContent: 'space-between' })
    expect(FLEX_TOKENS['self-stretch']).toEqual({ alignSelf: 'stretch' })
  })

  it('SIZE_TOKENS contains width and height', () => {
    expect(SIZE_TOKENS['w-4']).toEqual({ width: 16 })
    expect(SIZE_TOKENS['h-4']).toEqual({ height: 16 })
    expect(SIZE_TOKENS['aspect-square']).toEqual({ aspectRatio: 1 })
    expect(SIZE_TOKENS['aspect-video']).toEqual({ aspectRatio: 16 / 9 })
  })

  it('LAYOUT_TOKENS contains gap, z-index, position', () => {
    expect(LAYOUT_TOKENS['gap-4']).toEqual({ gap: 16 })
    expect(LAYOUT_TOKENS['z-10']).toEqual({ zIndex: 10 })
    expect(LAYOUT_TOKENS['absolute']).toEqual({ position: 'absolute' })
    expect(LAYOUT_TOKENS['relative']).toEqual({ position: 'relative' })
    expect(LAYOUT_TOKENS['inset-0']).toEqual({ top: 0, right: 0, bottom: 0, left: 0 })
  })

  it('TEXT_TOKENS contains font sizes and weights', () => {
    expect(TEXT_TOKENS['text-base']).toEqual({ fontSize: 16, lineHeight: 24 })
    expect(TEXT_TOKENS['text-xl']).toEqual({ fontSize: 20, lineHeight: 28 })
    expect(TEXT_TOKENS['font-bold']).toEqual({ fontWeight: '700' })
    expect(TEXT_TOKENS['italic']).toEqual({ fontStyle: 'italic' })
    expect(TEXT_TOKENS['text-center']).toEqual({ textAlign: 'center' })
    expect(TEXT_TOKENS['uppercase']).toEqual({ textTransform: 'uppercase' })
    expect(TEXT_TOKENS['underline']).toEqual({ textDecorationLine: 'underline' })
  })

  it('COLOR_TOKENS contains background, text, and border colors', () => {
    expect(COLOR_TOKENS['bg-white']).toEqual({ backgroundColor: '#ffffff' })
    expect(COLOR_TOKENS['bg-black']).toEqual({ backgroundColor: '#000000' })
    expect(COLOR_TOKENS['text-white']).toEqual({ color: '#ffffff' })
    expect(COLOR_TOKENS['text-red-500']).toEqual({ color: '#ef4444' })
    expect(COLOR_TOKENS['border-blue-500']).toEqual({ borderColor: '#3b82f6' })
    expect(COLOR_TOKENS['bg-slate-900']).toEqual({ backgroundColor: '#0f172a' })
  })

  it('BORDER_TOKENS contains border radius and width', () => {
    expect(BORDER_TOKENS['rounded']).toEqual({ borderRadius: 4 })
    expect(BORDER_TOKENS['rounded-full']).toEqual({ borderRadius: 9999 })
    expect(BORDER_TOKENS['rounded-none']).toEqual({ borderRadius: 0 })
    expect(BORDER_TOKENS['border']).toEqual({ borderWidth: 1 })
    expect(BORDER_TOKENS['border-2']).toEqual({ borderWidth: 2 })
  })

  it('MISC_TOKENS contains opacity and overflow', () => {
    expect(MISC_TOKENS['opacity-0']).toEqual({ opacity: 0 })
    expect(MISC_TOKENS['opacity-50']).toEqual({ opacity: 0.5 })
    expect(MISC_TOKENS['opacity-100']).toEqual({ opacity: 1 })
    expect(MISC_TOKENS['overflow-hidden']).toEqual({ overflow: 'hidden' })
    expect(MISC_TOKENS['overflow-visible']).toEqual({ overflow: 'visible' })
  })

  it('ALL_TOKENS merges all individual token maps', () => {
    // Spot-check that ALL_TOKENS contains entries from each map
    expect(ALL_TOKENS['p-4']).toBeDefined()
    expect(ALL_TOKENS['m-4']).toBeDefined()
    expect(ALL_TOKENS['flex-1']).toBeDefined()
    expect(ALL_TOKENS['w-4']).toBeDefined()
    expect(ALL_TOKENS['gap-4']).toBeDefined()
    expect(ALL_TOKENS['text-base']).toBeDefined()
    expect(ALL_TOKENS['bg-white']).toBeDefined()
    expect(ALL_TOKENS['rounded']).toBeDefined()
    expect(ALL_TOKENS['opacity-50']).toBeDefined()
  })
})

describe('parseArbitraryValue', () => {
  it('parses pixel values: w-[42px]', () => {
    const result = parseArbitraryValue('w-[42px]')
    expect(result).toEqual({ width: 42 })
  })

  it('parses pixel values: h-[100px]', () => {
    const result = parseArbitraryValue('h-[100px]')
    expect(result).toEqual({ height: 100 })
  })

  it('parses percentage values: w-[50%]', () => {
    const result = parseArbitraryValue('w-[50%]')
    expect(result).toEqual({ width: '50%' })
  })

  it('parses plain numbers: p-[12]', () => {
    const result = parseArbitraryValue('p-[12]')
    expect(result).toEqual({ padding: 12 })
  })

  it('parses hex color: bg-[#ff0000]', () => {
    const result = parseArbitraryValue('bg-[#ff0000]')
    expect(result).toEqual({ backgroundColor: '#ff0000' })
  })

  it('parses text color: text-[#333]', () => {
    const result = parseArbitraryValue('text-[#333]')
    expect(result).toEqual({ color: '#333' })
  })

  it('parses text as fontSize when not a color: text-[18px]', () => {
    const result = parseArbitraryValue('text-[18px]')
    expect(result).toEqual({ fontSize: 18 })
  })

  it('parses rgb color: text-[rgb(255,0,0)]', () => {
    const result = parseArbitraryValue('text-[rgb(255,0,0)]')
    expect(result).toEqual({ color: 'rgb(255,0,0)' })
  })

  it('parses margin arbitrary: m-[20px]', () => {
    const result = parseArbitraryValue('m-[20px]')
    expect(result).toEqual({ margin: 20 })
  })

  it('parses margin directions: mx-[10px]', () => {
    expect(parseArbitraryValue('mx-[10px]')).toEqual({ marginHorizontal: 10 })
    expect(parseArbitraryValue('my-[5px]')).toEqual({ marginVertical: 5 })
    expect(parseArbitraryValue('mt-[8px]')).toEqual({ marginTop: 8 })
    expect(parseArbitraryValue('mb-[4px]')).toEqual({ marginBottom: 4 })
    expect(parseArbitraryValue('ml-[6px]')).toEqual({ marginLeft: 6 })
    expect(parseArbitraryValue('mr-[2px]')).toEqual({ marginRight: 2 })
  })

  it('parses padding directions: px-[10px]', () => {
    expect(parseArbitraryValue('px-[10px]')).toEqual({ paddingHorizontal: 10 })
    expect(parseArbitraryValue('py-[5px]')).toEqual({ paddingVertical: 5 })
    expect(parseArbitraryValue('pt-[8px]')).toEqual({ paddingTop: 8 })
  })

  it('parses position offsets: top-[10px]', () => {
    expect(parseArbitraryValue('top-[10px]')).toEqual({ top: 10 })
    expect(parseArbitraryValue('right-[5]')).toEqual({ right: 5 })
    expect(parseArbitraryValue('bottom-[20px]')).toEqual({ bottom: 20 })
    expect(parseArbitraryValue('left-[15px]')).toEqual({ left: 15 })
  })

  it('parses rounded: rounded-[12px]', () => {
    expect(parseArbitraryValue('rounded-[12px]')).toEqual({ borderRadius: 12 })
  })

  it('parses gap: gap-[8px]', () => {
    expect(parseArbitraryValue('gap-[8px]')).toEqual({ gap: 8 })
  })

  it('parses z-index: z-[99]', () => {
    expect(parseArbitraryValue('z-[99]')).toEqual({ zIndex: 99 })
  })

  it('parses border: border-[3]', () => {
    expect(parseArbitraryValue('border-[3]')).toEqual({ borderWidth: 3 })
  })

  it('returns null for non-matching classes', () => {
    expect(parseArbitraryValue('invalid-class')).toBeNull()
    expect(parseArbitraryValue('flex-1')).toBeNull()
    expect(parseArbitraryValue('')).toBeNull()
  })

  it('returns null for unknown prefixes', () => {
    expect(parseArbitraryValue('foo-[10px]')).toBeNull()
  })

  it('handles negative numbers', () => {
    expect(parseArbitraryValue('m-[-10]')).toEqual({ margin: -10 })
  })

  it('handles decimal numbers', () => {
    expect(parseArbitraryValue('w-[1.5]')).toEqual({ width: 1.5 })
  })
})

describe('hexToRgba', () => {
  it('converts 6-digit hex to rgba', () => {
    expect(hexToRgba('#ff0000', 50)).toBe('rgba(255,0,0,0.5)')
    expect(hexToRgba('#000000', 100)).toBe('rgba(0,0,0,1)')
    expect(hexToRgba('#ffffff', 0)).toBe('rgba(255,255,255,0)')
  })

  it('converts 3-digit hex to rgba', () => {
    expect(hexToRgba('#f00', 50)).toBe('rgba(255,0,0,0.5)')
    expect(hexToRgba('#fff', 80)).toBe('rgba(255,255,255,0.8)')
  })

  it('handles hex without # prefix', () => {
    // The function strips #, so passing without # should also work
    expect(hexToRgba('ff0000', 75)).toBe('rgba(255,0,0,0.75)')
  })

  it('handles 0 opacity', () => {
    expect(hexToRgba('#3b82f6', 0)).toBe('rgba(59,130,246,0)')
  })

  it('handles 100 opacity', () => {
    expect(hexToRgba('#3b82f6', 100)).toBe('rgba(59,130,246,1)')
  })
})

describe('parseOpacityModifier', () => {
  it('parses bg-blue-500/50 to rgba', () => {
    const result = parseOpacityModifier('bg-blue-500/50')
    expect(result).not.toBeNull()
    expect(result!.backgroundColor).toContain('rgba')
    expect(result!.backgroundColor).toContain('0.5')
  })

  it('parses text-white/80 to rgba', () => {
    const result = parseOpacityModifier('text-white/80')
    expect(result).not.toBeNull()
    expect(result!.color).toContain('rgba')
    expect(result!.color).toContain('0.8')
  })

  it('parses border-red-500/25', () => {
    const result = parseOpacityModifier('border-red-500/25')
    expect(result).not.toBeNull()
    expect(result!.borderColor).toContain('rgba')
    expect(result!.borderColor).toContain('0.25')
  })

  it('returns null for non-slash classes', () => {
    expect(parseOpacityModifier('bg-blue-500')).toBeNull()
    expect(parseOpacityModifier('flex-1')).toBeNull()
  })

  it('returns null for invalid opacity values', () => {
    expect(parseOpacityModifier('bg-blue-500/abc')).toBeNull()
    expect(parseOpacityModifier('bg-blue-500/-10')).toBeNull()
    expect(parseOpacityModifier('bg-blue-500/200')).toBeNull()
  })

  it('returns null when base class has no color', () => {
    expect(parseOpacityModifier('flex-1/50')).toBeNull()
    expect(parseOpacityModifier('p-4/50')).toBeNull()
  })

  it('handles 0 opacity', () => {
    const result = parseOpacityModifier('bg-blue-500/0')
    expect(result).not.toBeNull()
    expect(result!.backgroundColor).toContain('0)')
  })

  it('handles 100 opacity', () => {
    const result = parseOpacityModifier('bg-blue-500/100')
    expect(result).not.toBeNull()
    expect(result!.backgroundColor).toContain('1)')
  })
})

describe('resolveClassName', () => {
  beforeEach(() => {
    clearClassNameCache()
  })

  it('resolves a single token', () => {
    const result = resolveClassName('flex-1')
    expect(result).toEqual({ flex: 1 })
  })

  it('merges multiple tokens', () => {
    const result = resolveClassName('flex-1 p-4 bg-white')
    expect(result).toMatchObject({
      flex: 1,
      padding: 16,
      backgroundColor: '#ffffff',
    })
  })

  it('returns frozen object', () => {
    const result = resolveClassName('flex-1')
    expect(Object.isFrozen(result)).toBe(true)
  })

  it('caches results', () => {
    const result1 = resolveClassName('flex-1 p-4')
    const result2 = resolveClassName('flex-1 p-4')
    expect(result1).toBe(result2) // same reference from cache
  })

  it('handles arbitrary values in className', () => {
    const result = resolveClassName('w-[100px] h-[50px]')
    expect(result).toMatchObject({ width: 100, height: 50 })
  })

  it('handles opacity modifiers in className', () => {
    const result = resolveClassName('bg-blue-500/50')
    expect(result.backgroundColor).toContain('rgba')
  })

  it('handles empty string', () => {
    const result = resolveClassName('')
    expect(result).toEqual({})
  })

  it('handles extra whitespace', () => {
    const result = resolveClassName('  flex-1   p-4  ')
    expect(result).toMatchObject({ flex: 1, padding: 16 })
  })

  it('ignores unknown classes', () => {
    const result = resolveClassName('flex-1 unknown-class p-4')
    expect(result).toMatchObject({ flex: 1, padding: 16 })
    expect(Object.keys(result)).toHaveLength(2)
  })

  it('later tokens override earlier ones', () => {
    const result = resolveClassName('p-4 p-8')
    expect(result).toMatchObject({ padding: 32 })
  })
})

describe('clearClassNameCache', () => {
  it('forces re-resolution after clearing', () => {
    const result1 = resolveClassName('flex-1')
    clearClassNameCache()
    const result2 = resolveClassName('flex-1')
    // Same value but different reference (re-resolved)
    expect(result1).toEqual(result2)
    expect(result1).not.toBe(result2)
  })
})
