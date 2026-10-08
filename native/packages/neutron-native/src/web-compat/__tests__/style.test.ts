/**
 * NF-NR-13 regressions — the RN→CSS style bridge: axis shorthands expand,
 * transforms convert, numeric lengths gain units, native-only values are
 * dropped with a warning instead of leaking into CSS, and arrays flatten
 * with RN's later-wins order.
 */

import { styleToCSS } from '../style'

export {}

describe('NF-NR-13 web style bridge', () => {
  it('expands axis shorthands to their physical CSS pairs', () => {
    expect(styleToCSS({ paddingHorizontal: 8, marginVertical: 4 } as never)).toEqual({
      paddingLeft: '8px', paddingRight: '8px', marginTop: '4px', marginBottom: '4px',
    })
  })

  it('converts transform arrays to a CSS transform string', () => {
    const css = styleToCSS({ transform: [{ translateX: 10 }, { scale: 2 }] } as never)
    expect(css.transform).toBe('translateX(10px) scale(2)')
    const rot = styleToCSS({ transform: [{ rotate: 45 }] } as never)
    expect(rot.transform).toBe('rotate(45deg)')
  })

  it('bare numbers on dimensional properties become px', () => {
    expect(styleToCSS({ width: 42, fontSize: 14 } as never)).toEqual({ width: '42px', fontSize: '14px' })
    expect(styleToCSS({ width: '50%' } as never)).toEqual({ width: '50%' })
  })

  it('native-only properties are dropped with a warning, never leaked', () => {
    const warn = jest.spyOn(console, 'warn').mockImplementation(() => {})
    const css = styleToCSS({ shadowColor: '#000', shadowRadius: 4 } as never)
    expect(css).toEqual({})
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('shadowColor'))
    warn.mockRestore()
  })

  it('arrays flatten element-wise with later entries winning', () => {
    expect(styleToCSS([{ width: 10 }, [{ width: 20, height: 5 }]] as never)).toEqual({ width: '20px', height: '5px' })
  })

  it('elevation approximates as a box-shadow', () => {
    expect(styleToCSS({ elevation: 4 } as never).boxShadow).toContain('4px')
  })
})
