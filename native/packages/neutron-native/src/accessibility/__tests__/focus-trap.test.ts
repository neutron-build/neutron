/**
 * NF-NR-17 regressions — Android FocusTrap confinement is real when
 * background roots are supplied (hidden while active, restored on release,
 * nested traps compose) and the no-roots case is documented as a hint only.
 */

import { useEffect } from 'react'
import { Platform } from 'react-native'
import { FocusTrap } from '../index'

export {}

// The RN mock defaults to iOS, where confinement is native; these tests
// exercise the ANDROID background-root path.
beforeEach(() => { (Platform as unknown as { OS: string }).OS = 'android' })
afterEach(() => { (Platform as unknown as { OS: string }).OS = 'ios' })

function makeRoot(initial?: string) {
  return { current: { importantForAccessibility: initial } }
}
function flushCleanups() {
  ;(useEffect as unknown as { __flushCleanups: () => void }).__flushCleanups()
}

describe('NF-NR-17 Android FocusTrap confinement', () => {
  it('hides background roots while active', () => {
    const rootA = makeRoot('yes')
    const rootB = makeRoot()
    const element = FocusTrap({ active: true, backgroundRoots: [rootA, rootB], children: null }) as unknown as { props: Record<string, unknown> }
    expect(element.props.importantForAccessibility).toBe('yes')
    expect((rootA.current as { importantForAccessibility?: string }).importantForAccessibility).toBe('no-hide-descendants')
    expect((rootB.current as { importantForAccessibility?: string }).importantForAccessibility).toBe('no-hide-descendants')
  })

  it('restores each root to ITS OWN prior value on release', () => {
    const rootA = makeRoot('yes')
    const rootB = makeRoot('auto')
    expect(FocusTrap({ active: true, backgroundRoots: [rootA, rootB], children: null })).toBeTruthy()
    flushCleanups()
    expect((rootA.current as { importantForAccessibility?: string }).importantForAccessibility).toBe('yes')
    expect((rootB.current as { importantForAccessibility?: string }).importantForAccessibility).toBe('auto')
  })

  it('nested traps compose: the inner trap never unhides the outer\'s roots', () => {
    const outerRoot = makeRoot('yes')
    const innerRoot = makeRoot('yes')
    // Outer trap activates first, hiding its root.
    expect(FocusTrap({ active: true, backgroundRoots: [outerRoot], children: null })).toBeTruthy()
    // Inner trap hides its own root too.
    expect(FocusTrap({ active: true, backgroundRoots: [innerRoot], children: null })).toBeTruthy()
    expect((innerRoot.current as { importantForAccessibility?: string }).importantForAccessibility).toBe('no-hide-descendants')
    // Inner trap releases (LIFO): only ITS root is restored.
    ;(useEffect as unknown as { __flushOneCleanup: () => void }).__flushOneCleanup()
    expect((innerRoot.current as { importantForAccessibility?: string }).importantForAccessibility).toBe('yes')
    expect((outerRoot.current as { importantForAccessibility?: string }).importantForAccessibility).toBe('no-hide-descendants')
    flushCleanups()
    expect((outerRoot.current as { importantForAccessibility?: string }).importantForAccessibility).toBe('yes')
  })

  it('without backgroundRoots the Android trap claims nothing beyond the hint', () => {
    const element = FocusTrap({ active: true, children: null }) as unknown as { props: Record<string, unknown> }
    expect(element.props.importantForAccessibility).toBe('yes')
    expect(element.props).not.toHaveProperty('accessibilityViewIsModal')
  })
})
