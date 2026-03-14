/**
 * Tests for the signal-based router navigator.
 */

describe('Router Navigator', () => {
  beforeEach(() => {
    jest.resetModules()
  })

  it('starts with initial state at /', () => {
    const nav = require('../navigator')
    expect(nav.routerState.value.pathname).toBe('/')
    expect(nav.routerState.value.segments).toEqual([])
    expect(nav.routerState.value.params).toEqual({})
  })

  it('navigate() updates pathname and segments', () => {
    const nav = require('../navigator')
    nav.navigate('/users/profile')
    expect(nav.routerState.value.pathname).toBe('/users/profile')
    expect(nav.routerState.value.segments).toEqual(['users', 'profile'])
  })

  it('navigate() with params sets route params', () => {
    const nav = require('../navigator')
    nav.navigate('/user/42', { params: { id: '42' } })
    expect(nav.routerState.value.params).toEqual({ id: '42' })
  })

  it('pathname computed signal reflects state', () => {
    const nav = require('../navigator')
    nav.navigate('/about')
    expect(nav.pathname.value).toBe('/about')
  })

  it('params computed signal reflects state', () => {
    const nav = require('../navigator')
    nav.navigate('/product/xyz', { params: { slug: 'xyz' } })
    expect(nav.params.value).toEqual({ slug: 'xyz' })
  })

  it('goBack() navigates to previous state', () => {
    const nav = require('../navigator')
    nav.navigate('/page1')
    nav.navigate('/page2')
    expect(nav.routerState.value.pathname).toBe('/page2')
    nav.goBack()
    expect(nav.routerState.value.pathname).toBe('/page1')
  })

  it('goBack() is a no-op when at the start of history', () => {
    const nav = require('../navigator')
    const initialPathname = nav.routerState.value.pathname
    nav.goBack()
    expect(nav.routerState.value.pathname).toBe(initialPathname)
  })

  it('canGoBack reflects history position', () => {
    const nav = require('../navigator')
    expect(nav.canGoBack.value).toBe(false)
    nav.navigate('/page1')
    expect(nav.canGoBack.value).toBe(true)
  })

  it('goForward() navigates forward in history', () => {
    const nav = require('../navigator')
    nav.navigate('/page1')
    nav.navigate('/page2')
    nav.goBack()
    expect(nav.routerState.value.pathname).toBe('/page1')
    nav.goForward()
    expect(nav.routerState.value.pathname).toBe('/page2')
  })

  it('goForward() is a no-op at end of history', () => {
    const nav = require('../navigator')
    nav.navigate('/end')
    nav.goForward()
    expect(nav.routerState.value.pathname).toBe('/end')
  })

  it('canGoForward reflects history position', () => {
    const nav = require('../navigator')
    nav.navigate('/a')
    nav.navigate('/b')
    expect(nav.canGoForward.value).toBe(false)
    nav.goBack()
    expect(nav.canGoForward.value).toBe(true)
  })

  it('replace() replaces current history entry', () => {
    const nav = require('../navigator')
    nav.navigate('/initial')
    nav.navigate('/temp')
    nav.replace('/replaced')
    expect(nav.routerState.value.pathname).toBe('/replaced')
    // Going back should go to /initial, not /temp
    nav.goBack()
    expect(nav.routerState.value.pathname).toBe('/initial')
  })

  it('navigate() truncates forward history when navigating from middle', () => {
    const nav = require('../navigator')
    nav.navigate('/a')
    nav.navigate('/b')
    nav.navigate('/c')
    nav.goBack()
    nav.goBack() // at /a
    nav.navigate('/d') // should truncate /b and /c
    expect(nav.canGoForward.value).toBe(false)
  })

  it('handleDeepLink() with full URL navigates by pathname', () => {
    const nav = require('../navigator')
    nav.handleDeepLink('myapp://example.com/deep/screen')
    expect(nav.routerState.value.pathname).toBe('/deep/screen')
  })

  it('handleDeepLink() with bare path navigates directly', () => {
    const nav = require('../navigator')
    nav.handleDeepLink('/bare/path')
    expect(nav.routerState.value.pathname).toBe('/bare/path')
  })

  it('navigate() strips leading slash from segments', () => {
    const nav = require('../navigator')
    nav.navigate('/foo/bar')
    expect(nav.routerState.value.segments).toEqual(['foo', 'bar'])
  })

  it('navigate() filters empty segments', () => {
    const nav = require('../navigator')
    nav.navigate('/foo//bar/')
    // Empty strings are filtered
    expect(nav.routerState.value.segments).toEqual(['foo', 'bar'])
  })

  it('setNavigationRef bridges to React Navigation on navigate', () => {
    const nav = require('../navigator')
    const mockNav = {
      navigate: jest.fn(),
      goBack: jest.fn(),
    }
    nav.setNavigationRef(mockNav)
    nav.navigate('/home')
    expect(mockNav.navigate).toHaveBeenCalledWith('home', {})
  })

  it('setNavigationRef bridges goBack to React Navigation', () => {
    const nav = require('../navigator')
    const mockNav = {
      navigate: jest.fn(),
      goBack: jest.fn(),
    }
    nav.setNavigationRef(mockNav)
    nav.navigate('/first')
    nav.navigate('/second')
    nav.goBack()
    expect(mockNav.goBack).toHaveBeenCalled()
  })
})
