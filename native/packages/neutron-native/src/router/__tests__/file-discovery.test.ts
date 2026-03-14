/**
 * Tests for file-based route discovery — buildRouteTree and matchRoute.
 */

import { buildRouteTree, matchRoute } from '../file-discovery'
import type { RouteManifest } from '../file-discovery'

function DummyComponent() { return null }
function AboutComponent() { return null }
function UserComponent() { return null }
function IndexComponent() { return null }

describe('buildRouteTree', () => {
  it('builds a flat tree from simple manifest entries', () => {
    const manifest: RouteManifest = {
      entries: [
        { path: 'index', segment: 'index', dynamic: false, group: false, layout: false, notFound: false },
        { path: 'about', segment: 'about', dynamic: false, group: false, layout: false, notFound: false },
      ],
    }
    const components = {
      'index': IndexComponent,
      'about': AboutComponent,
    }
    const tree = buildRouteTree(manifest, components)
    expect(tree).toHaveLength(2)
    expect(tree[0].segment).toBe('index')
    expect(tree[0].component).toBe(IndexComponent)
    expect(tree[1].segment).toBe('about')
    expect(tree[1].component).toBe(AboutComponent)
  })

  it('builds nested tree from deep paths', () => {
    const manifest: RouteManifest = {
      entries: [
        { path: 'user', segment: 'user', dynamic: false, group: false, layout: false, notFound: false },
        { path: 'user/[id]', segment: '[id]', dynamic: true, group: false, layout: false, notFound: false },
      ],
    }
    const components = {
      'user': DummyComponent,
      'user/[id]': UserComponent,
    }
    const tree = buildRouteTree(manifest, components)
    expect(tree).toHaveLength(1)
    expect(tree[0].segment).toBe('user')
    expect(tree[0].children).toHaveLength(1)
    expect(tree[0].children![0].segment).toBe('[id]')
    expect(tree[0].children![0].dynamic).toBe(true)
  })

  it('handles group routes', () => {
    const manifest: RouteManifest = {
      entries: [
        { path: '(tabs)', segment: '(tabs)', dynamic: false, group: true, layout: false, notFound: false },
        { path: '(tabs)/home', segment: 'home', dynamic: false, group: false, layout: false, notFound: false },
      ],
    }
    const components = {
      '(tabs)/home': DummyComponent,
    }
    const tree = buildRouteTree(manifest, components)
    expect(tree).toHaveLength(1)
    expect(tree[0].group).toBe(true)
    expect(tree[0].children).toHaveLength(1)
    expect(tree[0].children![0].segment).toBe('home')
  })

  it('assigns layout component from layout entries', () => {
    const LayoutComponent = () => null
    const manifest: RouteManifest = {
      entries: [
        { path: 'settings', segment: 'settings', dynamic: false, group: false, layout: true, notFound: false },
      ],
    }
    const components = {
      'settings': LayoutComponent,
    }
    const tree = buildRouteTree(manifest, components)
    expect(tree[0].layout).toBe(LayoutComponent)
  })

  it('handles lazy components (returns promise)', () => {
    const lazyLoader = () => Promise.resolve({ default: DummyComponent })
    const manifest: RouteManifest = {
      entries: [
        { path: 'lazy', segment: 'lazy', dynamic: false, group: false, layout: false, notFound: false },
      ],
    }
    const components = { 'lazy': lazyLoader as any }
    const tree = buildRouteTree(manifest, components)
    // Lazy components should not be assigned directly
    expect(tree[0].component).toBeUndefined()
  })

  it('sorts entries by depth so parents come first', () => {
    const manifest: RouteManifest = {
      entries: [
        { path: 'a/b/c', segment: 'c', dynamic: false, group: false, layout: false, notFound: false },
        { path: 'a', segment: 'a', dynamic: false, group: false, layout: false, notFound: false },
        { path: 'a/b', segment: 'b', dynamic: false, group: false, layout: false, notFound: false },
      ],
    }
    const components = {
      'a': DummyComponent,
      'a/b': DummyComponent,
      'a/b/c': DummyComponent,
    }
    const tree = buildRouteTree(manifest, components)
    expect(tree).toHaveLength(1)
    expect(tree[0].segment).toBe('a')
    expect(tree[0].children![0].segment).toBe('b')
    expect(tree[0].children![0].children![0].segment).toBe('c')
  })
})

describe('matchRoute', () => {
  const tree = buildRouteTree(
    {
      entries: [
        { path: 'index', segment: 'index', dynamic: false, group: false, layout: false, notFound: false },
        { path: 'about', segment: 'about', dynamic: false, group: false, layout: false, notFound: false },
        { path: 'user', segment: 'user', dynamic: false, group: false, layout: false, notFound: false },
        { path: 'user/[id]', segment: '[id]', dynamic: true, group: false, layout: false, notFound: false },
        { path: '(tabs)', segment: '(tabs)', dynamic: false, group: true, layout: false, notFound: false },
        { path: '(tabs)/home', segment: 'home', dynamic: false, group: false, layout: false, notFound: false },
      ],
    },
    {
      'index': IndexComponent,
      'about': AboutComponent,
      'user': DummyComponent,
      'user/[id]': UserComponent,
      '(tabs)/home': DummyComponent,
    },
  )

  it('matches the index route with empty segments', () => {
    const result = matchRoute([], tree)
    expect(result).not.toBeNull()
    expect(result!.record.segment).toBe('index')
  })

  it('matches a simple static route', () => {
    const result = matchRoute(['about'], tree)
    expect(result).not.toBeNull()
    expect(result!.record.segment).toBe('about')
    expect(result!.record.component).toBe(AboutComponent)
  })

  it('matches a dynamic route and extracts params', () => {
    const result = matchRoute(['user', '42'], tree)
    expect(result).not.toBeNull()
    expect(result!.record.segment).toBe('[id]')
    expect(result!.params).toEqual({ id: '42' })
  })

  it('matches routes inside groups transparently', () => {
    const result = matchRoute(['home'], tree)
    expect(result).not.toBeNull()
    expect(result!.record.segment).toBe('home')
  })

  it('returns null for unmatched routes', () => {
    const result = matchRoute(['nonexistent'], tree)
    expect(result).toBeNull()
  })

  it('returns null for partially matched deep routes', () => {
    const result = matchRoute(['about', 'extra', 'deep'], tree)
    expect(result).toBeNull()
  })
})
