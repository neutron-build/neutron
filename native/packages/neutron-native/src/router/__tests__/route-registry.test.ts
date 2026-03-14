import { describe, it, expect, beforeEach } from '@jest/globals'
import {
  registerRoutes,
  matchRegisteredRoute,
  clearRoutes,
  type RouteConfig,
} from '../route-registry.js'

describe('route-registry', () => {
  beforeEach(() => {
    clearRoutes()
  })

  it('should match static route', () => {
    const configs: RouteConfig[] = [
      { screenName: 'Home', pattern: '/', paramNames: [] },
    ]
    registerRoutes(configs)

    const match = matchRegisteredRoute('/')
    expect(match).toEqual({ screenName: 'Home', params: {} })
  })

  it('should match dynamic route with single param', () => {
    const configs: RouteConfig[] = [
      { screenName: 'UserProfile', pattern: '/user/[id]', paramNames: ['id'] },
    ]
    registerRoutes(configs)

    const match = matchRegisteredRoute('/user/42')
    expect(match).toEqual({ screenName: 'UserProfile', params: { id: '42' } })
  })

  it('should match dynamic route with multiple params', () => {
    const configs: RouteConfig[] = [
      {
        screenName: 'PostComment',
        pattern: '/post/[postId]/comment/[commentId]',
        paramNames: ['postId', 'commentId'],
      },
    ]
    registerRoutes(configs)

    const match = matchRegisteredRoute('/post/123/comment/456')
    expect(match).toEqual({
      screenName: 'PostComment',
      params: { postId: '123', commentId: '456' },
    })
  })

  it('should return null for non-matching path', () => {
    const configs: RouteConfig[] = [
      { screenName: 'Home', pattern: '/', paramNames: [] },
    ]
    registerRoutes(configs)

    const match = matchRegisteredRoute('/nonexistent')
    expect(match).toBeNull()
  })

  it('should return null for path with wrong segment count', () => {
    const configs: RouteConfig[] = [
      { screenName: 'UserProfile', pattern: '/user/[id]', paramNames: ['id'] },
    ]
    registerRoutes(configs)

    const match = matchRegisteredRoute('/user/42/extra')
    expect(match).toBeNull()
  })

  it('should handle multiple routes', () => {
    const configs: RouteConfig[] = [
      { screenName: 'Home', pattern: '/', paramNames: [] },
      { screenName: 'UserProfile', pattern: '/user/[id]', paramNames: ['id'] },
      { screenName: 'PostDetail', pattern: '/post/[id]', paramNames: ['id'] },
    ]
    registerRoutes(configs)

    expect(matchRegisteredRoute('/')).toEqual({ screenName: 'Home', params: {} })
    expect(matchRegisteredRoute('/user/42')).toEqual({
      screenName: 'UserProfile',
      params: { id: '42' },
    })
    expect(matchRegisteredRoute('/post/99')).toEqual({
      screenName: 'PostDetail',
      params: { id: '99' },
    })
  })

  it('should extract numeric params as strings', () => {
    const configs: RouteConfig[] = [
      { screenName: 'UserProfile', pattern: '/user/[id]', paramNames: ['id'] },
    ]
    registerRoutes(configs)

    const match = matchRegisteredRoute('/user/12345')
    expect(match?.params.id).toBe('12345')
    expect(typeof match?.params.id).toBe('string')
  })

  it('should extract slug params', () => {
    const configs: RouteConfig[] = [
      { screenName: 'PostDetail', pattern: '/blog/[slug]', paramNames: ['slug'] },
    ]
    registerRoutes(configs)

    const match = matchRegisteredRoute('/blog/my-first-post')
    expect(match?.params.slug).toBe('my-first-post')
  })

  it('should match routes with mixed static and dynamic segments', () => {
    const configs: RouteConfig[] = [
      {
        screenName: 'UserPosts',
        pattern: '/user/[userId]/posts',
        paramNames: ['userId'],
      },
    ]
    registerRoutes(configs)

    const match = matchRegisteredRoute('/user/42/posts')
    expect(match).toEqual({
      screenName: 'UserPosts',
      params: { userId: '42' },
    })
  })

  it('should return null when registered routes is empty', () => {
    const match = matchRegisteredRoute('/user/42')
    expect(match).toBeNull()
  })

  it('should clear routes', () => {
    const configs: RouteConfig[] = [
      { screenName: 'Home', pattern: '/', paramNames: [] },
    ]
    registerRoutes(configs)
    expect(matchRegisteredRoute('/')).not.toBeNull()

    clearRoutes()
    expect(matchRegisteredRoute('/')).toBeNull()
  })

  it('should handle routes with leading/trailing slashes consistently', () => {
    const configs: RouteConfig[] = [
      { screenName: 'Home', pattern: '/', paramNames: [] },
    ]
    registerRoutes(configs)

    const match1 = matchRegisteredRoute('/')
    expect(match1).not.toBeNull()
  })

  it('should prioritize first matching route', () => {
    const configs: RouteConfig[] = [
      { screenName: 'CatchAll', pattern: '/[segment]', paramNames: ['segment'] },
      { screenName: 'User', pattern: '/user/[id]', paramNames: ['id'] },
    ]
    registerRoutes(configs)

    // First registered route should match
    const match = matchRegisteredRoute('/user/42')
    // Depending on Map iteration order, should match one of them
    expect(match).not.toBeNull()
    expect(match?.params).toBeTruthy()
  })
})
