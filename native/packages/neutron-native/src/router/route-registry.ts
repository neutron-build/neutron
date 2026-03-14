/**
 * Route registry for manual route registration.
 *
 * Allows apps to register routes and match them by path pattern,
 * enabling direct navigation via React Navigation screenName + params.
 *
 * Example:
 *   registerRoutes([
 *     { screenName: 'UserProfile', pattern: '/user/[id]', paramNames: ['id'] },
 *     { screenName: 'PostDetail', pattern: '/post/[id]/comment/[cid]', paramNames: ['id', 'cid'] },
 *   ])
 *
 *   matchRegisteredRoute('/user/42') → { screenName: 'UserProfile', params: { id: '42' } }
 */

export interface RouteConfig {
  screenName: string
  pattern: string
  paramNames: readonly string[]
}

// Module-level registry
const registry = new Map<string, RouteConfig>()

/**
 * Register routes for pattern matching.
 */
export function registerRoutes(configs: RouteConfig[]): void {
  for (const config of configs) {
    registry.set(config.pattern, config)
  }
}

/**
 * Match a path against registered routes.
 * Returns matched screenName + extracted params, or null if no match.
 */
export function matchRegisteredRoute(
  path: string,
): { screenName: string; params: Record<string, string> } | null {
  // Try exact match first, then pattern match
  for (const [pattern, config] of registry.entries()) {
    const match = matchPattern(pattern, path)
    if (match) {
      return {
        screenName: config.screenName,
        params: match,
      }
    }
  }
  return null
}

/**
 * Match a path against a pattern like '/user/[id]' or '/post/[id]/comment/[cid]'.
 * Returns extracted params or null if no match.
 */
function matchPattern(pattern: string, path: string): Record<string, string> | null {
  const patternSegments = pattern.split('/').filter(Boolean)
  const pathSegments = path.split('/').filter(Boolean)

  if (patternSegments.length !== pathSegments.length) {
    return null
  }

  const params: Record<string, string> = {}

  for (let i = 0; i < patternSegments.length; i++) {
    const patternSeg = patternSegments[i]
    const pathSeg = pathSegments[i]

    // Check if pattern segment is a param like [id]
    if (patternSeg.startsWith('[') && patternSeg.endsWith(']')) {
      const paramName = patternSeg.slice(1, -1)
      params[paramName] = pathSeg
    } else if (patternSeg !== pathSeg) {
      // Literal segment must match exactly
      return null
    }
  }

  return params
}

/**
 * Clear all registered routes (mainly for testing).
 */
export function clearRoutes(): void {
  registry.clear()
}
