/**
 * Neutron Native Router — navigator state.
 *
 * The router is a thin signal-based layer on top of React Navigation 7.
 * We keep our own signal state that mirrors NavigationState so that
 * components can `useRoute()` with fine-grained reactivity from signals.
 */

import { signal, computed } from '@preact/signals-core'
import type { RouterState, NavigateOptions } from './types.js'
import { matchRegisteredRoute } from './route-registry.js'

// ─── Router state ─────────────────────────────────────────────────────────────

/** Current navigation state. Read with useRoute() or router.state.value */
export const routerState = signal<RouterState>({
  segments: [],
  params: {},
  pathname: '/',
})

/** Computed pathname string from segments */
export const pathname = computed(() => '/' + routerState.value.segments.join('/'))

/** Computed params */
export const params = computed(() => routerState.value.params)

// ─── Navigation history stack ─────────────────────────────────────────────────

const _history = signal<RouterState[]>([routerState.value])
const _index = signal(0)

export const canGoBack = computed(() => _index.value > 0)
export const canGoForward = computed(() => _index.value < _history.value.length - 1)

// ─── Navigation actions ───────────────────────────────────────────────────────

/**
 * Navigate to a path.
 * Supports named params: navigate('/user/[id]', { params: { id: '42' } })
 * or positional: navigate('/user/42')
 */
export function navigate(
  path: string,
  opts: NavigateOptions & { params?: Record<string, string> } = {},
): void {
  const segments = path.replace(/^\//, '').split('/').filter(Boolean)
  const extractedParams = opts.params ?? {}

  const next: RouterState = {
    segments,
    params: extractedParams,
    pathname: '/' + segments.join('/'),
  }

  if (opts.replace) {
    const h = _history.value.slice(0, _index.value)
    _history.value = [...h, next]
    _index.value = h.length
  } else {
    // Truncate forward history when navigating from middle
    const h = _history.value.slice(0, _index.value + 1)
    _history.value = [...h, next]
    _index.value = _index.value + 1
  }

  routerState.value = next

  // Bridge to React Navigation if available
  _bridgeToRN('navigate', path, extractedParams)
}

export function goBack(): void {
  if (!canGoBack.value) return
  _index.value = _index.value - 1
  routerState.value = _history.value[_index.value]
  _bridgeToRN('back', '')
}

export function goForward(): void {
  if (!canGoForward.value) return
  _index.value = _index.value + 1
  routerState.value = _history.value[_index.value]
}

export function replace(path: string, p?: Record<string, string>): void {
  navigate(path, { replace: true, params: p })
}

// ─── Deep link handler ────────────────────────────────────────────────────────

/**
 * Call this with an incoming deep link URL. The router will parse the path
 * and navigate to the appropriate screen.
 */
export function handleDeepLink(url: string): void {
  try {
    const u = new URL(url)
    navigate(u.pathname, { replace: true })
  } catch {
    // Bare path without scheme
    navigate(url, { replace: true })
  }
}

// ─── React Navigation bridge ──────────────────────────────────────────────────

type RNNavigation = {
  navigate(name: string, params?: Record<string, string>): void
  goBack(): void
}

let _rnNavigation: RNNavigation | null = null

/** Register the React Navigation navigation ref so the router can delegate */
export function setNavigationRef(nav: RNNavigation): void {
  _rnNavigation = nav
}

function _bridgeToRN(action: 'navigate' | 'back', path: string, p?: Record<string, string>): void {
  if (!_rnNavigation) return
  if (action === 'back') {
    _rnNavigation.goBack()
  } else {
    // Try registered route match first
    const registered = matchRegisteredRoute(path)
    if (registered) {
      _rnNavigation.navigate(registered.screenName, registered.params)
      return
    }

    // Fallback to slug transform (backward compatible)
    const screenName = path.replace(/^\//, '').replace(/\//g, '_') || 'index'
    _rnNavigation.navigate(screenName, p)
  }
}
