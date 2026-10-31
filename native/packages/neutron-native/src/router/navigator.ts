/**
 * Neutron Native Router — navigator state.
 *
 * The router is a thin signal-based layer on top of React Navigation 7.
 * Signals are a projection of the attached native root state. Without an
 * adapter the synchronous scoped JS navigator owns history.
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

  if (!_rnNavigation?.isReady()) routerState.value = next

  _publishToRN()
}

export function goBack(): void {
  if (!canGoBack.value) return
  _index.value = _index.value - 1
  if (!_rnNavigation?.isReady()) routerState.value = _history.value[_index.value]
  _publishToRN()
}

export function goForward(): void {
  if (!canGoForward.value) return
  _index.value = _index.value + 1
  if (!_rnNavigation?.isReady()) routerState.value = _history.value[_index.value]
  _publishToRN()
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

export interface NativeNavigationState {
  index?: number
  routes: { name: string, key?: string, params?: Record<string, unknown>, state?: NativeNavigationState }[]
}
export interface RNNavigation {
  resetRoot(state: NativeNavigationState): void
  getRootState(): NativeNavigationState
  addListener(event: 'state', listener: () => void): () => void
  isReady(): boolean
}
let _rnNavigation: RNNavigation | null = null
let _unsubscribe: (() => void) | null = null
let _publishing = false
let _receiveNative: (() => void) | null = null
const _nativeKeys = new WeakMap<RouterState, string>()
/** Attach the public NavigationContainer ref. Native back/gestures feed the same
 * history projection; the returned cleanup owns the listener and adapter. */
export function setNavigationRef(nav: RNNavigation): () => void {
  _unsubscribe?.()
  _rnNavigation = nav
  const receive = () => {
    if (_publishing || !nav.isReady()) return
    const state = nav.getRootState()
    const states = state.routes.map(route => {
      const nested = (state: NativeNavigationState): string[] => { const child = state.routes[state.index ?? 0]; return child ? [child.name, ...(child.state ? nested(child.state) : [])] : [] }
      const path = route.state ? '/' + [route.name, ...nested(route.state)].join('/') : typeof route.params?.__neutronPath === 'string' ? route.params.__neutronPath : '/' + route.name
      const segments = path.replace(/^\//, '').split('/').filter(Boolean)
      const values = route.params?.__neutronParams
      return { pathname: '/' + segments.join('/'), segments,
        params: values && typeof values === 'object' ? values as Record<string, string> : {} }
    })
    if (!states.length) return
    const index = Math.min(state.index ?? states.length - 1, states.length - 1)
    // A native back preserves the forward entries, just like programmatic back.
    const prefix = states.every((value, i) => JSON.stringify(value) === JSON.stringify(_history.value[i]))
    if (!prefix) _history.value = states
    states.forEach((_value, i) => { const key = state.routes[i].key; if (key) _nativeKeys.set(_history.value[i], key) })
    _index.value = index
    routerState.value = _history.value[index]
  }
  _receiveNative = receive
  const unsubscribe = nav.addListener('state', receive)
  _unsubscribe = unsubscribe
  receive()
  return () => { if (_rnNavigation === nav) { unsubscribe(); _rnNavigation = null; _unsubscribe = null; _receiveNative = null } }
}
function nestedRoute(segments: string[]): NativeNavigationState {
  return { index: 0, routes: [{ name: segments[0], ...(segments.length > 1 ? { state: nestedRoute(segments.slice(1)) } : {}) }] }
}
function _publishToRN(): void {
  const nav = _rnNavigation
  if (!nav?.isReady()) return
  _publishing = true
  try {
    nav.resetRoot({ index: _index.value, routes: _history.value.slice(0, _index.value + 1).map(route => ({
      ...(_nativeKeys.has(route) ? { key: _nativeKeys.get(route) } : {}),
      name: matchRegisteredRoute(route.pathname)?.screenName ?? route.segments[0] ?? 'index',
      ...(!matchRegisteredRoute(route.pathname) && route.segments.length > 1 ? { state: nestedRoute(route.segments.slice(1)) } : {}),
      params: { ...matchRegisteredRoute(route.pathname)?.params, __neutronPath: route.pathname, __neutronParams: route.params },
    })) })
  } finally { _publishing = false; _receiveNative?.() }
}
