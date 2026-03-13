/**
 * Router hooks — ergonomic access to router state inside components.
 */

import { useComputed } from '../signals/hooks.js'
import { routerState, navigate, goBack, replace } from './navigator.js'

/** Returns current route params (reactive, signal-backed) */
export function useParams<T extends Record<string, string> = Record<string, string>>(): T {
  return useComputed(() => routerState.value.params as T).value
}

/** Returns current pathname string (reactive) */
export function usePathname(): string {
  return useComputed(() => routerState.value.pathname).value
}

/** Returns router actions */
export function useRouter() {
  return { navigate, goBack, replace }
}

/** Returns the full current route state */
export function useRoute() {
  return useComputed(() => routerState.value).value
}

/**
 * Returns the current URL search params as a plain object.
 * Reactive — re-renders when the query string changes.
 *
 * @example
 * const { q, page } = useSearchParams<{ q: string; page: string }>()
 */
export function useSearchParams<T extends Record<string, string> = Record<string, string>>(): T {
  return useComputed(() => {
    const { pathname } = routerState.value
    const qIdx = pathname.indexOf('?')
    if (qIdx === -1) return {} as T
    const qs = pathname.slice(qIdx + 1)
    const result: Record<string, string> = {}
    for (const [key, val] of new URLSearchParams(qs).entries()) {
      result[key] = val
    }
    return result as T
  }).value
}

/**
 * Imperative search-params helpers — set or delete params without navigation.
 * The current path is preserved; only the query string changes.
 */
export function useSearchParamsSetter() {
  function setParam(key: string, value: string) {
    const { pathname } = routerState.value
    const [base, qs = ''] = pathname.split('?')
    const p = new URLSearchParams(qs)
    p.set(key, value)
    navigate(`${base}?${p.toString()}`, { replace: true })
  }

  function deleteParam(key: string) {
    const { pathname } = routerState.value
    const [base, qs = ''] = pathname.split('?')
    const p = new URLSearchParams(qs)
    p.delete(key)
    const newQs = p.toString()
    navigate(newQs ? `${base}?${newQs}` : base, { replace: true })
  }

  return { setParam, deleteParam }
}
