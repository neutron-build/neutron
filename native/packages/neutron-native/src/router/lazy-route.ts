import { Component, Suspense, createElement, lazy, type ComponentType, type ReactNode } from 'react'

class RouteLoadBoundary extends Component<{ children: ReactNode; fallback?: (error: Error) => ReactNode }, { error: Error | null }> {
  state = { error: null as Error | null }
  static getDerivedStateFromError(error: Error) { return { error } }
  componentDidCatch(error: Error) { console.error('[neutron-native] Route loading failed', error) }
  render(): ReactNode { return this.state.error ? this.props.fallback?.(this.state.error) ?? null : this.props.children }
}

/** Creating this wrapper starts no imports; React owns loading and error lifecycle. */
export function lazyRoute(load: () => Promise<{ default: ComponentType }>, loading: ReactNode = null,
  errorFallback?: (error: Error) => ReactNode): ComponentType {
  const Route = lazy(load)
  return function LazyRoute() {
    return createElement(RouteLoadBoundary, { fallback: errorFallback, children:
      createElement(Suspense, { fallback: loading }, createElement(Route)) })
  }
}
