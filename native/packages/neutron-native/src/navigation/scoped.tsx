import { Children, createContext, isValidElement, useContext, useEffect, useSyncExternalStore, type ComponentType, type ReactNode } from 'react'
import { routerState, navigate, replace, goBack, canGoBack } from '../router/navigator.js'
import type { ScreenConfig } from './types.js'
export const NavigationDepth = createContext(0)
export function useStackActions() {
  // A rendered screen receives parent depth + 1; its actions target siblings.
  const depth = Math.max(0, useContext(NavigationDepth) - 1)
  const path = (name: string) => '/' + [...routerState.value.segments.slice(0, depth), name].join('/')
  return { push: (name: string, params?: Record<string, string>) => navigate(path(name), { params }),
    replace: (name: string, params?: Record<string, string>) => replace(path(name), params), pop: goBack }
}

/** Read JSX declarations without running Screen or any route component. */
export function declaredScreens(children: ReactNode, marker: ComponentType<any>): ScreenConfig[] {
  const screens: ScreenConfig[] = []
  const names = new Set<string>()
  Children.forEach(children, child => {
    if (!isValidElement<ScreenConfig>(child)) return
    if (child.type !== marker) throw new Error('Navigator children must be its Screen declarations')
    const { name, component, options } = child.props
    if (!name || !component || names.has(name)) throw new Error(`Invalid or duplicate screen: ${name}`)
    names.add(name); screens.push({ name, component, options })
  })
  return screens
}

export function useNavigator(screens: ScreenConfig[], initial?: string) {
  const depth = useContext(NavigationDepth)
  const route = useSyncExternalStore(
    listener => routerState.subscribe(() => listener()),
    () => routerState.value,
    () => routerState.value,
  )
  const requested = route.segments[depth] ?? initial
  const active = screens.find(s => s.name === requested) ?? screens.find(s => s.name === initial) ?? screens[0]
  useEffect(() => {
    if (active && active.name !== route.segments[depth]) {
      replace('/' + [...route.segments.slice(0, depth), active.name].join('/'), route.params)
    }
  }, [active?.name, depth, route])
  return { depth, active, canGoBack: canGoBack.value, back: goBack,
    select: (name: string) => navigate('/' + [...route.segments.slice(0, depth), name].join('/')) }
}
