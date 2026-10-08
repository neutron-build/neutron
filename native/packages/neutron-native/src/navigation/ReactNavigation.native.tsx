/** Maintained native navigation UI; the router observes its public container ref. */
import { useEffect, useRef, type ReactNode } from 'react'
import { NavigationContainer, createNavigationContainerRef } from '@react-navigation/native'
import { createNativeStackNavigator } from '@react-navigation/native-stack'
import { setNavigationRef, type RNNavigation } from '../router/navigator.js'
import { declaredScreens, NavigationDepth, useNavigator } from './scoped.js'
import type { NavigatorProps, ScreenConfig } from './types.js'
const Native = createNativeStackNavigator()
function Screen(_props: ScreenConfig) { return null }
/** Mount exactly one root per router runtime. Native gestures/back are projected
 * into router subscriptions; replace/forward reset this same native root. */
export function ReactNavigationRoot({ children }: { children: ReactNode }) {
  const ref = useRef(createNavigationContainerRef()).current
  const cleanup = useRef<(() => void) | undefined>(undefined)
  useEffect(() => () => cleanup.current?.(), [])
  return <NavigationContainer ref={ref} onReady={() => {
    cleanup.current?.()
    cleanup.current = setNavigationRef(ref as unknown as RNNavigation)
  }}>{children}</NavigationContainer>
}
export function NativeStack({ children, initialRouteName, screenOptions }: NavigatorProps) {
  const screens = declaredScreens(children, Screen)
  const nav = useNavigator(screens, initialRouteName)
  return <Native.Navigator initialRouteName={initialRouteName} screenOptions={screenOptions as any}>
    {screens.map(screen => <Native.Screen key={screen.name} name={screen.name} options={screen.options as any}>
      {() => <NavigationDepth.Provider value={nav.depth + 1}><screen.component /></NavigationDepth.Provider>}
    </Native.Screen>)}
  </Native.Navigator>
}
NativeStack.Screen = Screen
