import { type ComponentType } from 'react'
import { View, Text, Pressable } from 'react-native'
import { declaredScreens, useNavigator, NavigationDepth } from './scoped.js'
import { navigate, goBack } from '../router/navigator.js'
import type { NavigatorProps, ScreenOptions } from './types.js'

// ─── Screen registry ─────────────────────────────────────────────────────────



// ─── Stack.Screen ─────────────────────────────────────────────────────────────

interface StackScreenProps {
  name: string
  component: ComponentType
  options?: ScreenOptions
}

function StackScreen({ name, component, options }: StackScreenProps) {
  void { name, component, options }
  return null
}

// ─── Stack Navigator ──────────────────────────────────────────────────────────

/**
 * Stack — scoped declarative screens backed by synchronous router history.
 * Native transitions and gestures require a React Navigation adapter.
 *
 * @example
 * <Stack initialRouteName="home">
 *   <Stack.Screen name="home" component={HomeScreen} />
 *   <Stack.Screen name="detail" component={DetailScreen} options={{ title: 'Detail' }} />
 * </Stack>
 */
export function Stack({ children, initialRouteName, screenOptions }: NavigatorProps) {
  const screens = declaredScreens(children, StackScreen)
  const nav = useNavigator(screens, initialRouteName)
  const activeScreen = nav.active
  const ActiveComponent = activeScreen?.component
  const activeOptions = { ...screenOptions, ...activeScreen?.options }
  const canGoBackVal = nav.canGoBack
  const handleBack = nav.back

  return (
    <View style={{ flex: 1 }}>
      {activeOptions.headerShown !== false && (
        <View style={{
          height: 56,
          flexDirection: 'row',
          alignItems: 'center',
          paddingHorizontal: 16,
          backgroundColor: '#fff',
          borderBottomWidth: 1,
          borderBottomColor: '#e0e0e0',
          ...activeOptions.headerStyle,
        }}>
          {canGoBackVal && (
            <Pressable
              onPress={handleBack}
              style={{ marginRight: 8, padding: 8 }}
              accessible
              accessibilityRole="button"
              accessibilityLabel="Go back"
            >
              <Text style={{ color: activeOptions.headerTintColor ?? '#007aff', fontSize: 17 }}>←</Text>
            </Pressable>
          )}
          <Text style={{
            fontSize: 17,
            fontWeight: '600',
            color: '#000',
            flex: 1,
            textAlign: canGoBackVal ? 'left' : 'center',
            ...activeOptions.headerTitleStyle,
          }}>
            {activeOptions.title ?? activeScreen?.name ?? ''}
          </Text>
        </View>
      )}
      {ActiveComponent
        ? <NavigationDepth.Provider value={nav.depth + 1}><ActiveComponent /></NavigationDepth.Provider>
        : <View style={{ flex: 1 }} />
      }
    </View>
  )
}

Stack.Screen = StackScreen

// Compatibility actions target the root. In nested screens use useStackActions().
Stack.push = (name: string, params?: Record<string, string>) => navigate(`/${name}`, { params })
Stack.pop = () => goBack()
