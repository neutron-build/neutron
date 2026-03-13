import { type ComponentType } from 'react'
import { View, Text, Pressable } from 'react-native'
import { useSignal, useComputed } from '../signals/hooks.js'
import { routerState, navigate, goBack } from '../router/navigator.js'
import type { NavigatorProps, ScreenConfig, ScreenOptions } from './types.js'

// ─── Screen registry ─────────────────────────────────────────────────────────

const _screens = new Map<string, ScreenConfig>()

// ─── Stack.Screen ─────────────────────────────────────────────────────────────

interface StackScreenProps {
  name: string
  component: ComponentType
  options?: ScreenOptions
}

function StackScreen({ name, component, options }: StackScreenProps) {
  _screens.set(name, { name, component, options })
  return null
}

// ─── Stack Navigator ──────────────────────────────────────────────────────────

/**
 * Stack — push/pop navigation with native slide animation.
 * Wraps React Navigation's NativeStackNavigator under the hood.
 *
 * @example
 * <Stack initialRouteName="home">
 *   <Stack.Screen name="home" component={HomeScreen} />
 *   <Stack.Screen name="detail" component={DetailScreen} options={{ title: 'Detail' }} />
 * </Stack>
 */
export function Stack({ children, initialRouteName, screenOptions }: NavigatorProps) {
  // Render children first to populate _screens registry
  void children  // traverse children to register screens

  const currentSegment = useComputed(() => routerState.value.segments[0] ?? initialRouteName ?? '')

  const _historyStack = useSignal<string[]>([currentSegment.value])

  // Keep history stack in sync with router
  const activeScreen = _screens.get(currentSegment.value)
  const ActiveComponent = activeScreen?.component
  const activeOptions = { ...screenOptions, ...activeScreen?.options }

  const canGoBackVal = _historyStack.value.length > 1

  function handleBack() {
    if (canGoBackVal) {
      _historyStack.value = _historyStack.value.slice(0, -1)
      goBack()
    }
  }

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
        ? <ActiveComponent />
        : <View style={{ flex: 1 }} />
      }
    </View>
  )
}

Stack.Screen = StackScreen

// Allow imperative push from anywhere
Stack.push = (name: string, params?: Record<string, string>) => navigate(`/${name}`, { params })
Stack.pop = () => goBack()
