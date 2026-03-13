import { type ComponentType } from 'react'
import { View, Text, Pressable } from 'react-native'
import { useSignal, useComputed } from '../signals/hooks.js'
import { routerState, navigate } from '../router/navigator.js'
import type { NavigatorProps, ScreenConfig, ScreenOptions } from './types.js'

// ─── Screen registry ─────────────────────────────────────────────────────────

const _drawerScreens: ScreenConfig[] = []

// ─── Drawer.Screen ────────────────────────────────────────────────────────────

interface DrawerScreenProps {
  name: string
  component: ComponentType
  options?: ScreenOptions & { drawerLabel?: string; drawerIcon?: ComponentType<{ focused: boolean; color: string }> }
}

function DrawerScreen({ name, component, options }: DrawerScreenProps) {
  if (!_drawerScreens.find(s => s.name === name)) {
    _drawerScreens.push({ name, component, options })
  }
  return null
}

// ─── Drawer Navigator ─────────────────────────────────────────────────────────

const DRAWER_WIDTH = 280

interface DrawerProps extends NavigatorProps {
  drawerStyle?: Record<string, string | number>
}

/**
 * Drawer — side-panel drawer navigation.
 *
 * @example
 * <Drawer>
 *   <Drawer.Screen name="home" component={HomeScreen} options={{ drawerLabel: 'Home' }} />
 *   <Drawer.Screen name="settings" component={SettingsScreen} />
 * </Drawer>
 */
export function Drawer({
  children: _children,
  initialRouteName,
  drawerStyle,
  screenOptions,
}: DrawerProps) {
  const isOpen = useSignal(false)
  const activeTab = useComputed(() => routerState.value.segments[0] ?? initialRouteName ?? _drawerScreens[0]?.name ?? '')

  const activeScreen = _drawerScreens.find(s => s.name === activeTab.value)
  const ActiveComponent = activeScreen?.component
  const activeOptions = { ...screenOptions, ...activeScreen?.options }

  return (
    <View style={{ flex: 1 }}>
      {/* Header with hamburger */}
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
          <Pressable
            onPress={() => { isOpen.value = !isOpen.value }}
            style={{ padding: 8, marginRight: 8 }}
            accessible
            accessibilityRole="button"
            accessibilityLabel="Open menu"
          >
            <Text style={{ fontSize: 20 }}>☰</Text>
          </Pressable>
          <Text style={{
            fontSize: 17,
            fontWeight: '600',
            flex: 1,
            color: '#000',
            ...activeOptions.headerTitleStyle,
          }}>
            {activeOptions.title ?? activeScreen?.name ?? ''}
          </Text>
        </View>
      )}

      {/* Content area */}
      <View style={{ flex: 1 }}>
        {ActiveComponent ? <ActiveComponent /> : null}
      </View>

      {/* Overlay + Drawer panel */}
      {isOpen.value && (
        <View style={{ position: 'absolute', top: 0, left: 0, right: 0, bottom: 0 }}>
          {/* Tap-to-close backdrop */}
          <Pressable
            onPress={() => { isOpen.value = false }}
            style={{
              position: 'absolute', top: 0, left: 0, right: 0, bottom: 0,
              backgroundColor: 'rgba(0,0,0,0.4)',
            }}
          />
          {/* Drawer panel */}
          <View style={{
            position: 'absolute',
            top: 0,
            left: 0,
            bottom: 0,
            width: DRAWER_WIDTH,
            backgroundColor: '#fff',
            paddingTop: 64,
            paddingHorizontal: 16,
            ...drawerStyle,
          }}>
            {_drawerScreens.map(screen => {
              const opts = screen.options as DrawerScreenProps['options']
              const isFocused = screen.name === activeTab.value
              const label = opts?.drawerLabel ?? screen.name
              const Icon = opts?.drawerIcon

              return (
                <Pressable
                  key={screen.name}
                  onPress={() => {
                    navigate(`/${screen.name}`)
                    isOpen.value = false
                  }}
                  accessible
                  accessibilityRole="menuitem"
                  style={{
                    flexDirection: 'row',
                    alignItems: 'center',
                    paddingVertical: 12,
                    paddingHorizontal: 16,
                    borderRadius: 8,
                    backgroundColor: isFocused ? '#f0f0f0' : 'transparent',
                    marginBottom: 4,
                  }}
                >
                  {Icon && <Icon focused={isFocused} color={isFocused ? '#007aff' : '#555'} />}
                  <Text style={{
                    fontSize: 16,
                    marginLeft: Icon ? 12 : 0,
                    color: isFocused ? '#007aff' : '#000',
                    fontWeight: isFocused ? '600' : '400',
                  }}>
                    {label}
                  </Text>
                </Pressable>
              )
            })}
          </View>
        </View>
      )}
    </View>
  )
}

Drawer.Screen = DrawerScreen
