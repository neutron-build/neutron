import { type ComponentType } from 'react'
import { View, Text, Pressable } from 'react-native'
import { declaredScreens, useNavigator, NavigationDepth } from './scoped.js'

import type { NavigatorProps, ScreenConfig, ScreenOptions } from './types.js'

// ─── Screen registry ─────────────────────────────────────────────────────────

interface TabScreenConfig extends ScreenConfig {
  options: ScreenOptions & {
    tabBarLabel?: string
    tabBarIcon?: ComponentType<{ focused: boolean; color: string; size: number }>
    tabBarBadge?: string | number
  }
}



// ─── Tabs.Screen ──────────────────────────────────────────────────────────────

interface TabsScreenProps {
  name: string
  component: ComponentType
  options?: TabScreenConfig['options']
}

function TabsScreen({ name, component, options }: TabsScreenProps) {
  void { name, component, options }
  return null
}

// ─── Tab Navigator ────────────────────────────────────────────────────────────

const ACTIVE_COLOR = '#007aff'
const INACTIVE_COLOR = '#8e8e93'
const TAB_BAR_HEIGHT = 49

interface TabsProps extends NavigatorProps {
  tabBarStyle?: Record<string, string | number>
  activeColor?: string
  inactiveColor?: string
}

/**
 * Tabs — bottom tab navigator.
 *
 * @example
 * <Tabs>
 *   <Tabs.Screen name="home" component={HomeScreen} options={{ tabBarLabel: 'Home' }} />
 *   <Tabs.Screen name="profile" component={ProfileScreen} options={{ tabBarLabel: 'Profile' }} />
 * </Tabs>
 */
export function Tabs({
  children,
  initialRouteName,
  tabBarStyle,
  activeColor = ACTIVE_COLOR,
  inactiveColor = INACTIVE_COLOR,
  screenOptions,
}: TabsProps) {
  const _tabScreens = declaredScreens(children, TabsScreen) as TabScreenConfig[]
  const nav = useNavigator(_tabScreens, initialRouteName)
  const activeScreen = nav.active
  const ActiveComponent = activeScreen?.component

  return (
    <View style={{ flex: 1 }}>
      <View style={{ flex: 1 }}>
        {ActiveComponent ? <NavigationDepth.Provider value={nav.depth + 1}><ActiveComponent /></NavigationDepth.Provider> : null}
      </View>
      <View style={{
        height: TAB_BAR_HEIGHT,
        flexDirection: 'row',
        backgroundColor: '#f8f8f8',
        borderTopWidth: 0.5,
        borderTopColor: '#c8c7cc',
        ...tabBarStyle,
      }}>
        {_tabScreens.map(screen => {
          const isFocused = screen.name === activeScreen?.name
          const color = isFocused ? activeColor : inactiveColor
          const label = screen.options?.tabBarLabel ?? screen.name
          const Icon = screen.options?.tabBarIcon
          const badge = screen.options?.tabBarBadge
          void { ...screenOptions, ...screen.options }

          return (
            <Pressable
              key={screen.name}
              onPress={() => nav.select(screen.name)}
              accessible
              accessibilityRole="tab"
              accessibilityState={{ selected: isFocused }}
              accessibilityLabel={label}
              style={{
                flex: 1,
                alignItems: 'center',
                justifyContent: 'center',
                paddingVertical: 6,
              }}
            >
              {Icon && (
                <View style={{ position: 'relative' }}>
                  <Icon focused={isFocused} color={color} size={24} />
                  {badge != null && (
                    <View style={{
                      position: 'absolute',
                      top: -4,
                      right: -8,
                      backgroundColor: '#ff3b30',
                      borderRadius: 8,
                      minWidth: 16,
                      height: 16,
                      alignItems: 'center',
                      justifyContent: 'center',
                      paddingHorizontal: 3,
                    }}>
                      <Text style={{ color: '#fff', fontSize: 10, fontWeight: '700' }}>
                        {String(badge)}
                      </Text>
                    </View>
                  )}
                </View>
              )}
              <Text style={{ fontSize: 10, color, marginTop: Icon ? 2 : 0 }}>
                {label}
              </Text>
            </Pressable>
          )
        })}
      </View>
    </View>
  )
}

Tabs.Screen = TabsScreen
