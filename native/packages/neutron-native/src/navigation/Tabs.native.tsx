import { type ComponentType } from 'react'
import { View, Text, Pressable } from 'react-native'
import { useComputed } from '../signals/hooks.js'
import { routerState, navigate } from '../router/navigator.js'
import type { NavigatorProps, ScreenConfig, ScreenOptions } from './types.js'

// ─── Screen registry ─────────────────────────────────────────────────────────

interface TabScreenConfig extends ScreenConfig {
  options: ScreenOptions & {
    tabBarLabel?: string
    tabBarIcon?: ComponentType<{ focused: boolean; color: string; size: number }>
    tabBarBadge?: string | number
  }
}

const _tabScreens: TabScreenConfig[] = []

// ─── Tabs.Screen ──────────────────────────────────────────────────────────────

interface TabsScreenProps {
  name: string
  component: ComponentType
  options?: TabScreenConfig['options']
}

function TabsScreen({ name, component, options }: TabsScreenProps) {
  if (!_tabScreens.find(s => s.name === name)) {
    _tabScreens.push({ name, component, options: options ?? {} })
  }
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
  children: _children,
  initialRouteName,
  tabBarStyle,
  activeColor = ACTIVE_COLOR,
  inactiveColor = INACTIVE_COLOR,
  screenOptions,
}: TabsProps) {
  const activeTab = useComputed(() => routerState.value.segments[0] ?? initialRouteName ?? _tabScreens[0]?.name ?? '')

  const activeScreen = _tabScreens.find(s => s.name === activeTab.value)
  const ActiveComponent = activeScreen?.component

  return (
    <View style={{ flex: 1 }}>
      <View style={{ flex: 1 }}>
        {ActiveComponent ? <ActiveComponent /> : null}
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
          const isFocused = screen.name === activeTab.value
          const color = isFocused ? activeColor : inactiveColor
          const label = screen.options?.tabBarLabel ?? screen.name
          const Icon = screen.options?.tabBarIcon
          const badge = screen.options?.tabBarBadge
          void { ...screenOptions, ...screen.options }

          return (
            <Pressable
              key={screen.name}
              onPress={() => navigate(`/${screen.name}`)}
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
