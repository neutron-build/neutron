/**
 * Home screen — the only screen in this example.
 *
 * Demonstrates:
 *   - View + Text primitives (Preact → Fabric → UIKit/Android Views)
 *   - Pressable with onPress handler
 *   - @preact/signals for local state
 *   - NeutronWind className (compiled to StyleSheet at build time)
 */
import { View, Text, Pressable } from '@neutron/native'
import { useSignal } from '@preact/signals'

export default function HomeScreen() {
  const count = useSignal(0)

  return (
    <View className="flex-1 items-center justify-center bg-white px-8">
      {/* Greeting */}
      <Text className="text-4xl font-bold text-slate-900 mb-2">
        Hello, World!
      </Text>
      <Text className="text-base text-slate-500 text-center mb-10">
        Rendered by Preact on React Native Fabric.
      </Text>

      {/* Counter — proves state + events work */}
      <View className="flex-row items-center gap-6">
        <Pressable
          className="w-14 h-14 rounded-full bg-blue-600 items-center justify-center"
          onPress={() => { count.value -= 1 }}
          accessibilityLabel="Decrement"
        >
          <Text className="text-white text-2xl font-bold">−</Text>
        </Pressable>

        <Text className="text-5xl font-bold text-slate-900 w-20 text-center">
          {count.value}
        </Text>

        <Pressable
          className="w-14 h-14 rounded-full bg-blue-600 items-center justify-center"
          onPress={() => { count.value += 1 }}
          accessibilityLabel="Increment"
        >
          <Text className="text-white text-2xl font-bold">+</Text>
        </Pressable>
      </View>

      <Text className="text-sm text-slate-400 mt-6">
        Tap the buttons to count
      </Text>
    </View>
  )
}
