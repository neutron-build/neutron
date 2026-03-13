/**
 * Home screen — app/index.tsx → route: /
 */
import { useState } from 'react'
import { View, Text, Pressable } from '@neutron/native'
import { useRouter } from '@neutron/native/router'

export default function HomeScreen() {
  const { navigate } = useRouter()
  const [count, setCount] = useState(0)

  return (
    <View className="flex-1 items-center justify-center bg-slate-50 p-8">
      <Text className="text-3xl font-bold text-slate-900 mb-2">
        Welcome to Neutron Native
      </Text>
      <Text className="text-base text-slate-500 text-center mb-8">
        React Native + Neutron — one codebase, iOS and Android.
      </Text>

      <View className="flex-row items-center gap-4 mb-8">
        <Pressable
          className="w-12 h-12 rounded-full bg-blue-600 items-center justify-center"
          onPress={() => setCount(c => c - 1)}
        >
          <Text className="text-white text-xl font-bold">−</Text>
        </Pressable>

        <Text className="text-4xl font-bold text-slate-900 w-16 text-center">
          {count}
        </Text>

        <Pressable
          className="w-12 h-12 rounded-full bg-blue-600 items-center justify-center"
          onPress={() => setCount(c => c + 1)}
        >
          <Text className="text-white text-xl font-bold">+</Text>
        </Pressable>
      </View>

      <Pressable
        className="bg-blue-600 px-6 py-3 rounded-lg"
        onPress={() => navigate('/about')}
      >
        <Text className="text-white font-semibold text-base">About</Text>
      </Pressable>
    </View>
  )
}
