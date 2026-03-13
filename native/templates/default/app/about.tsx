/**
 * About screen — app/about.tsx → route: /about
 */
import { View, Text, Pressable } from '@neutron/native'
import { useRouter } from '@neutron/native/router'

export default function AboutScreen() {
  const { goBack } = useRouter()

  return (
    <View className="flex-1 bg-white p-8">
      <Pressable
        className="flex-row items-center mb-6"
        onPress={goBack}
      >
        <Text className="text-blue-600 text-base">← Back</Text>
      </Pressable>

      <Text className="text-2xl font-bold text-slate-900 mb-4">About</Text>
      <Text className="text-base text-slate-600 mb-3">
        This app is built with Neutron Native — universal components on React Native.
      </Text>
      <Text className="text-base text-slate-600">
        File-based routing. NeutronWind styling. Expo Go compatible.
      </Text>
    </View>
  )
}
