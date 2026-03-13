/**
 * Root layout — wraps all screens.
 * Add providers (auth, theme, etc.) here.
 */
import { View } from '@neutron/native'
import type { ReactNode } from 'react'

interface RootLayoutProps {
  children?: ReactNode
}

export default function RootLayout({ children }: RootLayoutProps) {
  return (
    <View className="flex-1 bg-white">
      {children}
    </View>
  )
}
