/**
 * Root layout — wraps every screen.
 *
 * For this minimal example there is only one screen (index.tsx), so the
 * layout just renders its children inside a full-screen View.
 *
 * In a real app you would add providers (auth, theme, i18n) here, or swap
 * the children for a <Stack> / <Tabs> navigator.
 */
import { View } from '@neutron/native'
import type { ComponentChildren } from 'preact'
import HomeScreen from './index'

interface RootLayoutProps {
  children?: ComponentChildren
}

export default function RootLayout({ children }: RootLayoutProps) {
  return (
    <View style={{ flex: 1, backgroundColor: '#ffffff' }}>
      {/* Single-screen app: render HomeScreen directly */}
      {children ?? <HomeScreen />}
    </View>
  )
}
