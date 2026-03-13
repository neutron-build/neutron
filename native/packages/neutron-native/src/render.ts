/**
 * Neutron Native — render entry point.
 *
 * Uses React Native's standard AppRegistry. No custom renderer —
 * React Native's built-in Fabric renderer handles all native view creation.
 *
 * On web builds, the bundler maps `react` → `preact/compat` automatically,
 * so the same component code works with Preact's 3KB runtime.
 *
 * @example
 * import { NeutronApp } from '@neutron/native'
 * import App from './app/_layout'
 * NeutronApp({ component: App, appName: 'MyApp' })
 */

import { AppRegistry } from 'react-native'
import type { ComponentType } from 'react'

// ─── NeutronApp — high-level AppRegistry helper ───────────────────────────────

interface NeutronAppOptions {
  /** Root component to render */
  component: ComponentType
  /** AppRegistry name (must match native side) */
  appName: string
}

/**
 * Register a Neutron Native app with React Native's AppRegistry.
 * This is the recommended entry point for new projects.
 */
export function NeutronApp({ component, appName }: NeutronAppOptions): void {
  AppRegistry.registerComponent(appName, () => component)
}
