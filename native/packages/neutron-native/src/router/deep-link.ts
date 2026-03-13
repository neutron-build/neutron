/**
 * Deep link handling — registers RN Linking listeners and routes them
 * through the Neutron Native router.
 */

import { handleDeepLink } from './navigator.js'

/** App URL schemes and associated hostnames to match */
export interface DeepLinkConfig {
  /** e.g. ['myapp://'] */
  schemes: string[]
  /** Universal link domains, e.g. ['example.com'] */
  domains?: string[]
}

/**
 * Initialize deep link handling. Call once at app startup after render().
 */
export function initDeepLinks(config: DeepLinkConfig): () => void {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const Linking = (globalThis as any).ReactNativeLinking ?? tryRequire('react-native')?.Linking
  if (!Linking) return () => {}

  function onUrl({ url }: { url: string }) {
    if (_matchesConfig(url, config)) {
      handleDeepLink(url)
    }
  }

  // Handle cold start URL
  Linking.getInitialURL().then((url: string | null) => {
    if (url && _matchesConfig(url, config)) {
      handleDeepLink(url)
    }
  })

  // Handle warm/hot URL
  const subscription = Linking.addEventListener('url', onUrl)
  return () => subscription.remove()
}

function _matchesConfig(url: string, config: DeepLinkConfig): boolean {
  if (config.schemes.some(s => url.startsWith(s))) return true
  if (config.domains?.some(d => url.includes(d))) return true
  return false
}

function tryRequire(mod: string): Record<string, unknown> | null {
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    return require(mod)
  } catch {
    return null
  }
}
