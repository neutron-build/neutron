/**
 * Compat helpers — WEB BUILDS ONLY.
 *
 * On web, the bundler maps react → preact/compat so your React-import code
 * runs on Preact's 3KB runtime. On mobile, react is real React (provided by
 * React Native) — these aliases are NOT used.
 *
 * Usage in your web bundler config (vite, webpack, rspack):
 *   import { preactCompatAliases } from '@neutron/native/compat'
 *   resolve: { alias: preactCompatAliases() }
 */

import { createRequire } from 'module'

const require = createRequire(import.meta.url)

/**
 * Returns webpack/rspack/vite resolve.alias entries that map React → Preact/compat.
 *
 * Use this ONLY in web build configs. Mobile builds (Re.Pack) should NOT
 * use these aliases — they need real React for React Native's Fabric renderer.
 */
export function preactCompatAliases(): Record<string, string> {
  return {
    'react':                  require.resolve('preact/compat'),
    'react-dom':              require.resolve('preact/compat'),
    'react-dom/server':       require.resolve('preact/compat/server'),
    'react/jsx-runtime':      require.resolve('preact/jsx-runtime'),
    'react/jsx-dev-runtime':  require.resolve('preact/jsx-runtime'),
  }
}
