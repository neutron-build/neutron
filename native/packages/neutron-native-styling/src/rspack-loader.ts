/**
 * NeutronWind Rspack/Webpack loader.
 *
 * Wraps the Babel plugin for use in a Re.Pack 5 (Rspack) build pipeline.
 * Add to your repack.config.ts:
 *
 *   {
 *     test: /\.(tsx|jsx)$/,
 *     use: [{ loader: '@neutron/native-styling/rspack', options: { platform: 'ios' } }],
 *   }
 *
 * Pass `platform: 'ios' | 'android' | 'all'` via loader options to enable
 * platform-specific class variants (ios:shadow-lg, android:elevation-4).
 * Re.Pack automatically provides the platform via REPACK_PLATFORM env var if
 * you don't set it explicitly.
 */

import { transformSync } from '@babel/core'
import neutronWindPlugin from './babel-plugin.js'
import type { Platform } from './babel-plugin.js'

export interface LoaderContext {
  resourcePath: string
  getOptions(): Record<string, unknown>
  callback(err: Error | null, result?: string, sourceMap?: unknown): void
}

export default function neutronWindLoader(this: LoaderContext, source: string): void {
  const rawOptions = this.getOptions()

  // Auto-detect platform from Re.Pack's env if not explicitly set
  const platform: Platform = (rawOptions.platform as Platform)
    ?? (process.env.REPACK_PLATFORM as Platform)
    ?? 'all'

  const result = transformSync(source, {
    filename: this.resourcePath,
    plugins: [[neutronWindPlugin, { ...rawOptions, platform }]],
    sourceMaps: true,
    configFile: false,
    babelrc: false,
  })

  if (!result) {
    this.callback(null, source)
    return
  }

  this.callback(null, result.code ?? source, result.map)
}
