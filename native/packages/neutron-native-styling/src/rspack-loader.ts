/**
 * NeutronWind Rspack/Webpack loader.
 *
 * Wraps the Babel plugin for use in a Re.Pack 5 (Rspack) build pipeline.
 * Add to your repack.config.ts:
 *
 *   {
 *     test: /\.(tsx|jsx)$/,
 *     use: [{ loader: '@neutron-build/native-styling/rspack', options: { platform: 'ios' } }],
 *   }
 *
 * Pass `platform: 'ios' | 'android' | 'all'` via loader options to enable
 * platform-specific class variants (ios:shadow-lg, android:elevation-4).
 * Re.Pack automatically provides the platform via REPACK_PLATFORM env var if
 * you don't set it explicitly.
 *
 * The loader PARSES TypeScript and JSX itself (NF-NR-02): a .tsx input must
 * reach NeutronWind as JSX with type annotations intact, before any SWC
 * stage lowers it. It only strips nothing and emits the same syntax back —
 * the downstream pipeline (SWC/babel-preset) keeps responsibility for
 * lowering. Loading a syntax plugin does not transform code.
 */

import { transformSync } from '@babel/core'
import syntaxTypeScript from '@babel/plugin-syntax-typescript'
import syntaxJSX from '@babel/plugin-syntax-jsx'
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

  const isTypeScript = /\.[cm]?tsx?$/.test(this.resourcePath)

  const result = transformSync(source, {
    filename: this.resourcePath,
    // Parser-only plugins: JSX + TypeScript syntax so .tsx/.ts inputs parse
    // (and type annotations survive) — no transform is applied here. Passed
    // as VALUES, not string names: pnpm's isolated layout keeps them
    // unresolvable from @babel/core's own module path.
    plugins: ([
      isTypeScript ? [syntaxTypeScript, { isTSX: true }] : null,
      syntaxJSX,
      [neutronWindPlugin, { ...rawOptions, platform }],
    ] as Array<typeof syntaxJSX | [typeof syntaxTypeScript, unknown] | [typeof neutronWindPlugin, unknown]>).filter(
      (entry): entry is typeof syntaxJSX | [typeof syntaxTypeScript, unknown] | [typeof neutronWindPlugin, unknown] => entry !== null,
    ),
    // The app's own Babel config (if any) still runs in ITS pipeline stage;
    // this stage is deliberately isolated so class resolution is hermetic.
    configFile: false,
    babelrc: false,
    sourceType: 'unambiguous',
    sourceMaps: true,
    // Keep the output in the same syntax family as the input; the loader
    // hands the still-JSX/TS source (with className rewritten) downstream.
    retainLines: false,
  })

  if (!result) {
    this.callback(null, source)
    return
  }

  this.callback(null, result.code ?? source, result.map)
}
