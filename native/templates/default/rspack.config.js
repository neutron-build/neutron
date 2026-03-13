/**
 * Neutron Native — Re.Pack / Rspack bundler configuration.
 *
 * This file is the Rspack equivalent of metro.config.js.
 * Neutron reads your neutron.config.ts and merges it here automatically.
 * You only need to modify this file for advanced bundler customisation.
 *
 * Docs: https://re-pack.dev/docs/configuration/rspack-config
 */

'use strict'

const path = require('path')
const { RepackPlugin, EXTENSIONS } = require('@callstack/repack')

// Platform is passed via PLATFORM env var by the Neutron CLI.
const platform = process.env.PLATFORM

if (!platform) {
  throw new Error(
    'PLATFORM env var is required (set to "ios" or "android"). ' +
    'Run via neutron-native dev / neutron-native build.'
  )
}

/** @type {import('@rspack/core').Configuration} */
module.exports = {
  mode: process.env.NODE_ENV === 'production' ? 'production' : 'development',

  // ── Entry ─────────────────────────────────────────────────────────────────
  entry: './index.js',

  // ── Context ───────────────────────────────────────────────────────────────
  context: __dirname,

  // ── Output ────────────────────────────────────────────────────────────────
  output: {
    // Re.Pack controls the final path — this is the intermediate output dir.
    path: path.join(__dirname, 'dist', platform),
    filename: 'index.bundle',
    chunkFilename: '[name].chunk.bundle',
  },

  // ── Resolve ───────────────────────────────────────────────────────────────
  resolve: {
    /**
     * Platform-aware extension resolution order.
     * Re.Pack provides EXTENSIONS which includes .native, .ios, .android, etc.
     * Neutron adds .native.tsx / .native.ts before generic .tsx / .ts so
     * platform-split components are resolved first.
     */
    extensions: EXTENSIONS.map(ext =>
      ext === '.tsx' ? ['.native.tsx', ext] :
      ext === '.ts'  ? ['.native.ts',  ext] :
      [ext]
    ).flat(),

    alias: {
      /**
       * Preact compat aliases — critical for third-party libraries that
       * import 'react' or 'react-dom'. They transparently get preact/compat
       * instead, keeping bundle size at ~3KB rather than ~42KB.
       */
      'react':          'preact/compat',
      'react-dom':      'preact/compat',
      'react-dom/test-utils': 'preact/test-utils',
      'react/jsx-runtime':    'preact/jsx-runtime',

      // Absolute import alias for the project source root.
      '~': path.resolve(__dirname, 'src'),
    },
  },

  // ── Module rules ──────────────────────────────────────────────────────────
  module: {
    rules: [
      {
        // TypeScript + TSX — compiled by SWC (built into Rspack).
        test: /\.[jt]sx?$/,
        use: {
          loader: 'builtin:swc-loader',
          options: {
            jsc: {
              parser: { syntax: 'typescript', tsx: true },
              transform: {
                react: {
                  // Use Preact's JSX runtime instead of React's.
                  runtime: 'automatic',
                  importSource: 'preact',
                },
              },
            },
            env: {
              // Target Hermes (ES2019 + commonjs for RN compatibility).
              targets: 'defaults',
            },
          },
        },
        exclude: /node_modules/,
      },
      {
        // Image assets (PNG, JPG, GIF, WebP).
        test: /\.(png|jpe?g|gif|webp)$/i,
        type: 'asset/resource',
        generator: { filename: 'assets/images/[hash][ext]' },
      },
      {
        // Font assets.
        test: /\.(ttf|otf|woff2?)$/i,
        type: 'asset/resource',
        generator: { filename: 'assets/fonts/[name][ext]' },
      },
      {
        // SVG files — inline as React components via SVGR.
        // Requires: npm install @svgr/webpack
        // test: /\.svg$/i,
        // use: ['@svgr/webpack'],
      },
    ],
  },

  // ── Plugins ───────────────────────────────────────────────────────────────
  plugins: [
    /**
     * RepackPlugin — core Re.Pack integration.
     * Handles bundle splitting, asset processing, HMR, chunk loading.
     */
    new RepackPlugin({
      context: __dirname,
      platform,
      // Enable React Native Fast Refresh HMR in development.
      hmr: process.env.NODE_ENV !== 'production',
    }),

    // NeutronWind — className → StyleSheet.create transform.
    // Uncomment when @neutron/native-styling is installed.
    // new (require('@neutron/native-styling/rspack-plugin'))(),
  ],

  // ── Optimisation ──────────────────────────────────────────────────────────
  optimization: {
    minimize: process.env.NODE_ENV === 'production',
    // Split vendor chunks for Module Federation / OTA updates.
    splitChunks: {
      chunks: 'all',
      cacheGroups: {
        // Keep preact in its own chunk — it never changes between app updates.
        preact: {
          test: /[\\/]node_modules[\\/]preact[\\/]/,
          name: 'preact',
          priority: 20,
        },
        // Keep react-native in its own chunk.
        reactNative: {
          test: /[\\/]node_modules[\\/]react-native[\\/]/,
          name: 'react-native',
          priority: 10,
        },
      },
    },
  },

  // ── Dev server ────────────────────────────────────────────────────────────
  devServer: {
    port: parseInt(process.env.REPACK_DEV_SERVER_PORT ?? '8081', 10),
    host: process.env.REPACK_DEV_SERVER_HOST ?? 'localhost',
    hot: true,
  },

  // ── Source maps ───────────────────────────────────────────────────────────
  devtool: process.env.NODE_ENV === 'production' ? 'source-map' : 'eval-source-map',
}
