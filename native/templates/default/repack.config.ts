/**
 * Re.Pack 5 (Rspack) configuration for Neutron Native.
 *
 * Re.Pack replaces Metro as the bundler, giving us:
 *   - 5× faster builds via Rspack (Rust-based)
 *   - Module Federation v2 for OTA updates
 *   - Hot Module Replacement compatible with Hermes V1
 *   - Full source map support
 */

import path from 'node:path'
import { defineConfig } from '@re-pack/dev-server'

export default defineConfig({
  context: __dirname,

  entry: {
    main: './index.js',
  },

  resolve: {
    alias: {
      '@': path.resolve(__dirname, 'app'),
    },
    // Platform extension resolution: .native.tsx > .tsx > .jsx > .js
    extensions: [
      '.native.tsx', '.native.ts', '.native.jsx', '.native.js',
      '.tsx', '.ts', '.jsx', '.js',
    ],
  },

  module: {
    rules: [
      {
        // TypeScript + JSX via SWC (faster than Babel for main transform)
        test: /\.(tsx?|jsx?)$/,
        exclude: /node_modules\/(?!@neutron)/,
        use: [
          // NeutronWind: className → StyleSheet (must run before SWC)
          {
            loader: '@neutron/native-styling/rspack',
          },
          {
            loader: 'builtin:swc-loader',
            options: {
              jsc: {
                parser: { syntax: 'typescript', tsx: true },
                transform: {
                  react: {
                    runtime: 'automatic',
                    importSource: 'react',
                  },
                },
                target: 'es2020',
              },
            },
          },
        ],
      },
      {
        // Native asset handling (images, fonts)
        test: /\.(png|jpe?g|gif|svg|ttf|otf|woff2?)$/,
        type: 'asset/resource',
      },
    ],
  },

  optimization: {
    // Split vendor chunks for faster OTA updates via Module Federation
    splitChunks: {
      chunks: 'all',
      cacheGroups: {
        react: {
          test: /node_modules\/react(?:-native)?/,
          name: 'react',
          priority: 20,
        },
      },
    },
  },
})
