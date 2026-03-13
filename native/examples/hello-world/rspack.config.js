/**
 * Hello World — Re.Pack / Rspack bundler config.
 * Inherits from the default Neutron Native bundler template.
 * No customisations needed for a bare-minimum app.
 */
'use strict'

const path = require('path')
const { RepackPlugin, EXTENSIONS } = require('@callstack/repack')

const platform = process.env.PLATFORM
if (!platform) throw new Error('PLATFORM env var required (ios | android)')

/** @type {import('@rspack/core').Configuration} */
module.exports = {
  mode: process.env.NODE_ENV === 'production' ? 'production' : 'development',
  entry: './index.js',
  context: __dirname,

  output: {
    path: path.join(__dirname, 'dist', platform),
    filename: 'index.bundle',
    chunkFilename: '[name].chunk.bundle',
  },

  resolve: {
    extensions: EXTENSIONS.map(ext =>
      ext === '.tsx' ? ['.native.tsx', ext] :
      ext === '.ts'  ? ['.native.ts',  ext] :
      [ext]
    ).flat(),

    alias: {
      'react':              'preact/compat',
      'react-dom':          'preact/compat',
      'react-dom/test-utils': 'preact/test-utils',
      'react/jsx-runtime':  'preact/jsx-runtime',
    },
  },

  module: {
    rules: [
      {
        test: /\.[jt]sx?$/,
        use: {
          loader: 'builtin:swc-loader',
          options: {
            jsc: {
              parser: { syntax: 'typescript', tsx: true },
              transform: {
                react: {
                  runtime: 'automatic',
                  importSource: 'preact',
                },
              },
            },
          },
        },
        exclude: /node_modules/,
      },
      {
        test: /\.(png|jpe?g|gif|webp)$/i,
        type: 'asset/resource',
        generator: { filename: 'assets/images/[hash][ext]' },
      },
    ],
  },

  plugins: [
    new RepackPlugin({
      context: __dirname,
      platform,
      hmr: process.env.NODE_ENV !== 'production',
    }),
  ],

  devServer: {
    port: parseInt(process.env.REPACK_DEV_SERVER_PORT ?? '8081', 10),
    host: process.env.REPACK_DEV_SERVER_HOST ?? 'localhost',
    hot: true,
  },

  devtool: process.env.NODE_ENV === 'production' ? 'source-map' : 'eval-source-map',
}
