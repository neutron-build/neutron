import { defineConfig } from 'vite'
import preact from '@preact/preset-vite'

export default defineConfig({
  plugins: [preact()],

  resolve: {
    alias: {
      'react': 'preact/compat',
      'react-dom/test-utils': 'preact/test-utils',
      'react-dom': 'preact/compat',
      'react/jsx-runtime': 'preact/jsx-runtime',
    },
  },

  build: {
    outDir: 'dist',
    rollupOptions: {
      output: {
        manualChunks: {
          // Core: Preact + signals (always loaded)
          'preact': ['preact', '@preact/signals', 'preact/compat'],
          // SQL module: CodeMirror (lazy-loaded with sql module)
          'codemirror': [
            'codemirror',
            '@codemirror/lang-sql',
            '@codemirror/theme-one-dark',
            '@codemirror/view',
            '@codemirror/state',
          ],
          // Table: TanStack (loaded with SQL browser)
          'table': ['@tanstack/react-table'],
        },
      },
    },
  },

  // Dev: proxy /api/* to the Go server
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:4983',
        changeOrigin: true,
      },
    },
  },
})
