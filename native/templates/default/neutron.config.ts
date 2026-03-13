import { defineConfig } from '@neutron/native/config'

export default defineConfig({
  // ── Identity ──────────────────────────────────────────────────────────────
  name: '{{name}}',
  bundleId: 'com.example.{{name}}',
  version: '1.0.0',
  buildNumber: 1,

  // ── Assets ────────────────────────────────────────────────────────────────
  icon: './assets/icon.png',
  splash: {
    image: './assets/splash.png',
    backgroundColor: '#ffffff',
    resizeMode: 'contain',
  },

  // ── Fonts ─────────────────────────────────────────────────────────────────
  // fonts: ['./assets/fonts/Inter.ttf'],

  // ── Permissions ───────────────────────────────────────────────────────────
  // Uncomment the permissions your app needs.
  // permissions: {
  //   camera: 'Used for profile photos',
  //   location: 'Used for nearby search',
  //   notifications: 'Used for order updates',
  // },

  // ── Plugins ───────────────────────────────────────────────────────────────
  // Install optional @neutron/native-* packages and list them here.
  // plugins: [
  //   '@neutron/native-camera',
  //   '@neutron/native-location',
  //   ['@neutron/native-notifications', { mode: 'production' }],
  // ],

  // ── OTA updates ───────────────────────────────────────────────────────────
  // ota: {
  //   endpoint: 'https://updates.example.com',
  //   publicKey: process.env.OTA_PUBLIC_KEY,
  //   updateStrategy: 'next-launch',
  //   channel: 'production',
  // },

  // ── Deep linking ─────────────────────────────────────────────────────────
  scheme: '{{name}}',
  // associatedDomains: ['applinks:example.com'],

  // ── Dev server ────────────────────────────────────────────────────────────
  dev: {
    port: 8081,
  },
})
