import { defineConfig } from '@neutron/native/config'

export default defineConfig({
  name: 'Hello World',
  bundleId: 'com.neutron.helloworld',
  version: '1.0.0',
  buildNumber: 1,

  scheme: 'helloworld',

  dev: {
    port: 8081,
  },
})
