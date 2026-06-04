/**
 * Neutron Native app entry point.
 * React Native requires this file at the project root.
 */
import { NeutronApp } from '@neutron-build/native'
import App from './app/_layout'

NeutronApp({
  component: App,
  appName: '{{name}}',
})
