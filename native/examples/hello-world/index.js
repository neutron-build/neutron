/**
 * Hello World — Neutron Native entry point.
 *
 * React Native requires this file at the project root.
 * It mounts the Preact component tree into the native app shell.
 */
import { NeutronApp } from '@neutron/native'
import App from './app/_layout'

NeutronApp({
  component: App,
  appName: 'Hello World',
})
