#!/usr/bin/env node
/**
 * Neutron Native CLI
 *
 * Commands:
 *   neutron-native dev [--ios | --android]  Start Re.Pack dev server + Metro bridge
 *   neutron-native build [--ios | --android] [--release]  Bundle for distribution
 *   neutron-native run-ios    Launch iOS simulator
 *   neutron-native run-android  Launch Android emulator
 *   neutron-native new <name>  Scaffold a new Neutron Native project
 */

import { cac } from 'cac'
import pc from 'picocolors'
import { dev } from './commands/dev.js'
import { build } from './commands/build.js'
import { runIOS, runAndroid } from './commands/run.js'
import { newProject } from './commands/new.js'

const cli = cac('neutron-native')

cli
  .command('dev', 'Start the Neutron Native development server')
  .option('--ios', 'Target iOS simulator')
  .option('--android', 'Target Android emulator')
  .option('--port <port>', 'Dev server port', { default: '8081' })
  .option('--host <host>', 'Dev server host', { default: 'localhost' })
  .option('--config <path>', 'Path to rspack.config.js', { default: 'rspack.config.js' })
  .action(async (opts: { ios?: boolean; android?: boolean; port: string; host: string; config: string }) => {
    await dev(opts)
  })

cli
  .command('build', 'Bundle the app for distribution')
  .option('--ios', 'Build for iOS')
  .option('--android', 'Build for Android')
  .option('--release', 'Build in release mode (default: debug)')
  .option('--out <dir>', 'Output directory', { default: 'dist' })
  .option('--config <path>', 'Path to rspack.config.js', { default: 'rspack.config.js' })
  .action(async (opts: { ios?: boolean; android?: boolean; release?: boolean; out: string; config: string }) => {
    await build(opts)
  })

cli
  .command('run-ios', 'Build and run on iOS simulator')
  .option('--simulator <name>', 'Simulator name', { default: 'iPhone 16' })
  .option('--release', 'Run in release mode')
  .action(async (opts: { simulator: string; release?: boolean }) => {
    await runIOS(opts)
  })

cli
  .command('run-android', 'Build and run on Android emulator')
  .option('--device <id>', 'Device ID (from adb devices)')
  .option('--release', 'Run in release mode')
  .action(async (opts: { device?: string; release?: boolean }) => {
    await runAndroid(opts)
  })

cli
  .command('new <name>', 'Scaffold a new Neutron Native project')
  .option('--template <name>', 'Template name', { default: 'default' })
  .action(async (name: string, opts: { template: string }) => {
    await newProject(name, opts)
  })

cli.help()
cli.version('0.1.0')

cli.parse()

// ─── Global error handler ─────────────────────────────────────────────────────

process.on('unhandledRejection', (err) => {
  console.error(pc.red('Error:'), err instanceof Error ? err.message : String(err))
  process.exit(1)
})
