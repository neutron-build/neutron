import { execa } from 'execa'
import pc from 'picocolors'

interface RunIOSOptions {
  simulator: string
  release?: boolean
}

interface RunAndroidOptions {
  device?: string
  release?: boolean
}

export async function runIOS(opts: RunIOSOptions): Promise<void> {
  console.log(pc.cyan('Neutron Native') + pc.dim(` — launching iOS simulator: ${opts.simulator}`))

  const args = [
    'run-ios',
    '--simulator', opts.simulator,
  ]
  if (opts.release) args.push('--configuration', 'Release')

  await execa('node_modules/.bin/react-native', args, {
    stdio: 'inherit',
    env: { ...process.env, NEUTRON_NATIVE: '1' },
  })
}

export async function runAndroid(opts: RunAndroidOptions): Promise<void> {
  console.log(pc.cyan('Neutron Native') + pc.dim(' — launching Android'))

  const args = ['run-android']
  if (opts.device) args.push('--deviceId', opts.device)
  if (opts.release) args.push('--variant', 'release')

  await execa('node_modules/.bin/react-native', args, {
    stdio: 'inherit',
    env: { ...process.env, NEUTRON_NATIVE: '1' },
  })
}
