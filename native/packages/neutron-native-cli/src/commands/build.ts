import { execa } from 'execa'
import pc from 'picocolors'
import { mkdir } from 'node:fs/promises'
import { resolve } from 'node:path'
import { pathExists } from '../utils.js'

interface BuildOptions {
  ios?: boolean
  android?: boolean
  release?: boolean
  out: string
  config?: string
}

export async function build(opts: BuildOptions): Promise<void> {
  const platform = opts.ios ? 'ios' : 'android'
  const mode = opts.release ? 'release' : 'debug'

  console.log(pc.cyan('Neutron Native') + pc.dim(` — building ${platform} (${mode})`))

  await mkdir(opts.out, { recursive: true })

  // Detect whether the project uses rspack.config.js (Re.Pack) or falls back
  // to Metro (react-native bundle).
  const rspackConfig = resolve(process.cwd(), opts.config ?? 'rspack.config.js')
  const useRepack = await pathExists(rspackConfig)

  if (useRepack) {
    // Re.Pack bundle via @callstack/repack
    await execa(
      'node_modules/.bin/rspack',
      [
        'bundle',
        '--config', rspackConfig,
        '--mode', opts.release ? 'production' : 'development',
      ],
      {
        stdio: 'inherit',
        env: {
          ...process.env,
          NODE_ENV: opts.release ? 'production' : 'development',
          PLATFORM: platform,
          NEUTRON_NATIVE: '1',
          REPACK_OUTPUT_PATH: resolve(process.cwd(), opts.out),
        },
      }
    )
  } else {
    // Fallback: Metro bundler (plain React Native project)
    await execa(
      'node_modules/.bin/react-native',
      [
        'bundle',
        '--platform', platform,
        '--dev', opts.release ? 'false' : 'true',
        '--entry-file', 'index.js',
        '--bundle-output', `${opts.out}/main.jsbundle`,
        '--assets-dest', opts.out,
      ],
      {
        stdio: 'inherit',
        env: {
          ...process.env,
          NEUTRON_NATIVE: '1',
          NODE_ENV: opts.release ? 'production' : 'development',
        },
      }
    )
  }

  console.log(pc.green('✓') + ` Bundle written to ${pc.bold(opts.out)}/`)
}
