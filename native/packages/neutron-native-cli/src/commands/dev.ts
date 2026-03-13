import { execa } from 'execa'
import pc from 'picocolors'
import { resolve } from 'node:path'
import { pathExists } from '../utils.js'

interface DevOptions {
  ios?: boolean
  android?: boolean
  port: string
  host: string
  config?: string
}

export async function dev(opts: DevOptions): Promise<void> {
  console.log(pc.cyan('Neutron Native') + pc.dim(' — starting dev server'))

  const platform = opts.ios ? 'ios' : opts.android ? 'android' : 'all'
  console.log(pc.dim(`  Platform: ${platform}  Port: ${opts.port}  Host: ${opts.host}`))

  // Detect Re.Pack (rspack.config.js) vs Metro.
  const rspackConfig = resolve(process.cwd(), opts.config ?? 'rspack.config.js')
  const useRepack = await pathExists(rspackConfig)

  if (useRepack) {
    console.log(pc.dim('  Bundler: Re.Pack (Rspack)'))
  } else {
    console.log(pc.dim('  Bundler: Metro (fallback — add rspack.config.js to use Re.Pack)'))
  }

  await execa(
    'node_modules/.bin/react-native',
    ['start', '--port', opts.port, '--host', opts.host],
    {
      stdio: 'inherit',
      env: {
        ...process.env,
        NEUTRON_NATIVE: '1',
        NODE_ENV: 'development',
        // Re.Pack reads PLATFORM to resolve .native.tsx / .ios.tsx etc.
        PLATFORM: platform === 'all' ? 'ios' : platform,
        REPACK_DEV_SERVER_PORT: opts.port,
        REPACK_DEV_SERVER_HOST: opts.host,
      },
    }
  ).catch((err: NodeJS.ErrnoException) => {
    if (err.code !== 'SIGINT' && err.code !== 'SIGTERM') {
      throw err
    }
  })
}
