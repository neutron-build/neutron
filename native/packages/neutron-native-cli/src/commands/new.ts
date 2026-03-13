import { execa } from 'execa'
import { cp, writeFile, readFile } from 'node:fs/promises'
import { join, resolve, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import pc from 'picocolors'

const __filename = fileURLToPath(import.meta.url)
const __dirnameCur = dirname(__filename)
const TEMPLATES_DIR = resolve(__dirnameCur, '../../../templates')

interface NewOptions {
  template: string
}

export async function newProject(name: string, opts: NewOptions): Promise<void> {
  const dest = resolve(process.cwd(), name)
  const templateDir = join(TEMPLATES_DIR, opts.template)

  console.log(pc.cyan('Neutron Native') + pc.dim(` — scaffolding ${pc.bold(name)}`))

  // Copy template
  await cp(templateDir, dest, { recursive: true })

  // Replace {{name}} placeholders in template files.
  const filesToPatch = ['package.json', 'neutron.config.ts', 'index.js']
  for (const file of filesToPatch) {
    const filePath = join(dest, file)
    try {
      const contents = await readFile(filePath, 'utf8')
      await writeFile(filePath, contents.split('{{name}}').join(name))
    } catch {
      // File may not exist for some templates — skip silently.
    }
  }

  console.log('')
  console.log(pc.green('✓') + ` Created ${pc.bold(name)}`)
  console.log('')
  console.log('  Next steps:')
  console.log(pc.dim(`    cd ${name}`))
  console.log(pc.dim('    npm install'))
  console.log(pc.dim('    neutron-native dev --ios'))
  console.log('')

  // Check for required tools
  await _checkTools()
}

async function _checkTools(): Promise<void> {
  const checks = [
    { cmd: 'xcode-select', args: ['-p'], label: 'Xcode', platform: 'darwin' },
    { cmd: 'adb', args: ['version'], label: 'Android SDK (adb)', platform: 'all' },
  ]

  for (const check of checks) {
    if (check.platform !== 'all' && process.platform !== check.platform) continue
    try {
      await execa(check.cmd, check.args, { stdio: 'pipe' })
    } catch {
      console.log(pc.yellow('!') + ` ${check.label} not found — you may need to install it`)
    }
  }
}
