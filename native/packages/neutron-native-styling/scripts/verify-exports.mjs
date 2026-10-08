/**
 * Verify every exports-map target resolves after a build (NF-NR-02).
 * Run as part of `pnpm build` verification in CI — fails if the package
 * maps import/require/types onto files that do not exist.
 */
import { readFileSync, existsSync } from 'node:fs'
import { resolve, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const pkg = JSON.parse(readFileSync(resolve(here, '../package.json'), 'utf8'))

const missing = []
for (const [subpath, target] of Object.entries(pkg.exports)) {
  for (const [condition, file] of Object.entries(target)) {
    if (condition === 'types' || condition === 'default') continue
    if (!file.startsWith('./')) continue
    if (!existsSync(resolve(here, '..', file))) missing.push(`${subpath} [${condition}] -> ${file}`)
  }
}
if (missing.length) {
  console.error('exports map targets missing after build:\n  ' + missing.join('\n  '))
  process.exit(1)
}
console.log('exports map verified: every import/require target exists')
