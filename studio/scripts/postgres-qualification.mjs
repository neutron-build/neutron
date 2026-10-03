#!/usr/bin/env node
// Qualify an already-built, embedded local Studio against disposable PostgreSQL.
// Never run the package's unrestricted specialty-model live suites against PG.
// node scripts/postgres-qualification.mjs --cli /absolute/neutron --out /owned/report.json
// Requires both NEUTRON_E2E_DATABASE_URL (Go API fixtures) and
// NEUTRON_TEST_DATABASE_URL (packaged browser fixture), psql, Chrome and Go.
import { spawn } from 'node:child_process'
import { writeFileSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const args = process.argv.slice(2)
const option = name => { const i = args.indexOf(name); return i < 0 ? null : args[i + 1] }
const cli = option('--cli')
const out = option('--out')
if (!cli || !path.isAbsolute(cli) || !out || !process.env.NEUTRON_E2E_DATABASE_URL || !process.env.NEUTRON_TEST_DATABASE_URL) {
  console.error('An absolute --cli, --out and both disposable PostgreSQL URL environment variables are required.')
  process.exit(2)
}
const studio = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const tests = [
  'TestStudioUserJourneyV2E2E', 'TestStudioRowProtocolV2E2E',
  'TestStudioCommitProtocolV2E2E', 'TestStudioSameRowChainE2E',
  'TestStudioDataEditorS03E2E', 'TestStudioSQLEditorE2E',
  'TestStudioSchemaNavDiagnosticsE2E', 'TestStudioLargeDataS06E2E',
  'TestStudioS07RuleTablesRefuseEditsWith4xx', 'TestStudioS07InvalidPrimaryIndexIsNoKey',
  'TestStudioS07LegacyRowEndpointsRetired', 'TestStudioS07DottedRenameTarget',
  'TestStudioS07MigrationManagedRefusalNamesRenames',
  'TestStudioM08ColumnOrderPlanE2E', 'TestStudioQ11RenameWithUnrelatedDropsE2E',
  'TestStudioX14OwnershipE2E', 'TestStudioX06JourneyPostgresE2E',
]
const report = { profile: 'local-studio-postgresql', cli, requiredTests: tests, stages: [], pass: false }
// Do not retain raw command output: connection failures may contain credentials.
function run(command, argv, cwd, parseGo = false) {
  return new Promise(resolve => {
    const child = spawn(command, argv, { cwd, env: { ...process.env, NEUTRON_LIVE_REQUIRED: '1' }, stdio: ['ignore', 'pipe', 'pipe'] })
    const events = []
    let partial = ''
    child.stdout.on('data', chunk => {
      if (!parseGo) return
      partial += chunk.toString()
      let nl
      while ((nl = partial.indexOf('\n')) >= 0) {
        const line = partial.slice(0, nl); partial = partial.slice(nl + 1)
        try {
          const e = JSON.parse(line)
          if (e.Test && ['pass', 'skip', 'fail'].includes(e.Action)) events.push({ test: e.Test, action: e.Action })
        } catch { /* Non-JSON output cannot establish test completion. */ }
      }
    })
    child.stderr.resume()
    child.on('error', () => resolve({ exitCode: null, launchFailed: true, events }))
    child.on('close', code => resolve({ exitCode: code, events }))
  })
}
try {
  const live = await run('go', ['test', '-json', '-count=1', '-timeout=15m', './internal/studio', '-run', `^(${tests.join('|')})$`], path.join(studio, '..', 'cli'), true)
  const passed = new Set(live.events.filter(e => e.action === 'pass').map(e => e.test))
  const missing = tests.filter(t => !passed.has(t))
  const skips = live.events.filter(e => e.action === 'skip')
  const livePass = live.exitCode === 0 && missing.length === 0 && skips.length === 0
  report.stages.push({ name: 'native-postgresql-api', ...live, missing, pass: livePass })
  if (!livePass) throw new Error('Native PostgreSQL API qualification failed or skipped required coverage.')
  for (const script of ['embed-gate.mjs', 'journey.mjs']) {
    const result = await run(process.execPath, [path.join(studio, 'scripts', script), '--cli', cli, '--out', `${out}.${script}.json`], studio)
    report.stages.push({ name: script, exitCode: result.exitCode, pass: result.exitCode === 0 })
    if (result.exitCode !== 0) throw new Error(`Packaged ${script} qualification failed.`)
  }
  report.pass = true
} catch (err) {
  report.failure = err.message
} finally {
  writeFileSync(out, JSON.stringify(report, null, 2) + '\n', { mode: 0o600 })
}
console.error(report.pass ? 'PASS: local Studio PostgreSQL qualification' : 'FAIL: local Studio PostgreSQL qualification; inspect the credential-free report')
process.exitCode = report.pass ? 0 : 1
