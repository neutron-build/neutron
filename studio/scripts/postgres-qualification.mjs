#!/usr/bin/env node
// Qualify an already-built, embedded local Studio against disposable PostgreSQL.
// Never run the package's unrestricted specialty-model live suites against PG.
// node scripts/postgres-qualification.mjs --cli /absolute/neutron --out /owned/report.json
// Requires NEUTRON_E2E_DATABASE_URL and Go. Browser journeys are opt-in
// through --browser-journeys, additionally requiring NEUTRON_TEST_DATABASE_URL,
// psql and Chrome. The default leaves packaged UI verification to T3 preview.
import { runStage } from './qualification-tools.mjs'
import { writeFileSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const args = process.argv.slice(2)
const option = name => { const i = args.indexOf(name); return i < 0 ? null : args[i + 1] }
const cli = option('--cli')
const out = option('--out')
const browserJourneys = args.includes('--browser-journeys')
if (!cli || !path.isAbsolute(cli) || !out || !process.env.NEUTRON_E2E_DATABASE_URL || (browserJourneys && !process.env.NEUTRON_TEST_DATABASE_URL)) {
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
function run(command, argv, cwd, parseGo = false) {
  return runStage(command, argv, { cwd, parseGo,
    env: { ...process.env, NEUTRON_LIVE_REQUIRED: '1' },
    diagnosticPath: `${out}.stage-${report.stages.length}.redacted.log`,
    timeoutMs: parseGo ? 16 * 60_000 : 5 * 60_000,
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
  for (const script of browserJourneys ? ['embed-gate.mjs', 'journey.mjs'] : []) {
    const result = await run(process.execPath, [path.join(studio, 'scripts', script), '--cli', cli, '--out', `${out}.${script}.json`], studio)
    report.stages.push({ name: script, ...result, pass: result.exitCode === 0 && !result.timedOut })
    if (result.exitCode !== 0) throw new Error(`Packaged ${script} qualification failed.`)
  }
  report.pass = true
  report.browserQualification = browserJourneys ? 'passed' : 'pending separate T3 preview verification'
} catch (err) {
  report.failure = err.message
} finally {
  writeFileSync(out, JSON.stringify(report, null, 2) + '\n', { mode: 0o600 })
}
console.error(report.pass ? `PASS: local Studio PostgreSQL API qualification; browser ${report.browserQualification}` : 'FAIL: local Studio PostgreSQL qualification; inspect the credential-free report')
process.exitCode = report.pass ? 0 : 1
