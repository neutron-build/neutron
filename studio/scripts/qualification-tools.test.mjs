import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { redactDiagnostics, runStage } from './qualification-tools.mjs'

test('diagnostics redact URL, decoded secret echoes and quoted DSN passwords', () => {
  const url = 'postgres://u:secret%20value@localhost/db?token=secret-token'
  const safe = redactDiagnostics(`failure ${url}\nsecret value secret-token\npassword='other secret'\npostgres://other:unknown@db/db`, { NEUTRON_E2E_DATABASE_URL: url })
  for (const value of [url, 'secret value', 'secret-token', 'other secret', 'unknown']) assert.equal(safe.includes(value), false)
  assert.match(safe, /failure/)
})

test('stage captures sanitized diagnostic evidence and terminal Go events', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'studio-qualification-test-'))
  try {
    const diagnosticPath = path.join(dir, 'stage.log')
    const result = await runStage(process.execPath, ['-e', `console.log(JSON.stringify({Action:'pass',Test:'SelectedTest'})); console.error('password=secret_value');`], { cwd: dir, diagnosticPath, parseGo: true, timeoutMs: 5000 })
    assert.equal(result.exitCode, 0)
    assert.deepEqual(result.events, [{ test: 'SelectedTest', action: 'pass' }])
    assert.equal(readFileSync(diagnosticPath, 'utf8').includes('secret_value'), false)
  } finally { rmSync(dir, { recursive: true, force: true }) }
})

test('stage deadline terminates a nonsettling child and records timeout', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'studio-qualification-test-'))
  try {
    const result = await runStage(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { cwd: dir, diagnosticPath: path.join(dir, 'stage.log'), timeoutMs: 100 })
    assert.equal(result.timedOut, true)
    assert.notEqual(result.exitCode, 0)
  } finally { rmSync(dir, { recursive: true, force: true }) }
})
