import { spawn } from 'node:child_process'
import { writeFileSync } from 'node:fs'

export function redactDiagnostics(text, env = process.env) {
  let safe = text
  for (const key of ['NEUTRON_E2E_DATABASE_URL', 'NEUTRON_TEST_DATABASE_URL']) {
    const raw = env[key]
    if (!raw) continue
    safe = safe.split(raw).join('[redacted database URL]')
    try {
      const u = new URL(raw)
      const secrets = [decodeURIComponent(u.password), ...[...u.searchParams].filter(([k]) => /password|token|secret/i.test(k)).map(([, v]) => v)]
      for (const secret of secrets.filter(Boolean).sort((a, b) => b.length - a.length)) safe = safe.split(secret).join('[redacted]')
    } catch { /* Malformed URI still replaced in full above. */ }
  }
  return safe.replace(/\b(?:postgres(?:ql)?|https?):\/\/[^\s"'<>]+/gi, '[redacted URL]')
    .replace(/(\b(?:password|token|secret)\s*=\s*)(?:'(?:\\.|[^'])*'|"(?:\\.|[^"])*"|[^\s]+)/gi, '$1[redacted]')
}

// Isolated process group: a deadline stops the command and its descendants.
// Raw diagnostics are bounded in memory, then redacted before disk publication.
export function runStage(command, argv, { cwd, diagnosticPath, env = process.env, timeoutMs = 16 * 60_000, parseGo = false }) {
  return new Promise(resolve => {
    const child = spawn(command, argv, { cwd, env, detached: process.platform !== 'win32', stdio: ['ignore', 'pipe', 'pipe'] })
    const events = []
    let partial = '', raw = '', timedOut = false, settled = false
    const signal = sig => { try { if (process.platform === 'win32') child.kill(sig); else if (child.pid) process.kill(-child.pid, sig) } catch { /* Already exited. */ } }
    const timer = setTimeout(() => { timedOut = true; signal('SIGKILL') }, timeoutMs)
    const collect = chunk => { raw += chunk.toString(); if (raw.length > 1_048_576) raw = raw.slice(-1_048_576) }
    child.stderr.on('data', collect)
    child.stdout.on('data', chunk => {
      collect(chunk)
      if (!parseGo) return
      partial += chunk.toString()
      let nl
      while ((nl = partial.indexOf('\n')) >= 0) {
        const line = partial.slice(0, nl); partial = partial.slice(nl + 1)
        try { const e = JSON.parse(line); if (e.Test && ['pass', 'skip', 'fail'].includes(e.Action)) events.push({ test: e.Test, action: e.Action }) } catch {}
      }
      if (partial.length > 1_048_576) partial = ''
    })
    const finish = (code, launchFailed = false) => {
      if (settled) return
      settled = true; clearTimeout(timer); signal('SIGKILL')
      writeFileSync(diagnosticPath, redactDiagnostics(raw, env), { mode: 0o600 })
      resolve({ exitCode: code, launchFailed, timedOut, events, diagnosticPath })
    }
    child.on('error', () => finish(null, true))
    child.on('close', code => finish(code))
  })
}
