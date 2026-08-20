import { describe, it, expect } from 'vitest'
import { isRlsDenied, friendlyError, RLS_EXPLANATION, RLS_FIX_SQL } from './rls'

// The engine's denial text (nucleus/src/executor/scalar_fns.rs):
// "<FN> is unavailable while row-level security is active because this
//  specialty-store surface has no policy-aware access path"
const ENGINE_DENIAL =
  "KV_GET is unavailable while row-level security is active because this specialty-store surface has no policy-aware access path"

describe('isRlsDenied', () => {
  it('should recognize the engine specialty-store denial', () => {
    expect(isRlsDenied(ENGINE_DENIAL)).toBe(true)
  })

  it('should be case-insensitive', () => {
    expect(isRlsDenied('DOC_COUNT Is Unavailable While Row-Level Security Is Active')).toBe(true)
  })

  it('should not match ordinary errors', () => {
    expect(isRlsDenied('relation "sales" does not exist')).toBe(false)
    expect(isRlsDenied('permission denied for table invoices')).toBe(false)
    expect(isRlsDenied('')).toBe(false)
    expect(isRlsDenied(undefined)).toBe(false)
    expect(isRlsDenied(null)).toBe(false)
  })
})

describe('friendlyError', () => {
  it('should replace the RLS denial with the explanation', () => {
    expect(friendlyError(ENGINE_DENIAL)).toBe(RLS_EXPLANATION)
  })

  it('should pass ordinary errors through unchanged', () => {
    expect(friendlyError('relation "sales" does not exist')).toBe('relation "sales" does not exist')
  })

  it('should render null/undefined as empty string', () => {
    expect(friendlyError(null)).toBe('')
    expect(friendlyError(undefined)).toBe('')
  })
})

describe('RLS remediation', () => {
  // The user hitting this denial has no path forward unless the notice shows
  // the exact statements the engine accepts. Both are real Nucleus DDL:
  // ALTER TABLE ... DISABLE ROW LEVEL SECURITY (executor/ddl.rs) and
  // DROP POLICY (executor/policy.rs). No SHOW POLICIES surface exists, so the
  // table/policy names are placeholders.
  it('should show the exact SQL that restores specialty-store access', () => {
    expect(RLS_FIX_SQL).toContain('ALTER TABLE <table> DISABLE ROW LEVEL SECURITY;')
    expect(RLS_FIX_SQL).toContain('DROP POLICY <name> ON <table>;')
  })

  it('should tell the user how to find the names the other statements need', () => {
    // The placeholders are useless without a way to resolve them. pg_policies
    // is served from the live RLS engine (executor/mod.rs), so it lists every
    // active policy and its table — the notice claimed no such surface existed.
    expect(RLS_FIX_SQL).toContain('SELECT * FROM pg_policies;')
    expect(RLS_EXPLANATION).toMatch(/pg_policies/)
  })

  it('should name the missing grant — superuser bypass, not a policy grant', () => {
    expect(RLS_EXPLANATION).toMatch(/superuser/i)
    expect(RLS_EXPLANATION).toMatch(/no CREATE POLICY can open them/i)
  })

  it('should carry the fix SQL inside the toast-length explanation too', () => {
    expect(RLS_EXPLANATION).toContain('ALTER TABLE <table> DISABLE ROW LEVEL SECURITY')
    expect(RLS_EXPLANATION).toContain('DROP POLICY <name> ON <table>')
  })
})
