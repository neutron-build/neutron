import { describe, it, expect } from 'vitest'
import { isRlsDenied, friendlyError, RLS_EXPLANATION } from './rls'

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
