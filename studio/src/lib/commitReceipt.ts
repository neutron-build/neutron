import type { CommitOperation, CommitResponse, OutcomeResponse } from './types'
import { isTaggedCell, decodeCell } from './wire'

/** An invalid receipt is ambiguous transport data, never a definitive refusal. */
export class InvalidCommitReceipt extends Error {
  constructor() { super('Invalid or mismatched commit receipt; check the original operation outcome'); this.name = 'InvalidCommitReceipt' }
}
const object = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v)
const count = (v: unknown): v is number => typeof v === 'number' && Number.isSafeInteger(v) && v >= 0
const optional = (v: Record<string, unknown>, key: string, type: 'string' | 'boolean') => v[key] === undefined || typeof v[key] === type
function cell(v: unknown): boolean {
  if (v === null || typeof v === 'string' || typeof v === 'boolean') return true
  if (typeof v === 'number') return Number.isFinite(v) && (!Number.isInteger(v) || Number.isSafeInteger(v))
  if (!isTaggedCell(v)) return false
  try { decodeCell(v); return true } catch { return false }
}
function invalid(): never { throw new InvalidCommitReceipt() }

/** Validate identity and safe result structure before discarding submitted IDs. */
export function validateCommitReceipt(value: unknown, operationId: string, submitted?: readonly CommitOperation[]): CommitResponse {
  if (!object(value) || value.operationId !== operationId || !count(value.rowsAffected) ||
      !Array.isArray(value.operations) || typeof value.reversible !== 'boolean' ||
      !optional(value, 'reversibleReason', 'string') || !optional(value, 'replayed', 'boolean') || !optional(value, 'reverted', 'string')) invalid()
  const operations = value.operations as unknown[]
  if (operations.length > 10000 || (submitted && operations.length !== submitted.length)) invalid()
  let total = 0
  operations.forEach((op, index) => {
    if (!object(op) || op.index !== index || !['insert', 'update', 'delete'].includes(String(op.op)) ||
        (submitted && op.op !== submitted[index].op) || !count(op.rowsAffected) || !optional(op, 'version', 'string')) invalid()
    if (op.key !== undefined) {
      if (!Array.isArray(op.key) || op.key.length === 0 || op.key.length > 1024) invalid()
      const names = new Set<string>()
      for (const key of op.key) {
        if (!object(key) || typeof key.column !== 'string' || !key.column || names.has(key.column) ||
            !Object.hasOwn(key, 'value') || !cell(key.value)) invalid()
        names.add(key.column)
      }
    }
    total += op.rowsAffected
    if (!Number.isSafeInteger(total)) invalid()
  })
  if (total !== value.rowsAffected) invalid()
  return value as unknown as CommitResponse
}

/** Error bodies also require the admitted operation identity before terminal refusal. */
export function validateCommitFailure(value: unknown, operationId: string): Record<string, unknown> {
  if (!object(value) || value.operationId !== operationId || typeof value.error !== 'string' || !value.error ||
      !optional(value, 'state', 'string') || !optional(value, 'auth', 'string') || !optional(value, 'currentVersion', 'string')) invalid()
  return value
}

export function validateCommitOutcome(value: unknown, operationId: string, submitted?: readonly CommitOperation[]): OutcomeResponse {
  if (!object(value) || value.operationId !== operationId || !['committed', 'failed', 'unknown', 'in_progress'].includes(String(value.state)) ||
      !optional(value, 'error', 'string') || !optional(value, 'reversible', 'boolean') || !optional(value, 'reversibleReason', 'string')) invalid()
  if (value.status !== undefined && (!count(value.status) || value.status < 100 || value.status > 599)) invalid()
  if (value.state === 'committed') {
    if (value.status !== undefined && (Number(value.status) < 200 || Number(value.status) >= 300)) invalid()
    validateCommitReceipt(value.response, operationId, submitted)
  } else if (value.state === 'failed') {
    // Some older servers omit nested failure identity. Do not release a private
    // batch on that weaker envelope: retain it for inspection/recovery.
    if (!count(value.status) || value.status < 400) invalid()
    const refusal = validateCommitFailure(value.response, operationId)
    return { ...value, error: refusal.error } as unknown as OutcomeResponse
  } else if (value.response !== undefined) invalid()
  return value as unknown as OutcomeResponse
}
