import { ApiError } from './api'
import type { CommitOperation, CommitResponse } from './types'

/** Fake producer receipt mirrors the request identity and operation positions. */
export function commitReceiptFor(operationId: string, operations: readonly CommitOperation[], reversible = false): CommitResponse {
  return { operationId, rowsAffected: operations.length, operations: operations.map((operation, index) => ({ index, op: operation.op, rowsAffected: 1 })), reversible }
}

export function commitRefusalFor(error: ApiError, operationId: string): ApiError {
  return new ApiError(error.status, error.message, { state: error.state, auth: error.auth, body: { ...error.body, operationId, error: error.message, ...(error.state ? { state: error.state } : {}) } })
}
