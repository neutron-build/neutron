import type {
  Connection, ConnectionInput, TestResult,
  Schema, NucleusFeatures, QueryResult,
  ColumnDetail, IndexDetail, SavedQuery, FKDetail,
  TableMeta, MutationOutcome, KeyCell, MatchCell, TableFilter, TableSort,
  CommitResponse, PreviewResponse, OutcomeResponse, CommitOperation,
  CancelQueryResponse, ExplainOutcome, ExplainPlan, ExplainRefusal,
  ExportFormat, ExportTicket, ImportBatchRequest, ImportBatchResponse, ImportOutcomeResponse,
} from './types'
import { decodeRows } from './wire'

const BASE = '/api'

/**
 * Error thrown for non-2xx responses. Carries the status and, when the
 * server's problem body provides them, the v2 mutation states (conflict /
 * missing / currentVersion) so callers can surface them explicitly.
 */
export class ApiError extends Error {
  status: number
  /** v2 outcome state: conflict | missing | binding | constraint | privilege. */
  state?: string
  /** Auth refusal code from the server boundary: origin | session. */
  auth?: string
  conflict?: boolean
  missing?: boolean
  /** The rows' relation binding is stale (reconnect or table replaced). */
  binding?: boolean
  currentVersion?: string
  /** The parsed JSON error body, for endpoints with richer refusals. */
  body?: Record<string, unknown>

  constructor(status: number, message: string, extra?: { state?: string; auth?: string; conflict?: boolean; missing?: boolean; currentVersion?: string; body?: Record<string, unknown> }) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.state = extra?.state
    this.auth = extra?.auth
    this.conflict = extra?.conflict
    this.missing = extra?.missing
    this.binding = extra?.state === undefined ? undefined : extra.state === 'binding'
    this.currentVersion = extra?.currentVersion
    this.body = extra?.body
  }
}

/** Session token for mutating requests (CSRF-class, per server launch). */
let sessionToken: string | null = null

/** Test hook: reset the cached session token. */
export function _setSessionTokenForTests(token: string | null) {
  sessionToken = token
}

async function fetchSessionToken(): Promise<string | null> {
  try {
    const res = await fetch(BASE + '/session')
    if (res.ok) {
      const body = await res.json() as { token?: string }
      return body.token || null
    }
  } catch {
    // Server unreachable: no token yet; the next mutation retries the fetch.
  }
  return null
}

/**
 * Headers for a mutating request: JSON content type plus the session token.
 * The token is fetched lazily from the same-origin /api/session endpoint and
 * cached; a failed fetch is not cached, so the next mutation tries again.
 */
export async function mutationHeaders(): Promise<Record<string, string>> {
  if (sessionToken === null) {
    sessionToken = await fetchSessionToken()
  }
  const headers: Record<string, string> = { 'Content-Type': 'application/json' }
  if (sessionToken) headers['X-Studio-Session'] = sessionToken
  return headers
}

async function request<T>(
  method: string,
  path: string,
  body?: unknown,
): Promise<T> {
  const res = await fetch(BASE + path, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  })
  if (!res.ok) {
    throw await toApiError(res)
  }
  return res.json() as Promise<T>
}

async function mutationRequest<T>(
  method: string,
  path: string,
  body?: unknown,
): Promise<T> {
  const payload = body === undefined ? undefined : JSON.stringify(body)
  let res = await fetch(BASE + path, { method, headers: await mutationHeaders(), body: payload })
  if (res.status === 403) {
    const err = await toApiError(res)
    // A stale token (the server restarted and minted a new one) is refused
    // before the handler runs, so one retry with a fresh token is safe.
    // Origin refusals are never retried.
    if (err.auth !== 'session') throw err
    sessionToken = null
    res = await fetch(BASE + path, { method, headers: await mutationHeaders(), body: payload })
  }
  if (!res.ok) {
    throw await toApiError(res)
  }
  if (res.status === 204) return undefined as T
  return res.json() as Promise<T>
}

async function toApiError(res: Response): Promise<ApiError> {
  let text = ''
  try {
    const raw = await res.text()
    try {
      const parsed = JSON.parse(raw) as {
        error?: string
        state?: string
        auth?: string
        currentVersion?: string
      }
      if (parsed && typeof parsed === 'object' && typeof parsed.error === 'string') {
        // The v2 protocol carries its outcome in "state": "conflict" |
        // "missing" | "binding" | "constraint" | "privilege"; the auth
        // boundary answers 403 with "auth": "origin" | "session".
        return new ApiError(res.status, parsed.error, {
          state: parsed.state,
          auth: parsed.auth,
          conflict: parsed.state === 'conflict',
          missing: parsed.state === 'missing',
          currentVersion: parsed.currentVersion,
          body: parsed as Record<string, unknown>,
        })
      }
      text = raw
    } catch {
      text = raw
    }
  } catch {
    // fall through to the status-only error
  }
  return new ApiError(res.status, text || `HTTP ${res.status}`)
}

/** Query results may carry tagged cells for bigint/decimal/binary/temporal
 *  values (lossless across JSON.parse); decode them, passthrough otherwise. */
async function requestQueryResult(method: string, path: string, body?: unknown): Promise<QueryResult> {
  const result = method === 'GET'
    ? await request<QueryResult>(method, path, body)
    : await mutationRequest<QueryResult>(method, path, body)
  if (Array.isArray(result.rows)) decodeRows(result.rows)
  return result
}

// --- Connections ---

export const api = {
  connections: {
    list: () =>
      request<Connection[]>('GET', '/connections'),

    add: (input: ConnectionInput) =>
      mutationRequest<Connection>('POST', '/connections', input),

    remove: (id: string) =>
      mutationRequest<void>('DELETE', `/connections/${id}`),

    test: (url: string) =>
      mutationRequest<TestResult>('POST', '/connections/test', { url }),

    connect: (id: string) =>
      mutationRequest<{ features: NucleusFeatures; schema: Schema }>('POST', `/connections/${id}/connect`),
  },

  // --- Query (arbitrary SQL; mutating, so it carries the session token) ---

  query: (sql: string, connectionId: string, params?: unknown[], requestId?: string) =>
    requestQueryResult('POST', '/query', { sql, connectionId, params, requestId }),

  /** Cancel a running editor statement server-side (pg_cancel_backend on
   *  the backend that runs it). 404 means it already finished. */
  cancelQuery: (connectionId: string, requestId: string) =>
    mutationRequest<CancelQueryResponse>('POST', '/query/cancel', { connectionId, requestId }),

  /** EXPLAIN a statement. analyze=false never executes it; analyze=true
   *  executes it read-only and rolled back unless allowWrites (then writes
   *  run and are still rolled back). Refusals (422) resolve as ok:false. */
  explain: async (input: {
    connectionId: string
    sql: string
    params?: unknown[]
    requestId?: string
    analyze: boolean
    allowWrites?: boolean
  }): Promise<ExplainOutcome> => {
    try {
      const plan = await mutationRequest<Omit<ExplainPlan, 'ok'>>('POST', '/query/explain', input)
      return { ...plan, ok: true }
    } catch (err) {
      if (err instanceof ApiError && err.status === 422 && err.body) {
        return { ...(err.body as Omit<ExplainRefusal, 'ok'>), ok: false }
      }
      throw err
    }
  },

  // --- Schema ---

  schema: (connectionId: string) =>
    request<Schema>('GET', `/schema?connectionId=${connectionId}`),

  // --- Features ---

  features: (connectionId: string) =>
    request<NucleusFeatures>('GET', `/features?connectionId=${connectionId}`),

  // --- Table data (paginated, multi-filterable, multi-sortable) ---

  tableData: (
    connectionId: string, schema: string, table: string,
    limit = 200, offset = 0,
    /** Multiple ANDed filters (S03); legacy single-filter callers pass one. */
    filters?: TableFilter[],
    sort?: { column: string; dir: 'asc' | 'desc' },
    /** Ordered multi-sort keys (S03); earlier keys take precedence. */
    sorts?: TableSort[],
    /** Full-tuple equality filter (composite FK follow); values are wire cells. */
    match?: MatchCell[],
  ) => {
    const params = new URLSearchParams({
      connectionId, schema, table,
      limit: String(limit), offset: String(offset),
    })
    if (filters && filters.length > 0) {
      params.set('filters', JSON.stringify(filters))
    }
    if (sort) {
      params.set('sortColumn', sort.column)
      params.set('sortDir', sort.dir)
    }
    if (sorts && sorts.length > 0) {
      params.set('sorts', JSON.stringify(sorts))
    }
    if (match && match.length > 0) {
      params.set('match', JSON.stringify(match))
    }
    return requestQueryResult('GET', `/table?${params.toString()}`)
  },

  // --- S01 typed row identities (v2 mutation protocol) ---

  tableMeta: (connectionId: string, schema: string, table: string) =>
    request<TableMeta>('GET',
      `/table/v2/meta?connectionId=${connectionId}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`),

  // X01: table search (vector similarity + full-text), read-only and
  // parameter-bound server-side.
  tableSearch: (input: {
    connectionId: string; schema: string; table: string
    kind: 'vector' | 'fts'
    column: string
    query: string
    operator?: 'l2' | 'cosine' | 'inner-product' | 'l1'
    config?: string
    limit?: number
  }) => mutationRequest<QueryResult>('POST', '/table/v2/search', input),

  tableInsert: (input: {
    connectionId: string; schema: string; table: string
    /** Relation binding from the table read — required by the server. */
    binding: string
    /** Column -> cell. Omitted columns request DEFAULT; null is SQL NULL. */
    values: Record<string, unknown>
  }) => mutationRequest<MutationOutcome>('POST', '/table/v2/insert', input),

  tableUpdateV2: (input: {
    connectionId: string; schema: string; table: string
    binding: string
    key: KeyCell[]
    version: string
    column: string
    value?: unknown
    isNull?: boolean
  }) => mutationRequest<MutationOutcome>('POST', '/table/v2/update', input),

  tableDeleteV2: (input: {
    connectionId: string; schema: string; table: string
    binding: string
    key: KeyCell[]
    version: string
  }) => mutationRequest<MutationOutcome>('POST', '/table/v2/delete', input),

  // --- S02 atomic staged commits and retry outcomes ---

  /** Commit one staged operation list atomically under an idempotency key.
   *  Same operationId + same payload replays the recorded outcome; same ID
   *  + different payload is a 409 operation_conflict; expired/evicted
   *  outcomes report unknown (never retried blindly). */
  commitOperations: (input: {
    connectionId: string
    operationId: string
    operations: CommitOperation[]
  }) => mutationRequest<CommitResponse>('POST', '/table/v2/commit', input),

  /** Dry-run the identical validation and execution, always rolled back. */
  previewOperations: (input: {
    connectionId: string
    operations: CommitOperation[]
  }) => mutationRequest<PreviewResponse>('POST', '/table/v2/preview', input),

  /** Resolve a recorded outcome for an operation ID (status lookup must
   *  precede any retry after a dropped response). */
  operationOutcome: (connectionId: string, operationId: string) =>
    mutationRequest<OutcomeResponse>('POST', '/table/v2/outcome', { connectionId, operationId }),

  /** Undo a committed operation list through its recorded inverse; honest
   *  refusal where the inverse cannot be exact. The revert is itself a
   *  deduplicated commit under revertOperationId. */
  revertOperation: (input: {
    connectionId: string
    operationId: string
    revertOperationId: string
  }) => mutationRequest<CommitResponse>('POST', '/table/v2/revert', input),

  // --- S06: streamed export and batched import ---

  /** Validate an export (session-guarded) and receive a single-use ticket;
   *  the browser then downloads `url` natively (streamed to disk). */
  tableExport: (input: {
    connectionId: string; schema: string; table: string
    format: ExportFormat
    filters?: TableFilter[]
    sorts?: TableSort[]
    match?: MatchCell[]
  }) => mutationRequest<ExportTicket>('POST', '/table/v2/export', input),

  /** Insert one import batch atomically (all rows or none). */
  importBatch: (input: ImportBatchRequest) =>
    mutationRequest<ImportBatchResponse>('POST', '/table/v2/import/batch', input),

  /** Resolve an import batch ID after a dropped response. */
  importOutcome: (connectionId: string, operationId: string) =>
    mutationRequest<ImportOutcomeResponse>('POST', '/table/v2/import/outcome', { connectionId, operationId }),

  // --- Interim v1 row endpoints (guarded, kept during the transition) ---

  tableUpdate: (input: {
    connectionId: string; schema: string; table: string
    pkColumn: string; pkValue: unknown
    column: string; value?: unknown; isNull?: boolean
  }) => mutationRequest<{ rowsAffected: number; error?: string }>('POST', '/table/update', input),

  tableDeleteRow: (input: {
    connectionId: string; schema: string; table: string
    pkColumn: string; pkValue: unknown
  }) => mutationRequest<{ rowsAffected: number; error?: string }>('POST', '/table/delete', input),

  tableFKs: (connectionId: string, schema: string, table: string) =>
    request<{ fks: FKDetail[]; error?: string }>('GET',
      `/table/fks?connectionId=${connectionId}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`
    ),

  // --- Schema designer ---

  columns: (connectionId: string, schema: string, table: string) =>
    request<{ columns: ColumnDetail[]; indexes: IndexDetail[] }>('GET',
      `/columns?connectionId=${connectionId}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`
    ),

  ddl: (connectionId: string, sql: string) =>
    mutationRequest<{ ok: boolean; duration: number; error?: string }>('POST', '/ddl', { connectionId, sql }),

  codegen: (connectionId: string, schema: string, table: string, lang: string) =>
    request<{ code: string }>('GET',
      `/codegen?connectionId=${connectionId}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}&lang=${lang}`
    ),

  // --- Saved queries ---

  savedQueries: {
    list: () =>
      request<SavedQuery[]>('GET', '/saved-queries'),

    save: (name: string, sql: string) =>
      mutationRequest<SavedQuery>('POST', '/saved-queries', { name, sql }),

    remove: (id: string) =>
      mutationRequest<void>('DELETE', `/saved-queries/${id}`),
  },
}
