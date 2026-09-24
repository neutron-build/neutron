import type {
  Connection, ConnectionInput, TestResult,
  Schema, NucleusFeatures, QueryResult,
  ColumnDetail, IndexDetail, SavedQuery, FKDetail,
  TableMeta, MutationOutcome, KeyCell,
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
  conflict?: boolean
  missing?: boolean
  currentVersion?: string

  constructor(status: number, message: string, extra?: { conflict?: boolean; missing?: boolean; currentVersion?: string }) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.conflict = extra?.conflict
    this.missing = extra?.missing
    this.currentVersion = extra?.currentVersion
  }
}

/** Session token for mutating requests (CSRF-class, per server launch). */
let sessionToken: string | null = null

/** Test hook: reset the cached session token. */
export function _setSessionTokenForTests(token: string | null) {
  sessionToken = token
}

async function fetchSessionToken(): Promise<string> {
  try {
    const res = await fetch(BASE + '/session')
    if (res.ok) {
      const body = await res.json() as { token?: string }
      return body.token ?? ''
    }
  } catch {
    // Server unreachable or a pre-session build: no token to present.
  }
  return ''
}

/**
 * Headers for a mutating request: JSON content type plus the session token.
 * The token is fetched lazily once per page load from the same-origin
 * /api/session endpoint; an older server without it simply sees no header.
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
  const headers = await mutationHeaders()
  const res = await fetch(BASE + path, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  if (!res.ok) {
    throw await toApiError(res)
  }
  return res.json() as Promise<T>
}

async function toApiError(res: Response): Promise<ApiError> {
  let text = ''
  try {
    const raw = await res.text()
    try {
      const parsed = JSON.parse(raw) as { error?: string; conflict?: boolean; missing?: boolean; currentVersion?: string }
      if (parsed && typeof parsed === 'object' && typeof parsed.error === 'string') {
        return new ApiError(res.status, parsed.error, parsed)
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
  const result = await request<QueryResult>(method, path, body)
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
      request<TestResult>('POST', '/connections/test', { url }),

    connect: (id: string) =>
      mutationRequest<{ features: NucleusFeatures; schema: Schema }>('POST', `/connections/${id}/connect`),
  },

  // --- Query (arbitrary SQL; mutating, so it carries the session token) ---

  query: (sql: string, connectionId: string, params?: unknown[]) =>
    requestQueryResult('POST', '/query', { sql, connectionId, params }),

  // --- Schema ---

  schema: (connectionId: string) =>
    request<Schema>('GET', `/schema?connectionId=${connectionId}`),

  // --- Features ---

  features: (connectionId: string) =>
    request<NucleusFeatures>('GET', `/features?connectionId=${connectionId}`),

  // --- Table data (paginated, filterable, sortable) ---

  tableData: (
    connectionId: string, schema: string, table: string,
    limit = 200, offset = 0,
    filter?: { column: string; op: string; value?: string },
    sort?: { column: string; dir: 'asc' | 'desc' },
  ) => {
    const params = new URLSearchParams({
      connectionId, schema, table,
      limit: String(limit), offset: String(offset),
    })
    if (filter) {
      params.set('filterColumn', filter.column)
      params.set('filterOp', filter.op)
      if (filter.value !== undefined) params.set('filterValue', filter.value)
    }
    if (sort) {
      params.set('sortColumn', sort.column)
      params.set('sortDir', sort.dir)
    }
    return requestQueryResult('GET', `/table?${params.toString()}`)
  },

  // --- S01 typed row identities (v2 mutation protocol) ---

  tableMeta: (connectionId: string, schema: string, table: string) =>
    request<TableMeta>('GET',
      `/table/v2/meta?connectionId=${connectionId}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`),

  tableInsert: (input: {
    connectionId: string; schema: string; table: string
    /** Column -> cell. Omitted columns request DEFAULT; null is SQL NULL. */
    values: Record<string, unknown>
  }) => mutationRequest<MutationOutcome>('POST', '/table/v2/insert', input),

  tableUpdateV2: (input: {
    connectionId: string; schema: string; table: string
    key: KeyCell[]
    version: string
    column: string
    value?: unknown
    isNull?: boolean
  }) => mutationRequest<MutationOutcome>('POST', '/table/v2/update', input),

  tableDeleteV2: (input: {
    connectionId: string; schema: string; table: string
    key: KeyCell[]
    version: string
  }) => mutationRequest<MutationOutcome>('POST', '/table/v2/delete', input),

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
