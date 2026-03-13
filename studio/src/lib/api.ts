import type {
  Connection, ConnectionInput, TestResult,
  Schema, NucleusFeatures, QueryResult,
  ColumnDetail, IndexDetail, SavedQuery,
} from './types'

const BASE = '/api'

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
    const text = await res.text()
    throw new Error(text || `HTTP ${res.status}`)
  }
  return res.json() as Promise<T>
}

// --- Connections ---

export const api = {
  connections: {
    list: () =>
      request<Connection[]>('GET', '/connections'),

    add: (input: ConnectionInput) =>
      request<Connection>('POST', '/connections', input),

    remove: (id: string) =>
      request<void>('DELETE', `/connections/${id}`),

    test: (url: string) =>
      request<TestResult>('POST', '/connections/test', { url }),

    connect: (id: string) =>
      request<{ features: NucleusFeatures; schema: Schema }>('POST', `/connections/${id}/connect`),
  },

  // --- Query ---

  query: (sql: string, connectionId: string, params?: unknown[]) =>
    request<QueryResult>('POST', '/query', { sql, connectionId, params }),

  // --- Schema ---

  schema: (connectionId: string) =>
    request<Schema>('GET', `/schema?connectionId=${connectionId}`),

  // --- Features ---

  features: (connectionId: string) =>
    request<NucleusFeatures>('GET', `/features?connectionId=${connectionId}`),

  // --- Table data (paginated) ---

  tableData: (connectionId: string, schema: string, table: string, limit = 200, offset = 0) =>
    request<QueryResult>('GET',
      `/table?connectionId=${connectionId}&schema=${schema}&table=${table}&limit=${limit}&offset=${offset}`
    ),

  // --- Schema designer ---

  columns: (connectionId: string, schema: string, table: string) =>
    request<{ columns: ColumnDetail[]; indexes: IndexDetail[] }>('GET',
      `/columns?connectionId=${connectionId}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`
    ),

  ddl: (connectionId: string, sql: string) =>
    request<{ ok: boolean; duration: number; error?: string }>('POST', '/ddl', { connectionId, sql }),

  codegen: (connectionId: string, schema: string, table: string, lang: string) =>
    request<{ code: string }>('GET',
      `/codegen?connectionId=${connectionId}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}&lang=${lang}`
    ),

  // --- Saved queries ---

  savedQueries: {
    list: () =>
      request<SavedQuery[]>('GET', '/saved-queries'),

    save: (name: string, sql: string) =>
      request<SavedQuery>('POST', '/saved-queries', { name, sql }),

    remove: (id: string) =>
      request<void>('DELETE', `/saved-queries/${id}`),
  },
}
