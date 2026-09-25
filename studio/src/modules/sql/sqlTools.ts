// Pure helpers for the SQL editor (S04): parameter detection, statement
// classification hints, catalog-aware completion namespaces, EXPLAIN plan
// flattening and the per-connection query history.
//
// Classification here only drives UI hints. The server is authoritative:
// EXPLAIN runs in a rolled-back read-only transaction, and EXPLAIN ANALYZE
// refuses writes unless explicitly allowed.

import type { Completion } from '@codemirror/autocomplete'
import type { SQLNamespace } from '@codemirror/lang-sql'
import type { QueryHistoryEntry, SqlTable } from '../../lib/types'

// --- Lexical scan -----------------------------------------------------------

type Token = { kind: 'word'; text: string } | { kind: 'param'; index: number } | { kind: 'paren' }

const isIdentStart = (c: string) => /[A-Za-z_\u0080-￿]/.test(c)
const isIdentChar = (c: string) => /[A-Za-z0-9_$\u0080-￿]/.test(c)

/**
 * Tokenize the parts of a statement that are SQL, skipping comments (-- and
 * nested block comments), string literals (including E'' escapes), quoted
 * identifiers and dollar-quoted bodies. Yields keywords/identifiers and
 * positional parameters.
 */
function scan(sql: string): Token[] {
  const out: Token[] = []
  const n = sql.length
  let i = 0
  while (i < n) {
    const c = sql[i]
    const next = sql[i + 1]
    if (c === '-' && next === '-') {
      while (i < n && sql[i] !== '\n') i++
      continue
    }
    if (c === '/' && next === '*') {
      let depth = 0
      while (i < n) {
        if (sql[i] === '/' && sql[i + 1] === '*') { depth++; i += 2 }
        else if (sql[i] === '*' && sql[i + 1] === '/') { depth--; i += 2; if (depth === 0) break }
        else i++
      }
      continue
    }
    if (c === "'") {
      const prev = sql[i - 1]
      const escapes = (prev === 'E' || prev === 'e') && !isIdentChar(sql[i - 2] ?? ' ')
      i++
      while (i < n) {
        if (escapes && sql[i] === '\\') { i += 2; continue }
        if (sql[i] === "'") {
          if (sql[i + 1] === "'") { i += 2; continue }
          break
        }
        i++
      }
      i++
      continue
    }
    if (c === '"') {
      i++
      while (i < n) {
        if (sql[i] === '"') {
          if (sql[i + 1] === '"') { i += 2; continue }
          break
        }
        i++
      }
      i++
      continue
    }
    if (c === '$') {
      if (/[0-9]/.test(next ?? '') && !isIdentChar(sql[i - 1] ?? ' ')) {
        let j = i + 1
        while (j < n && /[0-9]/.test(sql[j])) j++
        out.push({ kind: 'param', index: Number(sql.slice(i + 1, j)) })
        i = j
        continue
      }
      const m = /^\$([A-Za-z_][A-Za-z0-9_]*)?\$/.exec(sql.slice(i))
      if (m && !isIdentChar(sql[i - 1] ?? ' ')) {
        const tag = m[0]
        const end = sql.indexOf(tag, i + tag.length)
        i = end < 0 ? n : end + tag.length
        continue
      }
      i++
      continue
    }
    if (c === '(') {
      out.push({ kind: 'paren' })
      i++
      continue
    }
    if (isIdentStart(c)) {
      let j = i + 1
      while (j < n && isIdentChar(sql[j])) j++
      out.push({ kind: 'word', text: sql.slice(i, j).toUpperCase() })
      i = j
      continue
    }
    i++
  }
  return out
}

/**
 * Number of positional parameters a statement binds: the highest $n
 * referenced outside literals and comments ($3 alone still binds three
 * values — PostgreSQL parameters are positional).
 */
export function parameterCount(sql: string): number {
  let max = 0
  for (const t of scan(sql)) {
    if (t.kind === 'param' && t.index > max) max = t.index
  }
  return max
}

/** First keyword of the statement, upper-cased; "(" for a parenthesized query. */
export function leadingKeyword(sql: string): string {
  const first = scan(sql).find(t => t.kind !== 'param')
  if (!first) return ''
  return first.kind === 'paren' ? '(' : first.text
}

const WRITE_LEADING = new Set(['INSERT', 'UPDATE', 'DELETE', 'MERGE', 'CREATE', 'DROP', 'ALTER', 'TRUNCATE', 'GRANT', 'REVOKE', 'COPY'])
const WRITE_ANYWHERE = new Set(['INSERT', 'UPDATE', 'DELETE', 'MERGE'])

/**
 * UI hint: does the statement (probably) write? True for write statements
 * and for queries containing DML (data-modifying CTEs) or FOR UPDATE row
 * locks. Hidden writes in functions are not detectable here — the server's
 * read-only transaction catches those.
 */
export function isWriteStatement(sql: string): boolean {
  const tokens = scan(sql)
  const first = tokens.find(t => t.kind === 'word')
  if (!first || first.kind !== 'word') return false
  if (WRITE_LEADING.has(first.text)) return true
  return tokens.some(t => t.kind === 'word' && WRITE_ANYWHERE.has(t.text))
}

// --- Completion ---------------------------------------------------------------

/** Schemas present in the catalog, sorted. */
export function completionSchemas(tables: readonly SqlTable[]): string[] {
  return Array.from(new Set(tables.map(t => t.schema))).sort()
}

/**
 * The schema whose tables complete unqualified: the current choice while it
 * still exists, else "public", else the first schema in the catalog.
 */
export function defaultCompletionSchema(tables: readonly SqlTable[], current?: string | null): string {
  const schemas = completionSchemas(tables)
  if (current && schemas.includes(current)) return current
  if (schemas.includes('public')) return 'public'
  return schemas[0] ?? 'public'
}

/** PostgreSQL folds unquoted identifiers to lower case; anything else needs quotes. */
export function quoteIdentIfNeeded(name: string): string {
  if (/^[a-z_][a-z0-9_$]*$/.test(name)) return name
  return '"' + name.replace(/"/g, '""') + '"'
}

/**
 * Completion namespace from the backend catalog: schema -> table -> columns
 * (with their types as detail). Used with lang-sql's defaultSchema, so the
 * selected schema's tables complete unqualified and every other schema's
 * tables complete schema-qualified.
 */
export function buildCompletionNamespace(tables: readonly SqlTable[]): SQLNamespace {
  const ns: Record<string, Record<string, Completion[]>> = {}
  for (const t of tables) {
    const schemaLevel = (ns[t.schema] ??= {})
    schemaLevel[t.name] = t.columns.map(c => {
      const quoted = quoteIdentIfNeeded(c.name)
      const completion: Completion = { label: c.name, type: 'property', detail: c.type }
      if (quoted !== c.name) completion.apply = quoted
      return completion
    })
  }
  return ns
}

// --- EXPLAIN plans ------------------------------------------------------------

export interface PlanRow {
  depth: number
  nodeType: string
  /** Relation / index / CTE the node reads, when there is one. */
  target?: string
  /** Join type, conditions and filters, compacted. */
  detail?: string
  startupCost?: number
  totalCost?: number
  planRows?: number
  actualRows?: number
  actualTotalTime?: number
  actualLoops?: number
}

export interface PlanSummary {
  rows: PlanRow[]
  planningTime?: number
  executionTime?: number
  /** True when the plan carries actual execution statistics. */
  analyzed: boolean
}

type PlanNode = Record<string, unknown>

const num = (v: unknown): number | undefined => (typeof v === 'number' && Number.isFinite(v) ? v : undefined)
const str = (v: unknown): string | undefined => (typeof v === 'string' && v !== '' ? v : undefined)

/**
 * Flatten PostgreSQL's EXPLAIN (FORMAT JSON) document into depth-annotated
 * rows, in plan order. Returns null for anything that is not that document
 * (the caller then shows the raw JSON rather than inventing a plan).
 */
export function flattenPlan(plan: unknown): PlanSummary | null {
  const doc = Array.isArray(plan) ? plan[0] : plan
  if (!doc || typeof doc !== 'object') return null
  const root = (doc as Record<string, unknown>)['Plan']
  if (!root || typeof root !== 'object') return null

  const rows: PlanRow[] = []
  let analyzed = false
  const walk = (node: PlanNode, depth: number) => {
    const relation = str(node['Relation Name'])
    const alias = str(node['Alias'])
    const target =
      relation !== undefined
        ? (str(node['Schema']) ? `${str(node['Schema'])}.${relation}` : relation) + (alias && alias !== relation ? ` ${alias}` : '')
        : str(node['CTE Name']) ?? str(node['Function Name']) ?? str(node['Index Name'])
    const details = [
      str(node['Join Type']) && `${node['Join Type']} join`,
      str(node['Operation']),
      relation !== undefined && str(node['Index Name']) && `using ${node['Index Name']}`,
      str(node['Index Cond']) && `index: ${node['Index Cond']}`,
      str(node['Hash Cond']) && `hash: ${node['Hash Cond']}`,
      str(node['Merge Cond']) && `merge: ${node['Merge Cond']}`,
      str(node['Join Filter']) && `join filter: ${node['Join Filter']}`,
      str(node['Filter']) && `filter: ${node['Filter']}`,
      Array.isArray(node['Sort Key']) && `sort: ${(node['Sort Key'] as unknown[]).join(', ')}`,
      Array.isArray(node['Group Key']) && `group: ${(node['Group Key'] as unknown[]).join(', ')}`,
    ].filter(Boolean) as string[]
    const actualRows = num(node['Actual Rows'])
    if (actualRows !== undefined) analyzed = true
    rows.push({
      depth,
      nodeType: str(node['Node Type']) ?? '(unknown node)',
      target,
      detail: details.length ? details.join('; ') : undefined,
      startupCost: num(node['Startup Cost']),
      totalCost: num(node['Total Cost']),
      planRows: num(node['Plan Rows']),
      actualRows,
      actualTotalTime: num(node['Actual Total Time']),
      actualLoops: num(node['Actual Loops']),
    })
    const children = node['Plans']
    if (Array.isArray(children)) {
      for (const child of children) {
        if (child && typeof child === 'object') walk(child as PlanNode, depth + 1)
      }
    }
  }
  walk(root as PlanNode, 0)
  const d = doc as Record<string, unknown>
  return {
    rows,
    planningTime: num(d['Planning Time']),
    executionTime: num(d['Execution Time']),
    analyzed,
  }
}

// --- Request IDs ----------------------------------------------------------------

/** A fresh request ID for one editor statement (server pattern [A-Za-z0-9_-]{8,128}). */
export function newRequestId(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID()
  }
  const bytes = new Uint8Array(16)
  crypto.getRandomValues(bytes)
  return Array.from(bytes, b => b.toString(16).padStart(2, '0')).join('')
}

// --- S05: honest plan tree ---------------------------------------------------------

export interface PlanTreeNode {
  nodeType: string
  target?: string
  detail?: string
  /** Curated metrics present on this node. */
  metrics: Array<[string, string]>
  /** Every OTHER field PostgreSQL emitted on this node, verbatim — the
   *  tree never drops engine evidence it does not recognize. */
  extras: Array<[string, string]>
  children: PlanTreeNode[]
}

/** Fields the curated metrics line covers; everything else lands in extras. */
const CURATED_NODE_FIELDS = new Set([
  'Node Type', 'Relation Name', 'Schema', 'Alias', 'CTE Name', 'Function Name', 'Index Name',
  'Join Type', 'Operation', 'Index Cond', 'Hash Cond', 'Merge Cond', 'Join Filter', 'Filter',
  'Sort Key', 'Group Key', 'Startup Cost', 'Total Cost', 'Plan Rows', 'Plan Width',
  'Actual Startup Time', 'Actual Total Time', 'Actual Rows', 'Actual Loops', 'Plans',
  'Parent Relationship', 'Rows Removed by Filter', 'Rows Removed by Join Filter',
  'Shared Hit Blocks', 'Shared Read Blocks', 'Shared Dirtied Blocks', 'Shared Written Blocks',
  'Local Hit Blocks', 'Local Read Blocks', 'Local Dirtied Blocks', 'Local Written Blocks',
  'Temp Read Blocks', 'Temp Written Blocks',
])

function renderValue(v: unknown): string {
  if (Array.isArray(v)) return v.map(renderValue).join(', ')
  if (v === null) return 'null'
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

/** Extract the plan tree with per-node metrics AND every uncurated field
 *  PostgreSQL emitted (honest to the engine's output). Returns null for a
 *  document that is not a PostgreSQL FORMAT JSON plan. */
export function planTree(plan: unknown): PlanTreeNode | null {
  const doc = Array.isArray(plan) ? plan[0] : plan
  if (!doc || typeof doc !== 'object') return null
  const root = (doc as Record<string, unknown>)['Plan']
  if (!root || typeof root !== 'object') return null

  const walk = (node: PlanNode): PlanTreeNode => {
    const relation = str(node['Relation Name'])
    const target =
      relation !== undefined
        ? (str(node['Schema']) ? `${str(node['Schema'])}.${relation}` : relation)
        : str(node['CTE Name']) ?? str(node['Function Name']) ?? str(node['Index Name'])
    const details = [
      str(node['Join Type']),
      str(node['Parent Relationship']),
      str(node['Operation']),
      str(node['Index Name']) && `using ${node['Index Name']}`,
      str(node['Index Cond']) && `index: ${node['Index Cond']}`,
      str(node['Hash Cond']) && `hash: ${node['Hash Cond']}`,
      str(node['Merge Cond']) && `merge: ${node['Merge Cond']}`,
      str(node['Join Filter']) && `join filter: ${node['Join Filter']}`,
      str(node['Filter']) && `filter: ${node['Filter']}`,
      num(node['Rows Removed by Filter']) !== undefined && `removed by filter: ${node['Rows Removed by Filter']}`,
      Array.isArray(node['Sort Key']) && `sort: ${(node['Sort Key'] as unknown[]).join(', ')}`,
      Array.isArray(node['Group Key']) && `group: ${(node['Group Key'] as unknown[]).join(', ')}`,
    ].filter(Boolean) as string[]

    const metrics: Array<[string, string]> = []
    const push = (label: string, v: unknown) => {
      if (v !== undefined && v !== null && v !== '') metrics.push([label, renderValue(v)])
    }
    const cost = num(node['Total Cost'])
    if (cost !== undefined) metrics.push(['cost', `${num(node['Startup Cost']) ?? 0}..${cost}`])
    push('est rows', node['Plan Rows'])
    if (num(node['Actual Rows']) !== undefined) {
      push('actual rows', node['Actual Rows'])
      push('time', `${num(node['Actual Startup Time']) ?? 0}..${num(node['Actual Total Time']) ?? 0} ms`)
      push('loops', node['Actual Loops'])
    }
    const buffers: string[] = []
    const bufferKinds = ['Shared', 'Local', 'Temp'] as const
    const bufferOps = ['Hit', 'Read', 'Dirtied', 'Written'] as const
    for (const kind of bufferKinds) {
      const parts: string[] = []
      for (const op of bufferOps) {
        const v = num(node[`${kind} ${op} Blocks`])
        if (v !== undefined) parts.push(`${op.toLowerCase()}=${v}`)
      }
      if (parts.length > 0) buffers.push(`${kind.toLowerCase()}: ${parts.join(' ')}`)
    }
    if (buffers.length > 0) metrics.push(['buffers', buffers.join(' · ')])

    const extras: Array<[string, string]> = []
    for (const [key, value] of Object.entries(node)) {
      if (CURATED_NODE_FIELDS.has(key)) continue
      if (value === undefined || value === null || value === '') continue
      extras.push([key, renderValue(value)])
    }

    const children: PlanTreeNode[] = []
    const childNodes = node['Plans']
    if (Array.isArray(childNodes)) {
      for (const child of childNodes) {
        if (child && typeof child === 'object') children.push(walk(child as PlanNode))
      }
    }
    return {
      nodeType: str(node['Node Type']) ?? '(unknown node)',
      target,
      detail: details.length ? details.join('; ') : undefined,
      metrics,
      extras,
      children,
    }
  }
  return walk(root as PlanNode)
}

// --- History (localStorage, per connection) ---------------------------------------

export const HISTORY_MAX = 50

export function historyKey(connId: string) {
  return `neutron:query-history:${connId}`
}

export function loadHistory(connId: string): QueryHistoryEntry[] {
  try {
    const parsed = JSON.parse(localStorage.getItem(historyKey(connId)) ?? '[]')
    return Array.isArray(parsed) ? parsed : []
  } catch {
    return []
  }
}

function saveHistory(connId: string, entries: QueryHistoryEntry[]) {
  localStorage.setItem(historyKey(connId), JSON.stringify(entries.slice(0, HISTORY_MAX)))
}

/**
 * Record a run at the top of the connection's history. An identical run
 * (same SQL and parameters) moves to the top instead of repeating.
 */
export function pushHistory(connId: string, entry: QueryHistoryEntry) {
  const sameRun = (e: QueryHistoryEntry) =>
    e.sql === entry.sql && JSON.stringify(e.params ?? []) === JSON.stringify(entry.params ?? [])
  const existing = loadHistory(connId).filter(e => !sameRun(e))
  saveHistory(connId, [entry, ...existing])
}

/** Whether a script may change the catalog (a statement starting with
 *  CREATE/ALTER/DROP/COMMENT/TRUNCATE/GRANT/REVOKE). A heuristic used only
 *  to trigger a schema refresh, never to classify safety. */
export function mayChangeCatalog(sql: string): boolean {
  const stripped = sql.replace(/--[^\n]*/g, ' ').replace(/\/\*[\s\S]*?\*\//g, ' ')
  return /(^|;)\s*(create|alter|drop|comment\s+on|truncate|grant|revoke)\b/i.test(stripped)
}
