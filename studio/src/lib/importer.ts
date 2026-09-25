// CSV/JSON table import (S06): streaming source reading, column mapping,
// lossless per-column encoding, bounded batching and a resumable,
// honestly-reporting batch runner.
//
// Atomicity is explicit and per batch: every batch is ONE server
// transaction (POST /api/table/v2/import/batch — the S02 commit machinery),
// so a batch applies all of its rows or none. The import as a whole is not
// atomic: batches that committed before a failure stay committed, and the
// journal says exactly which source rows they were.
//
// Recovery: the journal (persisted after every state change) records the
// next unconsumed source row, the committed/skipped ranges and the
// in-flight batch with its operation ID. After an interruption — a failed
// batch, a dropped response, a closed tab, a restarted server — the runner
// never guesses: an in-flight batch is resolved through the server's
// recorded outcome first (committed -> advance; failed -> nothing applied,
// retry or skip; unknown -> the user must check the table before choosing
// "mark committed" or "retry"). Resuming re-reads the same file (checked by
// name, size and modification time) and skips the rows already handled.

import { CsvParser, type CsvRecord } from './csv'
import { JsonRecordSplitter, parseJsonRecord, type LosslessJson } from './losslessJson'
import { encodeCell, WireEncodeError } from './wire'
import { ApiError } from './api'
import type {
  ImportBatchRequest, ImportBatchResponse, ImportOutcomeResponse, TableMetaColumn,
} from './types'

// --- source reading ---

export type ImportFormat = 'csv' | 'json'

export interface CsvSourceOptions {
  delimiter: string
  /** First record names the fields. */
  header: boolean
}

export interface SourceField {
  id: string
  label: string
}

export type SourceValue =
  | { kind: 'text'; text: string; quoted: boolean }
  | { kind: 'json'; value: LosslessJson }

export interface SourceRecord {
  /** 1-based data row (header excluded). */
  row: number
  /** 1-based line the record starts on. */
  line: number
  values: Map<string, SourceValue>
  /** A structural problem with the record itself (e.g. a CSV row with the
   *  wrong number of fields); encoding refuses such rows. */
  shapeError?: string
}

/** Fields discovered while reading (CSV: the header or first record;
 *  JSON: member names in first-seen order). */
export class FieldRegistry {
  fields: SourceField[] = []
  private ids = new Set<string>()
  add(field: SourceField) {
    if (this.ids.has(field.id)) return
    this.ids.add(field.id)
    this.fields.push(field)
  }
}

export const READ_CHUNK_BYTES = 256 * 1024

/** Read a Blob as UTF-8 text chunks without holding the file in memory.
 *  Invalid UTF-8 is an error, never silently replaced. */
export async function* blobTextChunks(blob: Blob, chunkBytes = READ_CHUNK_BYTES): AsyncGenerator<string> {
  const decoder = new TextDecoder('utf-8', { fatal: true })
  for (let offset = 0; offset < blob.size; offset += chunkBytes) {
    const buf = await blob.slice(offset, Math.min(blob.size, offset + chunkBytes)).arrayBuffer()
    let text: string
    try {
      text = decoder.decode(new Uint8Array(buf), { stream: true })
    } catch {
      throw new Error(`the file is not valid UTF-8 (near byte ${offset}); re-save it as UTF-8`)
    }
    if (text) yield text
  }
  const tail = decoder.decode()
  if (tail) yield tail
}

function csvFieldId(i: number): string {
  return `c${i}`
}

/**
 * Stream source records from text chunks. The registry is filled as fields
 * are discovered. Parse errors (unterminated quote, malformed JSON) throw
 * with the line they occurred on — the file itself is malformed there.
 */
export async function* readSourceRecords(
  chunks: AsyncIterable<string>,
  format: ImportFormat,
  csv: CsvSourceOptions,
  registry: FieldRegistry,
): AsyncGenerator<SourceRecord> {
  let row = 0
  if (format === 'csv') {
    const parser = new CsvParser({ delimiter: csv.delimiter })
    let expected = -1
    const toSource = (rec: CsvRecord): SourceRecord | null => {
      if (expected < 0) {
        expected = rec.fields.length
        const seen = new Map<string, number>()
        rec.fields.forEach((f, i) => {
          const name = csv.header ? (f.text === '' ? `(unnamed ${i + 1})` : f.text) : `column ${i + 1}`
          const n = (seen.get(name) ?? 0) + 1
          seen.set(name, n)
          registry.add({ id: csvFieldId(i), label: n > 1 ? `${name} (#${i + 1})` : name })
        })
        if (csv.header) return null
      }
      row++
      const values = new Map<string, SourceValue>()
      rec.fields.forEach((f, i) => values.set(csvFieldId(i), { kind: 'text', text: f.text, quoted: f.quoted }))
      const out: SourceRecord = { row, line: rec.line, values }
      if (rec.fields.length !== expected) {
        out.shapeError = `has ${rec.fields.length} field${rec.fields.length === 1 ? '' : 's'}; expected ${expected}`
      }
      return out
    }
    for await (const chunk of chunks) {
      for (const rec of parser.push(chunk)) {
        const s = toSource(rec)
        if (s) yield s
      }
    }
    for (const rec of parser.end()) {
      const s = toSource(rec)
      if (s) yield s
    }
    return
  }
  const splitter = new JsonRecordSplitter()
  const toSource = (text: string, line: number): SourceRecord => {
    const members = parseJsonRecord(text, line)
    row++
    const values = new Map<string, SourceValue>()
    for (const [key, value] of members) {
      const id = `k:${key}`
      registry.add({ id, label: key })
      values.set(id, { kind: 'json', value })
    }
    return { row, line, values }
  }
  for await (const chunk of chunks) {
    for (const r of splitter.push(chunk)) yield toSource(r.text, r.line)
  }
  for (const r of splitter.end()) yield toSource(r.text, r.line)
}

// --- mapping and encoding ---

export type ColumnMapping =
  | { kind: 'field'; field: string }
  | { kind: 'default' }
  | { kind: 'null' }

export interface ImportValueOptions {
  /** CSV: an UNQUOTED empty field means SQL NULL ('null', PostgreSQL COPY
   *  convention) or the empty string ('empty'). A quoted "" is always the
   *  empty string. */
  emptyUnquoted: 'null' | 'empty'
  /** CSV: an UNQUOTED field exactly equal to this text is SQL NULL
   *  (e.g. "\N" or "NULL"); null disables the marker. */
  nullMarker: string | null
}

export const DEFAULT_VALUE_OPTIONS: ImportValueOptions = { emptyUnquoted: 'null', nullMarker: null }

export type EncodedCell =
  | { kind: 'value'; value: unknown }
  | { kind: 'null' }
  | { kind: 'omit' }

export class RowEncodeError extends Error {
  column?: string
  constructor(message: string, column?: string) {
    super(message)
    this.name = 'RowEncodeError'
    this.column = column
  }
}

/** Columns an import may target: insertable per the server's catalog. */
export function importTargets(columns: TableMetaColumn[]): TableMetaColumn[] {
  return columns.filter(c => c.insertable === true)
}

/** A column that must receive a value: NOT NULL with no default. */
export function isRequired(col: TableMetaColumn): boolean {
  return !col.nullable && !col.hasDefault && !col.autoAssigned
}

/** Map each target column to the source field of the same name
 *  (case-insensitive); unmatched columns use DEFAULT. */
export function autoMap(fields: SourceField[], columns: TableMetaColumn[]): Record<string, ColumnMapping> {
  const byName = new Map<string, SourceField>()
  for (const f of fields) {
    const k = f.label.toLowerCase()
    if (!byName.has(k)) byName.set(k, f)
  }
  const out: Record<string, ColumnMapping> = {}
  for (const col of columns) {
    const f = byName.get(col.name.toLowerCase())
    out[col.name] = f ? { kind: 'field', field: f.id } : { kind: 'default' }
  }
  return out
}

/** Mapping-level refusals: problems every row would share. */
export function mappingProblems(mapping: Record<string, ColumnMapping>, columns: TableMetaColumn[]): string[] {
  const problems: string[] = []
  let mapped = 0
  for (const col of columns) {
    const m = mapping[col.name] ?? { kind: 'default' }
    if (m.kind === 'field') mapped++
    if (m.kind === 'default' && isRequired(col)) {
      problems.push(`${col.name} is NOT NULL without a default — map a source field to it`)
    }
    if (m.kind === 'null' && !col.nullable) {
      problems.push(`${col.name} is NOT NULL — it cannot be set to NULL`)
    }
  }
  if (mapped === 0) problems.push('map at least one source field to a column')
  return problems
}

const TRUE_WORDS = new Set(['t', 'true', 'y', 'yes', 'on', '1'])
const FALSE_WORDS = new Set(['f', 'false', 'n', 'no', 'off', '0'])
const INT8_MIN = -(2n ** 63n)
const INT8_MAX = 2n ** 63n - 1n

function isBoolType(col: TableMetaColumn): boolean {
  return col.type === 'bool' || col.type === 'boolean'
}

function isJsonType(col: TableMetaColumn): boolean {
  return /^json(b)?$/.test(col.type)
}

/** Encode text for one column into its wire value, validating what the
 *  client can check exactly; the server re-validates everything. */
function encodeText(text: string, col: TableMetaColumn): unknown {
  if (isBoolType(col)) {
    const w = text.trim().toLowerCase()
    if (TRUE_WORDS.has(w)) return true
    if (FALSE_WORDS.has(w)) return false
    throw new RowEncodeError(`${col.name}: ${JSON.stringify(text)} is not a boolean (true/false, t/f, yes/no, on/off, 1/0)`, col.name)
  }
  if (isJsonType(col)) {
    try {
      JSON.parse(text) // validity only; the exact text is what is sent
    } catch {
      throw new RowEncodeError(`${col.name}: not valid JSON text`, col.name)
    }
    return text
  }
  if (col.tag === 'vector' || col.tag === 'tsvector') {
    throw new RowEncodeError(`${col.name}: ${col.tag} columns are not importable values`, col.name)
  }
  if (col.tag) {
    const t = text.trim()
    if (col.tag === 'int8') {
      if (!/^[+-]?\d+$/.test(t)) throw new RowEncodeError(`${col.name}: ${JSON.stringify(text)} is not an integer`, col.name)
      const n = BigInt(t)
      if (n < INT8_MIN || n > INT8_MAX) throw new RowEncodeError(`${col.name}: ${t} is out of range for bigint`, col.name)
      return { t: 'int8', v: n.toString() }
    }
    if (col.tag === 'bytea') {
      const hex = /^\\x/i.test(t) ? t.slice(2) : t
      if (!/^[0-9a-f]*$/i.test(hex) || hex.length % 2 !== 0) {
        throw new RowEncodeError(`${col.name}: bytea must be hex (\\x0a1b… or 0a1b…)`, col.name)
      }
      return { t: 'bytea', v: hex.toLowerCase() }
    }
    if (t === '') throw new RowEncodeError(`${col.name}: empty text is not a ${col.type} value`, col.name)
    try {
      return encodeCell(t, col.tag)
    } catch (err) {
      if (err instanceof WireEncodeError) throw new RowEncodeError(`${col.name}: ${err.message}`, col.name)
      throw err
    }
  }
  return text
}

/** Encode one source value for a column, keeping NULL / empty string /
 *  DEFAULT distinct and numbers as their literal digits. */
export function encodeSourceValue(sv: SourceValue | undefined, col: TableMetaColumn, opts: ImportValueOptions): EncodedCell {
  if (sv === undefined) return { kind: 'omit' } // JSON: an absent member is DEFAULT
  if (sv.kind === 'text') {
    if (!sv.quoted) {
      if (sv.text === '' && opts.emptyUnquoted === 'null') return { kind: 'null' }
      if (opts.nullMarker !== null && sv.text === opts.nullMarker) return { kind: 'null' }
    }
    return { kind: 'value', value: encodeText(sv.text, col) }
  }
  const v = sv.value
  switch (v.kind) {
    case 'null':
      return { kind: 'null' }
    case 'boolean':
      if (isBoolType(col)) return { kind: 'value', value: v.value }
      return { kind: 'value', value: encodeText(String(v.value), col) }
    case 'number':
      if (isJsonType(col)) return { kind: 'value', value: v.raw }
      if (isBoolType(col)) {
        throw new RowEncodeError(`${col.name}: a JSON number is not a boolean`, col.name)
      }
      return { kind: 'value', value: encodeText(v.raw, col) }
    case 'string':
      if (isJsonType(col)) return { kind: 'value', value: JSON.stringify(v.value) }
      return { kind: 'value', value: encodeText(v.value, col) }
    case 'object':
    case 'array':
      if (isJsonType(col)) return { kind: 'value', value: v.raw }
      if (col.tag || isBoolType(col)) {
        throw new RowEncodeError(`${col.name}: a JSON ${v.kind} is not a ${col.type} value`, col.name)
      }
      return { kind: 'value', value: v.raw }
  }
}

/** Encode one source record into an insert values map (omitted column =
 *  DEFAULT, null = SQL NULL). Throws RowEncodeError naming the column. */
export function encodeRecord(
  rec: SourceRecord,
  columns: TableMetaColumn[],
  mapping: Record<string, ColumnMapping>,
  opts: ImportValueOptions,
): Record<string, unknown> {
  if (rec.shapeError) throw new RowEncodeError(`the row ${rec.shapeError}`)
  const values: Record<string, unknown> = {}
  for (const col of columns) {
    const m = mapping[col.name] ?? { kind: 'default' }
    let cell: EncodedCell
    if (m.kind === 'default') cell = { kind: 'omit' }
    else if (m.kind === 'null') cell = { kind: 'null' }
    else cell = encodeSourceValue(rec.values.get(m.field), col, opts)
    if (cell.kind === 'omit') {
      if (isRequired(col)) throw new RowEncodeError(`${col.name} is NOT NULL without a default and this row has no value for it`, col.name)
      continue
    }
    if (cell.kind === 'null') {
      if (!col.nullable) throw new RowEncodeError(`${col.name} is NOT NULL; this row has NULL`, col.name)
      values[col.name] = null
      continue
    }
    values[col.name] = cell.value
  }
  return values
}

// --- batching ---

/** Server bounds: 100 operations and 1 MiB per request. Batches stay under
 *  both (the byte budget leaves room for the envelope). */
export const MAX_BATCH_ROWS = 100
export const MAX_BATCH_BYTES = 900 * 1024

const encoder = new TextEncoder()

export function jsonByteLength(value: unknown): number {
  return encoder.encode(JSON.stringify(value)).length
}

// --- journal ---

export interface ImportFileFingerprint {
  name: string
  size: number
  lastModified: number
}

export interface ImportPlan {
  connectionId: string
  schema: string
  table: string
  binding: string
  format: ImportFormat
  csv: CsvSourceOptions
  values: ImportValueOptions
  /** Target column name -> source. Only insertable columns appear. */
  mapping: Record<string, ColumnMapping>
  /** Rows per batch (1..MAX_BATCH_ROWS); the byte budget may cut sooner. */
  batchSize: number
}

export type PendingState = 'sending' | 'failed' | 'unknown' | 'invalid' | 'retrying'

/** The batch that is in flight or needs a decision. */
export interface PendingBatch {
  batch: number
  operationId: string
  attempt: number
  /** First and last 1-based source rows the batch covers. */
  fromRow: number
  toRow: number
  /** The source rows actually in the batch, in order (skipped rows
   *  excluded): maps the server's batch-relative failedRow to a row. */
  rows: number[]
  state: PendingState
  error?: string
  /** 1-based SOURCE row that caused the failure, when known. */
  failedRow?: number
}

export type ImportStatus = 'ready' | 'running' | 'paused' | 'needs-decision' | 'done' | 'error'

export interface ImportJournal {
  version: 1
  importId: string
  plan: ImportPlan
  file: ImportFileFingerprint
  /** Every source row <= nextRow is handled (committed or skipped). */
  nextRow: number
  rowsCommitted: number
  batchesCommitted: number
  /** Source rows deliberately skipped, with the reason (ranges). */
  skipped: Array<{ fromRow: number; toRow: number; reason: string }>
  /** The next batch number (1-based). */
  nextBatch: number
  pending?: PendingBatch
  status: ImportStatus
  /** A file-level error that stopped the import (parse error, bad UTF-8). */
  error?: string
  startedAt: string
  updatedAt: string
}

export function newImportJournal(plan: ImportPlan, file: ImportFileFingerprint, importId: string): ImportJournal {
  const now = new Date().toISOString()
  return {
    version: 1, importId, plan, file,
    nextRow: 0, rowsCommitted: 0, batchesCommitted: 0, skipped: [],
    nextBatch: 1, status: 'ready', startedAt: now, updatedAt: now,
  }
}

export function sameFile(a: ImportFileFingerprint, b: ImportFileFingerprint): boolean {
  return a.name === b.name && a.size === b.size && a.lastModified === b.lastModified
}

export function batchOperationId(importId: string, batch: number, attempt: number): string {
  return `import-${importId}-b${batch}-a${attempt}`
}

export function journalKey(connectionId: string, schema: string, table: string): string {
  return `neutron-studio:import:${connectionId}:${schema}.${table}`
}

export interface JournalStorage {
  getItem(key: string): string | null
  setItem(key: string, value: string): void
  removeItem(key: string): void
}

export function loadJournal(storage: JournalStorage, key: string): ImportJournal | null {
  const raw = storage.getItem(key)
  if (!raw) return null
  try {
    const j = JSON.parse(raw) as ImportJournal
    return j && j.version === 1 && typeof j.importId === 'string' ? j : null
  } catch {
    return null
  }
}

function isSkipped(j: ImportJournal, row: number): boolean {
  return j.skipped.some(s => row >= s.fromRow && row <= s.toRow)
}

function addSkip(j: ImportJournal, fromRow: number, toRow: number, reason: string) {
  j.skipped.push({ fromRow, toRow, reason })
}

// --- runner ---

export interface ImportDeps {
  sendBatch(req: ImportBatchRequest): Promise<ImportBatchResponse>
  outcome(connectionId: string, operationId: string): Promise<ImportOutcomeResponse>
  /** Persist the journal (called after every state change). */
  save(journal: ImportJournal): void
  /** Stop between batches (an in-flight batch is always resolved first). */
  signal?: AbortSignal
  /** Wait between in-progress polls (tests pass a no-op). */
  sleep?(ms: number): Promise<void>
}

function touch(j: ImportJournal): ImportJournal {
  j.updatedAt = new Date().toISOString()
  return j
}

/** The source row a server failure names: the structured batch-relative
 *  failedRow, else an operations[N] prefix, mapped through the batch. */
function failedSourceRow(p: PendingBatch, body: Record<string, unknown> | undefined, message: string): number | undefined {
  let idx: number | undefined
  const f = body?.failedRow
  if (typeof f === 'number' && Number.isInteger(f) && f >= 0) idx = f
  else {
    const m = /^operations\[(\d+)\]/.exec(message)
    if (m) idx = Number(m[1])
  }
  return idx === undefined ? undefined : p.rows[idx]
}

function commitPending(j: ImportJournal, applied: number) {
  const p = j.pending
  if (!p) return
  j.rowsCommitted += applied
  j.batchesCommitted++
  j.nextRow = Math.max(j.nextRow, p.toRow)
  j.nextBatch = Math.max(j.nextBatch, p.batch + 1)
  j.pending = undefined
}

/**
 * Resolve the in-flight batch through the server's recorded outcome:
 * committed advances the journal; failed means nothing was applied;
 * anything else is unknown and needs the user's decision.
 */
export async function resolvePending(j: ImportJournal, deps: ImportDeps): Promise<ImportJournal> {
  const p = j.pending
  if (!p || (p.state !== 'sending' && p.state !== 'unknown')) return j
  const sleep = deps.sleep ?? ((ms: number) => new Promise<void>(r => setTimeout(r, ms)))
  for (let poll = 0; poll < 20; poll++) {
    let res: ImportOutcomeResponse
    try {
      res = await deps.outcome(j.plan.connectionId, p.operationId)
    } catch (err) {
      p.state = 'unknown'
      p.error = `the outcome of batch ${p.batch} could not be looked up (${err instanceof Error ? err.message : String(err)}); rows ${p.fromRow}–${p.toRow} may or may not have been inserted`
      j.status = 'needs-decision'
      deps.save(touch(j))
      return j
    }
    if (res.state === 'committed') {
      const applied = typeof res.response?.applied === 'number' ? res.response.applied : p.rows.length
      commitPending(j, applied)
      j.status = 'paused'
      deps.save(touch(j))
      return j
    }
    if (res.state === 'failed') {
      const body = res.response ?? {}
      p.state = 'failed'
      p.error = typeof body.error === 'string' ? body.error : 'the batch failed; nothing of it was applied'
      p.failedRow = failedSourceRow(p, body, p.error)
      j.status = 'needs-decision'
      deps.save(touch(j))
      return j
    }
    if (res.state === 'in_progress') {
      await sleep(250)
      continue
    }
    break
  }
  p.state = 'unknown'
  p.error = `no outcome is recorded for batch ${p.batch} (rows ${p.fromRow}–${p.toRow}) — the server may have restarted or forgotten it. Check whether those rows are in the table, then mark the batch committed or retry it.`
  j.status = 'needs-decision'
  deps.save(touch(j))
  return j
}

/** User decision: retry the pending batch under a NEW operation ID. The
 *  fresh ID was never sent, so it must go out directly — resolving it
 *  through the outcome lookup would honestly answer "unknown" and trap the
 *  user in a decision loop ('retrying', not 'sending'). */
export function retryPending(j: ImportJournal): ImportJournal {
  const p = j.pending
  if (!p) return j
  p.attempt++
  p.operationId = batchOperationId(j.importId, p.batch, p.attempt)
  p.state = 'retrying'
  p.error = undefined
  p.failedRow = undefined
  j.status = 'paused'
  return touch(j)
}

/** User decision: skip every row of the pending batch (recorded). */
export function skipPending(j: ImportJournal): ImportJournal {
  const p = j.pending
  if (!p) return j
  addSkip(j, p.fromRow, p.toRow, p.error ?? 'skipped')
  j.nextRow = Math.max(j.nextRow, p.toRow)
  j.nextBatch = Math.max(j.nextBatch, p.batch + 1)
  j.pending = undefined
  j.status = 'paused'
  return touch(j)
}

/** User decision: skip only the row that failed and retry the rest of the
 *  batch under a new operation ID (nothing of the failed attempt applied). */
export function skipFailedRow(j: ImportJournal): ImportJournal {
  const p = j.pending
  if (!p || p.failedRow === undefined) return j
  addSkip(j, p.failedRow, p.failedRow, p.error ?? 'skipped')
  return retryPending(j)
}

/** User decision after checking the table: an unknown batch did commit. */
export function markPendingCommitted(j: ImportJournal): ImportJournal {
  const p = j.pending
  if (!p) return j
  commitPending(j, p.rows.length)
  j.status = 'paused'
  return touch(j)
}

interface BuiltBatch {
  rows: Array<Record<string, unknown>>
  sourceRows: number[]
  /** Last source row read into this window (included or skipped). */
  lastSeen: number
  exhausted: boolean
  invalid?: { row: number; message: string }
}

/**
 * Run (or resume) an import from the journal's position. Returns when the
 * source is exhausted (done), a batch needs a decision, the signal stops it
 * between batches (paused), or the file itself is malformed (error).
 */
export async function runImport(
  records: AsyncIterable<SourceRecord>,
  columns: TableMetaColumn[],
  j: ImportJournal,
  deps: ImportDeps,
): Promise<ImportJournal> {
  // A user-decided retry ('retrying') sends directly; only an interrupted
  // send ('sending') is resolved through the server's outcome first.
  if (j.pending && j.pending.state !== 'sending' && j.pending.state !== 'retrying') {
    j.status = 'needs-decision'
    return j
  }
  const plan = j.plan
  const size = Math.max(1, Math.min(MAX_BATCH_ROWS, Math.floor(plan.batchSize)))
  j.status = 'running'
  j.error = undefined
  deps.save(touch(j))

  const iterator = records[Symbol.asyncIterator]()
  let lookahead: SourceRecord | null = null
  const next = async (): Promise<SourceRecord | null> => {
    if (lookahead) {
      const r = lookahead
      lookahead = null
      return r
    }
    const r = await iterator.next()
    return r.done ? null : r.value
  }

  // limitRow: rebuild a pending batch's exact window (a retry); null: the
  // next fresh batch, cut by row count and byte budget.
  const build = async (limitRow: number | null): Promise<BuiltBatch> => {
    const b: BuiltBatch = { rows: [], sourceRows: [], lastSeen: j.nextRow, exhausted: false }
    let bytes = 0
    for (;;) {
      if (limitRow !== null ? b.lastSeen >= limitRow : b.rows.length >= size) break
      const rec = await next()
      if (!rec) {
        b.exhausted = true
        break
      }
      if (rec.row <= j.nextRow) continue
      if (isSkipped(j, rec.row)) {
        b.lastSeen = rec.row
        continue
      }
      let values: Record<string, unknown> | null = null
      let problem: string | null = null
      let rowBytes = 0
      try {
        values = encodeRecord(rec, columns, plan.mapping, plan.values)
        rowBytes = jsonByteLength(values) + 1
        if (rowBytes > MAX_BATCH_BYTES) {
          problem = `the row encodes to ${rowBytes} bytes, above the ${MAX_BATCH_BYTES}-byte request budget`
        }
      } catch (err) {
        problem = err instanceof Error ? err.message : String(err)
      }
      if (problem !== null || values === null) {
        if (b.rows.length > 0) {
          // send the valid rows before it first; the bad row gets its own
          // decision (skip exactly that row, or stop)
          lookahead = rec
          break
        }
        b.invalid = { row: rec.row, message: problem ?? 'unencodable row' }
        b.lastSeen = rec.row
        break
      }
      if (b.rows.length > 0 && limitRow === null && bytes + rowBytes > MAX_BATCH_BYTES) {
        lookahead = rec
        break
      }
      bytes += rowBytes
      b.rows.push(values)
      b.sourceRows.push(rec.row)
      b.lastSeen = rec.row
    }
    return b
  }

  try {
    for (;;) {
      if (deps.signal?.aborted) {
        j.status = 'paused'
        deps.save(touch(j))
        return j
      }
      const resumed = j.pending
      const b = await build(resumed ? resumed.toRow : null)

      if (b.invalid) {
        const p: PendingBatch = resumed && resumed.fromRow === b.invalid.row ? resumed : {
          batch: resumed?.batch ?? j.nextBatch, operationId: '', attempt: resumed?.attempt ?? 1,
          fromRow: b.invalid.row, toRow: b.invalid.row, rows: [b.invalid.row], state: 'invalid',
        }
        p.fromRow = p.toRow = b.invalid.row
        p.rows = [b.invalid.row]
        p.state = 'invalid'
        p.failedRow = b.invalid.row
        p.error = `row ${b.invalid.row}: ${b.invalid.message}`
        p.operationId = batchOperationId(j.importId, p.batch, p.attempt)
        j.pending = p
        j.nextBatch = Math.max(j.nextBatch, p.batch + 1)
        j.status = 'needs-decision'
        deps.save(touch(j))
        return j
      }

      if (b.rows.length === 0) {
        if (resumed && b.lastSeen >= resumed.toRow) {
          // every row of the retried window was skipped: nothing to send
          j.nextRow = Math.max(j.nextRow, resumed.toRow)
          j.nextBatch = Math.max(j.nextBatch, resumed.batch + 1)
          j.pending = undefined
          deps.save(touch(j))
          continue
        }
        if (resumed) {
          resumed.state = 'invalid'
          resumed.error = `the file ends before row ${resumed.toRow}; it is not the file this import started from`
          j.status = 'needs-decision'
          deps.save(touch(j))
          return j
        }
        j.status = 'done'
        deps.save(touch(j))
        return j
      }

      const p: PendingBatch = resumed ?? {
        batch: j.nextBatch,
        operationId: batchOperationId(j.importId, j.nextBatch, 1),
        attempt: 1, fromRow: 0, toRow: 0, rows: [], state: 'sending',
      }
      p.fromRow = b.sourceRows[0]
      p.toRow = b.lastSeen
      p.rows = b.sourceRows
      p.state = 'sending'
      j.pending = p
      j.nextBatch = Math.max(j.nextBatch, p.batch + 1)
      deps.save(touch(j))

      let res: ImportBatchResponse
      try {
        res = await deps.sendBatch({
          connectionId: plan.connectionId,
          operationId: p.operationId,
          schema: plan.schema,
          table: plan.table,
          binding: plan.binding,
          rows: b.rows,
        })
      } catch (err) {
        const definite = err instanceof ApiError &&
          err.state !== 'in_progress' && err.state !== 'unknown' && err.body !== undefined
        if (!definite) {
          // No response, or the server itself cannot say: resolve through
          // the recorded outcome before anything else.
          await resolvePending(j, deps)
          if (!j.pending) continue
          return j
        }
        // A definite refusal: the batch's transaction never committed.
        p.state = 'failed'
        p.error = err.message
        p.failedRow = failedSourceRow(p, err.body, err.message)
        j.status = 'needs-decision'
        deps.save(touch(j))
        return j
      }
      commitPending(j, res.applied)
      deps.save(touch(j))
    }
  } catch (err) {
    // The file itself is malformed (CSV/JSON syntax, invalid UTF-8): rows
    // before the error that already committed stay committed.
    j.status = 'error'
    j.error = err instanceof Error ? err.message : String(err)
    deps.save(touch(j))
    return j
  }
}

/** Human summary of where an import stands (the recovery report). */
export function describeJournal(j: ImportJournal): string {
  const parts = [`${j.rowsCommitted.toLocaleString()} row${j.rowsCommitted === 1 ? '' : 's'} committed in ${j.batchesCommitted} batch${j.batchesCommitted === 1 ? '' : 'es'}`]
  if (j.skipped.length > 0) {
    const n = j.skipped.reduce((acc, s) => acc + (s.toRow - s.fromRow + 1), 0)
    parts.push(`${n.toLocaleString()} row${n === 1 ? '' : 's'} skipped (${j.skipped.map(s => `${s.fromRow}–${s.toRow}`).join(', ')})`)
  }
  if (j.nextRow > 0 && j.status !== 'done') parts.push(`source rows 1–${j.nextRow} handled`)
  return parts.join('; ')
}
