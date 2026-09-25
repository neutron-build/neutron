import { useEffect, useRef, useState } from 'preact/hooks'
import { api } from '../../lib/api'
import {
  FieldRegistry, readSourceRecords, blobTextChunks, autoMap, mappingProblems, importTargets,
  isRequired, encodeRecord, encodeSourceValue, newImportJournal, runImport, resolvePending,
  retryPending, skipPending, skipFailedRow, markPendingCommitted, sameFile, journalKey,
  loadJournal, describeJournal, MAX_BATCH_ROWS,
  type ColumnMapping, type CsvSourceOptions, type ImportFormat, type ImportJournal,
  type ImportValueOptions, type JournalStorage, type SourceField, type SourceRecord,
  type ImportDeps,
} from '../../lib/importer'
import type { TableMeta, TableMetaColumn } from '../../lib/types'
import s from './ImportDialog.module.css'

// CSV/JSON import dialog (S06). Everything the file contains is read in
// streamed chunks; only the preview's first rows are held. The import
// itself runs batch by batch through the guarded import endpoint, each
// batch one transaction, with a persisted journal for honest recovery
// (see lib/importer.ts for the atomicity and recovery design).

export interface ImportDialogProps {
  connectionId: string
  schema: string
  table: string
  meta: TableMeta
  onClose: () => void
  /** Called after rows may have been committed (reload the grid). */
  onImported?: () => void
  /** Journal persistence; defaults to localStorage. */
  storage?: JournalStorage
  /** Batch transport; defaults to the Studio API. */
  transport?: Pick<ImportDeps, 'sendBatch' | 'outcome'>
}

const PREVIEW_RECORDS = 50
const PREVIEW_SHOWN = 10

interface Preview {
  fields: SourceField[]
  records: SourceRecord[]
}

function detectFormat(name: string): { format: ImportFormat; delimiter?: string } {
  const lower = name.toLowerCase()
  if (/\.(json|ndjson|jsonl)$/.test(lower)) return { format: 'json' }
  if (lower.endsWith('.tsv')) return { format: 'csv', delimiter: '\t' }
  return { format: 'csv' }
}

async function readPreview(file: Blob, format: ImportFormat, csv: CsvSourceOptions): Promise<Preview> {
  const registry = new FieldRegistry()
  const records: SourceRecord[] = []
  for await (const rec of readSourceRecords(blobTextChunks(file), format, csv, registry)) {
    records.push(rec)
    if (records.length >= PREVIEW_RECORDS) break
  }
  return { fields: registry.fields, records }
}

/** The oid part of a relation binding ("<epoch>:<oid>"). */
function bindingOid(binding: string | undefined): string | null {
  if (!binding) return null
  const i = binding.lastIndexOf(':')
  return i < 0 ? null : binding.slice(i + 1)
}

function cellPreview(rec: SourceRecord, col: TableMetaColumn, m: ColumnMapping, values: ImportValueOptions): { text: string; kind: 'value' | 'null' | 'default' | 'error' } {
  if (m.kind === 'default') return { text: 'DEFAULT', kind: 'default' }
  if (m.kind === 'null') return { text: 'NULL', kind: 'null' }
  try {
    const cell = encodeSourceValue(rec.values.get(m.field), col, values)
    if (cell.kind === 'omit') return { text: 'DEFAULT', kind: 'default' }
    if (cell.kind === 'null') return { text: 'NULL', kind: 'null' }
    const v = cell.value
    if (v !== null && typeof v === 'object' && 'v' in (v as Record<string, unknown>)) {
      return { text: String((v as { v: string }).v), kind: 'value' }
    }
    return { text: String(v), kind: 'value' }
  } catch (err) {
    return { text: err instanceof Error ? err.message : String(err), kind: 'error' }
  }
}

export function ImportDialog({ connectionId, schema: schemaName, table, meta, onClose, onImported, storage, transport }: ImportDialogProps) {
  const store: JournalStorage = storage ?? globalThis.localStorage
  const key = journalKey(connectionId, schemaName, table)
  const targets = importTargets(meta.columns)

  const [file, setFile] = useState<File | null>(null)
  const [format, setFormat] = useState<ImportFormat>('csv')
  const [csv, setCsv] = useState<CsvSourceOptions>({ delimiter: ',', header: true })
  const [values, setValues] = useState<ImportValueOptions>({ emptyUnquoted: 'null', nullMarker: null })
  const [preview, setPreview] = useState<Preview | null>(null)
  const [previewError, setPreviewError] = useState<string | null>(null)
  const [mapping, setMapping] = useState<Record<string, ColumnMapping>>({})
  const [batchSize, setBatchSize] = useState(MAX_BATCH_ROWS)
  const [scan, setScan] = useState<{ rows: number; errors: string[]; done: boolean } | null>(null)
  const [dryRun, setDryRun] = useState<{ ok: boolean; message: string } | null>(null)
  const [journal, setJournal] = useState<ImportJournal | null>(() => loadJournal(store, key))
  const [resuming, setResuming] = useState(() => {
    const j = loadJournal(store, key)
    return j !== null && j.status !== 'done'
  })
  const [running, setRunning] = useState(false)
  const [status, setStatus] = useState('')
  const abortRef = useRef<AbortController | null>(null)
  const dialogRef = useRef<HTMLDivElement | null>(null)
  const fileRef = useRef<HTMLInputElement | null>(null)

  // Focus the first control on open; the opener restores focus on close.
  useEffect(() => { fileRef.current?.focus() }, [])

  function save(j: ImportJournal) {
    try {
      if (j.status === 'done') store.removeItem(key)
      else store.setItem(key, JSON.stringify(j))
    } catch {
      // Storage full/unavailable: the in-memory journal still drives this
      // session; resuming after a reload is then not possible.
    }
    setJournal({ ...j })
    setStatus(`${statusWord(j)} — ${describeJournal(j)}`)
  }

  function statusWord(j: ImportJournal): string {
    switch (j.status) {
      case 'running': return 'Importing'
      case 'paused': return 'Stopped'
      case 'needs-decision': return 'Needs a decision'
      case 'done': return 'Import complete'
      case 'error': return 'Import stopped by a file error'
      default: return 'Ready'
    }
  }

  // --- source selection and preview ---

  async function loadPreview(f: File, fmt: ImportFormat, csvOpts: CsvSourceOptions, keepMapping?: Record<string, ColumnMapping>) {
    setPreviewError(null)
    setScan(null)
    setDryRun(null)
    try {
      const p = await readPreview(f, fmt, csvOpts)
      setPreview(p)
      setMapping(keepMapping ?? autoMap(p.fields, targets))
    } catch (err) {
      setPreview(null)
      setPreviewError(err instanceof Error ? err.message : String(err))
    }
  }

  function chooseFile(f: File | null) {
    setFile(f)
    if (!f) {
      setPreview(null)
      return
    }
    if (resuming && journal) {
      const fp = { name: f.name, size: f.size, lastModified: f.lastModified }
      if (!sameFile(journal.file, fp)) {
        setPreviewError(`This is not the file the unfinished import started from (${journal.file.name}, ${journal.file.size.toLocaleString()} bytes). Select that exact file to resume, or discard the unfinished import.`)
        setPreview(null)
        return
      }
      const plan = journal.plan
      setFormat(plan.format)
      setCsv(plan.csv)
      setValues(plan.values)
      setBatchSize(plan.batchSize)
      void loadPreview(f, plan.format, plan.csv, plan.mapping)
      return
    }
    const d = detectFormat(f.name)
    const nextCsv = { ...csv, delimiter: d.delimiter ?? csv.delimiter }
    setFormat(d.format)
    setCsv(nextCsv)
    void loadPreview(f, d.format, nextCsv)
  }

  function changeFormat(fmt: ImportFormat) {
    setFormat(fmt)
    if (file) void loadPreview(file, fmt, csv)
  }

  function changeCsv(patch: Partial<CsvSourceOptions>) {
    const next = { ...csv, ...patch }
    setCsv(next)
    if (file) void loadPreview(file, format, next)
  }

  const problems = preview ? mappingProblems(mapping, targets) : []

  // --- whole-file check (client-side encoding of every row) ---

  async function scanFile() {
    if (!file) return
    const ctl = new AbortController()
    abortRef.current = ctl
    const registry = new FieldRegistry()
    const state = { rows: 0, errors: [] as string[], done: false }
    setScan({ ...state })
    try {
      for await (const rec of readSourceRecords(blobTextChunks(file), format, csv, registry)) {
        if (ctl.signal.aborted) break
        state.rows++
        try {
          encodeRecord(rec, targets, mapping, values)
        } catch (err) {
          if (state.errors.length < 20) state.errors.push(`row ${rec.row} (line ${rec.line}): ${err instanceof Error ? err.message : String(err)}`)
        }
        if (state.rows % 5000 === 0) setScan({ ...state, errors: [...state.errors] })
      }
      state.done = !ctl.signal.aborted
    } catch (err) {
      state.errors.push(err instanceof Error ? err.message : String(err))
      state.done = true
    }
    abortRef.current = null
    setScan({ ...state, errors: [...state.errors] })
  }

  // --- server dry-run of the first batch (rolled back) ---

  async function dryRunFirstBatch() {
    if (!preview || !meta.binding) return
    const rows: Array<Record<string, unknown>> = []
    const rowNumbers: number[] = []
    for (const rec of preview.records.slice(0, Math.min(batchSize, PREVIEW_RECORDS))) {
      try {
        rows.push(encodeRecord(rec, targets, mapping, values))
        rowNumbers.push(rec.row)
      } catch (err) {
        setDryRun({ ok: false, message: `row ${rec.row}: ${err instanceof Error ? err.message : String(err)}` })
        return
      }
    }
    try {
      const res = await api.previewOperations({
        connectionId,
        operations: rows.map(v => ({ op: 'insert' as const, schema: schemaName, table, binding: meta.binding!, values: v })),
      })
      setDryRun({ ok: true, message: `The first ${res.counts.insert ?? rows.length} rows would insert (validated by the database, then rolled back — nothing was written).` })
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      const m = /^operations\[(\d+)\]/.exec(msg)
      const row = m ? rowNumbers[Number(m[1])] : undefined
      setDryRun({ ok: false, message: row !== undefined ? `row ${row}: ${msg}` : msg })
    }
  }

  // --- running ---

  function deps(signal: AbortSignal): ImportDeps {
    return {
      sendBatch: transport?.sendBatch ?? (req => api.importBatch(req)),
      outcome: transport?.outcome ?? ((c, id) => api.importOutcome(c, id)),
      save,
      signal,
    }
  }

  async function freshMeta(): Promise<TableMeta> {
    try {
      return (await api.tableMeta(connectionId, schemaName, table)) ?? meta
    } catch {
      return meta
    }
  }

  async function run(start: ImportJournal) {
    if (!file) return
    const ctl = new AbortController()
    abortRef.current = ctl
    setRunning(true)
    let j = start
    try {
      // The binding pins the relation. On resume the connection epoch may
      // have changed (reconnect); the table must still be the SAME relation.
      const live = await freshMeta()
      if (live.binding && live.binding !== j.plan.binding) {
        if (bindingOid(live.binding) !== bindingOid(j.plan.binding)) {
          j.status = 'error'
          j.error = `${schemaName}.${table} is no longer the table this import started on (it was dropped and re-created); rows committed so far stay committed — start a new import`
          save(j)
          return
        }
        j.plan = { ...j.plan, binding: live.binding }
      }
      const d = deps(ctl.signal)
      if (j.pending && j.pending.state === 'sending') {
        // An interrupted batch is resolved through the server's record
        // before anything is sent again.
        j = await resolvePending(j, d)
        if (j.status === 'needs-decision') return
      }
      const registry = new FieldRegistry()
      j = await runImport(readSourceRecords(blobTextChunks(file), j.plan.format, j.plan.csv, registry), targets, j, d)
    } finally {
      abortRef.current = null
      setRunning(false)
      setResuming(j.status !== 'done' && j.status !== 'ready')
      onImported?.()
    }
  }

  function start() {
    if (!file || !meta.binding || problems.length > 0) return
    const j = newImportJournal({
      connectionId, schema: schemaName, table, binding: meta.binding,
      format, csv, values, mapping,
      batchSize: Math.max(1, Math.min(MAX_BATCH_ROWS, Math.floor(batchSize) || 1)),
    }, { name: file.name, size: file.size, lastModified: file.lastModified }, crypto.randomUUID())
    save(j)
    void run(j)
  }

  function decide(action: 'retry' | 'skip-row' | 'skip-batch' | 'mark-committed') {
    if (!journal) return
    const j = { ...journal, pending: journal.pending ? { ...journal.pending } : undefined, skipped: [...journal.skipped] }
    if (action === 'retry') retryPending(j)
    else if (action === 'skip-row') skipFailedRow(j)
    else if (action === 'skip-batch') skipPending(j)
    else markPendingCommitted(j)
    save(j)
    if (file) void run(j)
    else setStatus('Decision recorded — select the same file to continue the import')
  }

  function discard() {
    try { store.removeItem(key) } catch { /* nothing persisted */ }
    setJournal(null)
    setResuming(false)
    setStatus('')
    setPreviewError(null)
  }

  function stop() {
    abortRef.current?.abort()
  }

  // --- dialog keyboard: Escape closes (not while running), Tab stays inside ---

  function onKeyDown(e: KeyboardEvent) {
    if (e.key === 'Escape') {
      if (running) return
      e.preventDefault()
      onClose()
      return
    }
    if (e.key !== 'Tab' || !dialogRef.current) return
    const focusable = Array.from(dialogRef.current.querySelectorAll<HTMLElement>(
      'button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex="0"]'))
    if (focusable.length === 0) return
    const first = focusable[0]
    const last = focusable[focusable.length - 1]
    if (e.shiftKey && document.activeElement === first) {
      e.preventDefault()
      last.focus()
    } else if (!e.shiftKey && document.activeElement === last) {
      e.preventDefault()
      first.focus()
    }
  }

  const pending = journal?.pending
  const decision = journal?.status === 'needs-decision' && pending
  const titleId = `import-title-${schemaName}-${table}`

  return (
    <div class={s.backdrop}>
      <div
        class={s.dialog}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        ref={dialogRef}
        onKeyDown={onKeyDown}
      >
        <div class={s.head}>
          <h2 id={titleId} class={s.title}>Import into {schemaName}.{table}</h2>
          <button class={s.close} onClick={onClose} disabled={running} aria-label="Close import dialog">×</button>
        </div>

        {resuming && journal && journal.status !== 'done' && (
          <div class={s.resume} role="note">
            <p>
              An import of <strong>{journal.file.name}</strong> is unfinished: {describeJournal(journal)}.
              {journal.pending && ` Batch ${journal.pending.batch} (rows ${journal.pending.fromRow}–${journal.pending.toRow}) ${journal.pending.state === 'sending' ? 'was in flight — its outcome will be looked up before anything is sent' : 'needs a decision'}.`}
            </p>
            <p>Select the same file to resume it, or discard the record (rows already committed stay committed).</p>
            <button class={s.btn} onClick={discard} disabled={running}>Discard unfinished import</button>
          </div>
        )}

        <div class={s.row}>
          <label class={s.label} for={`${titleId}-file`}>Source file (CSV, JSON array or NDJSON)</label>
          <input
            id={`${titleId}-file`}
            ref={fileRef}
            type="file"
            accept=".csv,.tsv,.txt,.json,.ndjson,.jsonl,text/csv,application/json"
            disabled={running}
            onChange={e => chooseFile((e.target as HTMLInputElement).files?.[0] ?? null)}
          />
        </div>

        {file && !resuming && (
          <fieldset class={s.options} disabled={running}>
            <legend>Format</legend>
            <label class={s.inline}>
              Format
              <select value={format} onChange={e => changeFormat((e.target as HTMLSelectElement).value as ImportFormat)}>
                <option value="csv">CSV</option>
                <option value="json">JSON (array or newline-delimited)</option>
              </select>
            </label>
            {format === 'csv' && (
              <>
                <label class={s.inline}>
                  Delimiter
                  <select value={csv.delimiter} onChange={e => changeCsv({ delimiter: (e.target as HTMLSelectElement).value })}>
                    <option value=",">comma</option>
                    <option value=";">semicolon</option>
                    <option value={'\t'}>tab</option>
                    <option value="|">pipe</option>
                  </select>
                </label>
                <label class={s.inline}>
                  <input type="checkbox" checked={csv.header} onChange={e => changeCsv({ header: (e.target as HTMLInputElement).checked })} />
                  First row is a header
                </label>
                <label class={s.inline}>
                  Unquoted empty field means
                  <select
                    value={values.emptyUnquoted}
                    onChange={e => setValues({ ...values, emptyUnquoted: (e.target as HTMLSelectElement).value as 'null' | 'empty' })}
                  >
                    <option value="null">NULL (PostgreSQL COPY convention)</option>
                    <option value="empty">empty string</option>
                  </select>
                </label>
                <label class={s.inline}>
                  NULL marker (unquoted, exact)
                  <input
                    type="text"
                    class={s.marker}
                    value={values.nullMarker ?? ''}
                    placeholder="none"
                    onInput={e => {
                      const v = (e.target as HTMLInputElement).value
                      setValues({ ...values, nullMarker: v === '' ? null : v })
                    }}
                  />
                </label>
              </>
            )}
          </fieldset>
        )}

        {previewError && <div class={s.error} role="alert">{previewError}</div>}

        {preview && !resuming && (
          <>
            <table class={s.mapTable}>
              <caption class={s.caption}>Column mapping — {preview.fields.length} source field{preview.fields.length === 1 ? '' : 's'}{format === 'json' ? ` (discovered in the first ${PREVIEW_RECORDS} records)` : ''}</caption>
              <thead>
                <tr><th scope="col">Column</th><th scope="col">Type</th><th scope="col">Source</th></tr>
              </thead>
              <tbody>
                {targets.map(col => {
                  const m = mapping[col.name] ?? { kind: 'default' }
                  const value = m.kind === 'field' ? `f:${m.field}` : m.kind
                  return (
                    <tr key={col.name}>
                      <th scope="row" class={s.colName}>
                        {isRequired(col) && <span class={s.required} title="NOT NULL without a default">*</span>}
                        {col.name}
                      </th>
                      <td class={s.colType}>{col.type}{col.nullable ? '' : ' not null'}</td>
                      <td>
                        <select
                          aria-label={`Source for column ${col.name}`}
                          value={value}
                          disabled={running}
                          onChange={e => {
                            const v = (e.target as HTMLSelectElement).value
                            const next: ColumnMapping = v === 'default' ? { kind: 'default' } : v === 'null' ? { kind: 'null' } : { kind: 'field', field: v.slice(2) }
                            setMapping({ ...mapping, [col.name]: next })
                            setDryRun(null)
                            setScan(null)
                          }}
                        >
                          <option value="default">DEFAULT (omit column)</option>
                          {col.nullable && <option value="null">NULL (every row)</option>}
                          {preview.fields.map(f => <option key={f.id} value={`f:${f.id}`}>{f.label}</option>)}
                        </select>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>

            {problems.length > 0 && (
              <ul class={s.problems} role="alert" aria-label="Mapping problems">
                {problems.map(p => <li key={p}>{p}</li>)}
              </ul>
            )}

            <div class={s.previewWrap}>
              <table class={s.previewTable} aria-label="Preview of the first rows as they will be inserted">
                <thead>
                  <tr>
                    <th scope="col">row</th>
                    {targets.map(col => <th key={col.name} scope="col">{col.name}</th>)}
                  </tr>
                </thead>
                <tbody>
                  {preview.records.slice(0, PREVIEW_SHOWN).map(rec => (
                    <tr key={rec.row}>
                      <th scope="row">{rec.row}</th>
                      {targets.map(col => {
                        if (rec.shapeError) return <td key={col.name} class={s.cellError}>row {rec.shapeError}</td>
                        const c = cellPreview(rec, col, mapping[col.name] ?? { kind: 'default' }, values)
                        return (
                          <td key={col.name} class={c.kind === 'error' ? s.cellError : c.kind === 'value' ? undefined : s.cellMarker} title={c.text}>
                            {c.text === '' && c.kind === 'value' ? <span class={s.cellMarker}>(empty string)</span> : c.text}
                          </td>
                        )
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}

        {preview && (
          <div class={s.batching}>
            <label class={s.inline}>
              Rows per batch
              <input
                type="number"
                min={1}
                max={MAX_BATCH_ROWS}
                value={batchSize}
                disabled={running || resuming}
                onInput={e => setBatchSize(Number((e.target as HTMLInputElement).value))}
              />
            </label>
            <p class={s.atomicity}>
              Each batch of up to {Math.max(1, Math.min(MAX_BATCH_ROWS, Math.floor(batchSize) || 1))} rows is inserted in ONE
              transaction: a batch applies completely or not at all. The import as a whole is not atomic — batches that
              committed before a failure stay committed, and the report below names them.
            </p>
          </div>
        )}

        {scan && (
          <div class={s.scan} role="status">
            {scan.done ? 'Checked' : 'Checking…'} {scan.rows.toLocaleString()} row{scan.rows === 1 ? '' : 's'}
            {scan.done && scan.errors.length === 0 && ' — every row encodes for this mapping (the database still validates constraints on import)'}
            {scan.errors.length > 0 && (
              <ul class={s.problems}>
                {scan.errors.map(e => <li key={e}>{e}</li>)}
              </ul>
            )}
          </div>
        )}

        {dryRun && (
          <div class={dryRun.ok ? s.ok : s.error} role={dryRun.ok ? 'status' : 'alert'}>{dryRun.message}</div>
        )}

        {journal && journal.status !== 'ready' && (
          <div class={s.progress}>
            <div
              role="progressbar"
              aria-label="Rows committed"
              aria-valuemin={0}
              aria-valuemax={scan?.done ? scan.rows : undefined}
              aria-valuenow={journal.rowsCommitted}
              aria-valuetext={describeJournal(journal)}
              class={s.bar}
            >
              <div
                class={s.barFill}
                style={{ width: scan?.done && scan.rows > 0 ? `${Math.min(100, (journal.nextRow / scan.rows) * 100)}%` : running ? '100%' : '0%' }}
              />
            </div>
          </div>
        )}
        <div class={s.status} role="status" aria-live="polite">{status}</div>
        {journal?.status === 'error' && journal.error && (
          <div class={s.error} role="alert">{journal.error}</div>
        )}

        {decision && pending && (
          <div class={s.decision} role="alert">
            <p>
              Batch {pending.batch} (rows {pending.fromRow}–{pending.toRow}){' '}
              {pending.state === 'unknown' ? 'has an UNKNOWN outcome' : pending.state === 'invalid' ? 'was not sent' : 'failed — nothing of it was applied'}
              {pending.failedRow !== undefined && `; the cause is row ${pending.failedRow}`}:
            </p>
            <p class={s.decisionError}>{pending.error}</p>
            <div class={s.actions}>
              {pending.state !== 'unknown' && pending.failedRow !== undefined && (
                <button class={s.btn} onClick={() => decide('skip-row')}>Skip row {pending.failedRow} and continue</button>
              )}
              <button class={s.btn} onClick={() => decide('skip-batch')}>Skip rows {pending.fromRow}–{pending.toRow} and continue</button>
              {pending.state !== 'invalid' && <button class={s.btn} onClick={() => decide('retry')}>Retry the batch</button>}
              {pending.state === 'unknown' && (
                <button class={s.btn} onClick={() => decide('mark-committed')}>I checked: the rows are there — mark committed</button>
              )}
            </div>
          </div>
        )}

        <div class={s.footer}>
          {!running && preview && !resuming && (
            <>
              <button class={s.btn} onClick={scanFile}>Check whole file</button>
              <button class={s.btn} onClick={dryRunFirstBatch} disabled={problems.length > 0 || !meta.binding}>Dry-run first batch</button>
              <button class={s.btnPrimary} onClick={start} disabled={problems.length > 0 || !meta.binding}>Import</button>
            </>
          )}
          {!running && resuming && file && preview && journal && journal.status !== 'needs-decision' && journal.status !== 'error' && (
            <button class={s.btnPrimary} onClick={() => void run({ ...journal })}>Resume import</button>
          )}
          {scan && !scan.done && !running && <button class={s.btn} onClick={stop}>Stop checking</button>}
          {running && <button class={s.btn} onClick={stop}>Stop after this batch</button>}
          {!running && <button class={s.btn} onClick={onClose}>Close</button>}
        </div>
      </div>
    </div>
  )
}

