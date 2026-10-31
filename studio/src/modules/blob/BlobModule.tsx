import { useSignal } from '@preact/signals'
import { useEffect, useRef } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { useRequestOwner } from '../../lib/requestOwner'
import { queryMutationOrThrow as runMutation, api, mutationHeaders } from '../../lib/api'
import { exportCSV, exportJSON } from '../../lib/export'
import { isRlsDenied } from '../../lib/rls'
import { RlsNotice } from '../../components/RlsNotice'
import s from './BlobModule.module.css'

// Nucleus has one GLOBAL blob store keyed by string — there is no store name
// and no per-blob hash column. Listing is: BLOB_LIST(prefix) → JSON array of
// key strings, then BLOB_META(key) → { size, content_type, created_at,
// updated_at } (timestamps are epoch ms). Delete is BLOB_DELETE(key).
interface BlobEntry {
  id: string          // the blob key
  size: number
  contentType: string
  createdAt: number   // epoch ms
}

interface BlobModuleProps {
  name: string
}

const BASE = '/api'
const PAGE_SIZE = 50

export function BlobModule({ name }: BlobModuleProps) {
  const allKeys = useSignal<string[]>([])
  const blobs = useSignal<BlobEntry[]>([])
  const loading = useSignal(false)
  const selected = useSignal<BlobEntry | null>(null)
  const page = useSignal(0)

  // Upload state
  const uploading = useSignal(false)
  const uploadProgress = useSignal(0) // 0-100
  const dragging = useSignal(false)
  const fileInputRef = useRef<HTMLInputElement>(null)

  // Delete confirmation
  const confirmDeleteId = useSignal<string | null>(null)
  const confirmTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  // Download state
  const downloadingId = useSignal<string | null>(null)

  // RLS seals the specialty stores for non-superuser sessions
  const rlsDenied = useSignal<string | null>(null)

  const conn = activeConnection.value
  const requests = useRequestOwner(JSON.stringify([conn?.id, name]))
  const unavailable = useSignal<string | null>(null)
  const owner = useRef({ epoch: 0, alive: true, connection: conn?.id, name })
  const listGeneration = useRef(0)
  const pageGeneration = useRef(0)
  const uploadGeneration = useRef(0)
  const downloadGeneration = useRef(0)
  const xhrRef = useRef<XMLHttpRequest | null>(null)
  const downloadAbort = useRef<AbortController | null>(null)
  const selectionVersion = useRef(0)
  const deleteInFlight = useRef(new Set<string>())
  const confirmation = useRef<{ key: string; epoch: number; list: string[] } | null>(null)

  function invalidate() {
    requests.invalidate()
    owner.current.epoch++
    listGeneration.current++; pageGeneration.current++
    uploadGeneration.current++; downloadGeneration.current++
    xhrRef.current?.abort(); xhrRef.current = null
    downloadAbort.current?.abort(); downloadAbort.current = null
    if (confirmTimerRef.current) clearTimeout(confirmTimerRef.current)
    confirmation.current = null; confirmDeleteId.value = null
    allKeys.value = []; blobs.value = []; selected.value = null; page.value = 0
    loading.value = false; uploading.value = false; uploadProgress.value = 0
    downloadingId.value = null; unavailable.value = null; rlsDenied.value = null
  }
  if (owner.current.connection !== conn?.id || owner.current.name !== name) {
    invalidate(); owner.current.connection = conn?.id; owner.current.name = name
  }
  function capture(channel: string) {
    const ticket = requests.begin(channel)
    const epoch = owner.current.epoch
    const connectionId = conn?.id
    return { connectionId, owns: () => ticket() && owner.current.alive && epoch === owner.current.epoch && connectionId === activeConnection.value?.id }
  }
  function fail(err: unknown) {
    const msg = err instanceof Error ? err.message : String(err)
    unavailable.value = msg; rlsDenied.value = isRlsDenied(msg) ? msg : null
  }
  useEffect(() => {
    owner.current.alive = true
    let current = activeConnection.value
    const stop = activeConnection.subscribe(next => {
      if (next !== current) { current = next; invalidate() }
    })
    return () => { owner.current.alive = false; invalidate(); stop() }
  }, [])

  // A list instance and page request own their metadata; failures never invent bytes.
  async function load() {
    const binding = capture('list')
    if (!binding.connectionId || !binding.owns()) return
    const generation = ++listGeneration.current
    const pageTicket = ++pageGeneration.current
    loading.value = true; unavailable.value = null
    blobs.value = []; selected.value = null; selectionVersion.current++
    confirmation.current = null; confirmDeleteId.value = null
    const owns = () => binding.owns() && generation === listGeneration.current
    try {
      const r = await api.query(`SELECT BLOB_LIST('')`, binding.connectionId)
      if (!owns()) return
      if (r.error || r.canceled) throw new Error(r.error || 'Blob list canceled')
      if (!r.rows.length) throw new Error('Blob list unavailable')
      const cell = r.rows[0][0]
      const list = typeof cell === 'string' ? JSON.parse(cell) : cell
      if (!Array.isArray(list) || list.some(key => typeof key !== 'string')) throw new Error('Invalid blob list')
      const keys = parseKeys(list)
      allKeys.value = keys
      confirmation.current = null; confirmDeleteId.value = null
      selectionVersion.current++; selected.value = null
      page.value = Math.min(page.value, Math.max(0, Math.ceil(keys.length / PAGE_SIZE) - 1))
      await loadPage()
    } catch (err) { if (owns()) { blobs.value = []; fail(err) } }
    finally { if (owns() && pageTicket === pageGeneration.current) loading.value = false }
  }

  async function loadPage() {
    const binding = capture('page')
    if (!binding.connectionId || !binding.owns()) return
    const generation = ++pageGeneration.current
    const keys = allKeys.value
    const at = page.value
    const owns = () => binding.owns() && generation === pageGeneration.current && keys === allKeys.value && at === page.value
    loading.value = true; unavailable.value = null; blobs.value = []; selectionVersion.current++; selected.value = null
    confirmation.current = null; confirmDeleteId.value = null
    const entries: BlobEntry[] = []
    try {
      for (const key of keys.slice(at * PAGE_SIZE, (at + 1) * PAGE_SIZE)) {
        if (!owns()) return
        const r = await api.query(`SELECT BLOB_META('${key.replace(/'/g, "''")}')`, binding.connectionId)
        if (!owns()) return
        if (r.error || r.canceled) throw new Error(r.error || 'Blob metadata canceled')
        const cell = r.rows[0]?.[0]
        if (cell == null) throw new Error(`Blob metadata unavailable: ${key}`)
        const meta = typeof cell === 'string' ? JSON.parse(cell) : cell
        if (!meta || typeof meta !== 'object' || meta.size == null || !Number.isSafeInteger(Number(meta.size)) || Number(meta.size) < 0) throw new Error(`Invalid blob size: ${key}`)
        entries.push(parseMeta(key, meta))
      }
      if (owns()) blobs.value = entries
    } catch (err) { if (owns()) fail(err) }
    finally { if (owns()) loading.value = false }
  }

  useEffect(() => { void load() }, [conn?.id, name])
  useEffect(() => { void loadPage() }, [page.value])

  async function uploadFile(file: File) {
    if (uploading.value) return
    const binding = capture('upload')
    if (!binding.connectionId || !binding.owns()) return
    const generation = ++uploadGeneration.current
    const keys = allKeys.value
    const at = page.value
    const selectionRevision = selectionVersion.current
    const owns = () => binding.owns() && generation === uploadGeneration.current
    uploading.value = true; uploadProgress.value = 0
    try {
      const formData = new FormData()
      formData.append('connectionId', binding.connectionId)
      formData.append('store', name)
      formData.append('file', file) // File bytes are sent untouched, never text-decoded.
      await new Promise<void>((resolve, reject) => {
        const xhr = new XMLHttpRequest()
        xhrRef.current = xhr
        xhr.open('POST', `${BASE}/blob/upload`)
        xhr.upload.onprogress = ev => {
          if (owns() && ev.lengthComputable) uploadProgress.value = Math.round(ev.loaded / ev.total * 100)
        }
        xhr.onload = () => {
          if (xhr.status < 200 || xhr.status >= 300) { reject(new Error(xhr.responseText || `HTTP ${xhr.status}`)); return }
          try {
            const receipt = JSON.parse(xhr.responseText)
            if (typeof receipt.id !== 'string' || !receipt.id || receipt.size !== file.size) throw new Error('Upload receipt unavailable; verify stored bytes before retrying')
            resolve()
          } catch (err) { reject(err) }
        }
        xhr.onerror = () => reject(new Error('Upload outcome unavailable; verify before retrying'))
        xhr.onabort = () => reject(new Error('Upload canceled; verify outcome before retrying'))
        mutationHeaders().then(headers => {
          if (!owns()) { reject(new Error('Upload view changed before dispatch')); return }
          for (const [k, v] of Object.entries(headers)) if (k.toLowerCase() !== 'content-type') xhr.setRequestHeader(k, v)
          xhr.send(formData)
        }, reject)
      })
      if (!owns()) return
      toast('success', `Uploaded ${file.name}`)
      if (keys === allKeys.value && at === page.value && selectionRevision === selectionVersion.current) await load()
    } catch (err) { if (owns()) { fail(err); toast('error', err instanceof Error ? err.message : String(err)) } }
    finally { if (owns()) { xhrRef.current = null; uploading.value = false; uploadProgress.value = 0 } }
  }

  function onFileSelected(ev: Event) {
    const input = ev.target as HTMLInputElement
    const file = input.files?.[0]
    if (file) uploadFile(file)
    // Reset input so same file can be re-uploaded
    input.value = ''
  }

  function openFileDialog() {
    fileInputRef.current?.click()
  }

  // Drag and drop handlers
  function onDragEnter(ev: DragEvent) {
    ev.preventDefault()
    ev.stopPropagation()
    dragging.value = true
  }

  function onDragOver(ev: DragEvent) {
    ev.preventDefault()
    ev.stopPropagation()
    dragging.value = true
  }

  function onDragLeave(ev: DragEvent) {
    ev.preventDefault()
    ev.stopPropagation()
    dragging.value = false
  }

  function onDrop(ev: DragEvent) {
    ev.preventDefault()
    ev.stopPropagation()
    dragging.value = false
    const file = ev.dataTransfer?.files[0]
    if (file) uploadFile(file)
  }

  async function downloadBlob(blob: BlobEntry) {
    const binding = capture('download')
    const rows = blobs.value
    const selection = selected.value
    if (!binding.connectionId || !binding.owns() || !(rows.includes(blob) || selection === blob && rows.some(row => row.id === blob.id && row.size === blob.size))) return
    downloadAbort.current?.abort()
    const controller = new AbortController()
    downloadAbort.current = controller
    const generation = ++downloadGeneration.current
    const owns = () => binding.owns() && generation === downloadGeneration.current && rows === blobs.value && selection === selected.value
    downloadingId.value = blob.id
    try {
      const res = await fetch(`${BASE}/blob/${encodeURIComponent(blob.id)}/data?connectionId=${encodeURIComponent(binding.connectionId)}&store=${encodeURIComponent(name)}`, { signal: controller.signal })
      if (!owns()) return
      if (!res.ok) throw new Error(await res.text() || `HTTP ${res.status}`)
      const data = await res.blob()
      if (!owns()) return
      if (data.size !== blob.size) throw new Error('Blob byte count changed; refresh metadata before downloading')
      const url = URL.createObjectURL(data)
      try {
        const a = document.createElement('a')
        a.href = url; a.download = blob.id
        document.body.appendChild(a)
        try { a.click() } finally { a.remove() }
      } finally { URL.revokeObjectURL(url) }
      toast('success', `Downloaded ${blob.id}`)
    } catch (err) { if (owns()) { fail(err); toast('error', err instanceof Error ? err.message : String(err)) } }
    finally { if (binding.owns() && generation === downloadGeneration.current) { downloadingId.value = null; downloadAbort.current = null } }
  }

  function requestDelete(blob: BlobEntry, ev: Event) {
    ev.stopPropagation()
    const id = blob.id
    const binding = capture('confirmation')
    if (!binding.connectionId || !binding.owns() || !blobs.value.includes(blob)) return
    const key = JSON.stringify([binding.connectionId, name, id])
    if (deleteInFlight.current.has(key)) return
    const prior = confirmation.current
    if (prior?.key === key && prior.epoch === owner.current.epoch && prior.list === allKeys.value) {
      if (confirmTimerRef.current) clearTimeout(confirmTimerRef.current)
      confirmation.current = null; confirmDeleteId.value = null
      void doDelete(id, key, capture(`delete:${key}`))
    } else {
      const token = { key, epoch: owner.current.epoch, list: allKeys.value }
      confirmation.current = token; confirmDeleteId.value = id
      if (confirmTimerRef.current) clearTimeout(confirmTimerRef.current)
      confirmTimerRef.current = setTimeout(() => {
        if (confirmation.current === token) { confirmation.current = null; confirmDeleteId.value = null }
      }, 3000)
    }
  }

  async function doDelete(id: string, key: string, binding: ReturnType<typeof capture>) {
    if (!binding.connectionId || !binding.owns()) return
    const rows = blobs.value
    const keys = allKeys.value
    const selection = selected.value
    const selectionRevision = selectionVersion.current
    const at = page.value
    deleteInFlight.current.add(key)
    try {
      await queryMutationOrThrow(`SELECT BLOB_DELETE('${id.replace(/'/g, "''")}')`, binding.connectionId)
      if (!binding.owns()) return
      toast('info', `Deleted blob ${id}`)
      if (selectionRevision === selectionVersion.current && selection === selected.value && selection?.id === id) selected.value = null
      if (rows === blobs.value && keys === allKeys.value && at === page.value && selectionRevision === selectionVersion.current && selected.value === null) await load()
    } catch (err) { if (binding.owns()) { fail(err); toast('error', err instanceof Error ? err.message : String(err)) } }
    finally { deleteInFlight.current.delete(key) }
  }

  const hasNextPage = (page.value + 1) * PAGE_SIZE < allKeys.value.length

  return (
    <div
      class={s.layout}
      onDragEnter={onDragEnter}
      onDragOver={onDragOver}
      onDragLeave={onDragLeave}
      onDrop={onDrop}
    >
      {rlsDenied.value && <RlsNotice detail={rlsDenied.value} />}
      {unavailable.value && <div role="alert">Blob data unavailable: {unavailable.value}</div>}
      {/* Hidden file input */}
      <input
        ref={fileInputRef}
        type="file"
        class={s.hiddenInput}
        onChange={onFileSelected}
      />

      {/* Drag overlay */}
      {dragging.value && (
        <div class={s.dropOverlay}>
          <div class={s.dropOverlayInner}>
            <div class={s.dropIcon}>&#8681;</div>
            <div class={s.dropText}>Drop file to upload</div>
          </div>
        </div>
      )}

      <div class={s.toolbar}>
        <span class={s.storeName}>{name}</span>
        <span class={s.blobCount}>{allKeys.value.length} blobs</span>
        <button class={s.uploadBtn} onClick={openFileDialog} disabled={uploading.value} title="Upload blob">
          {uploading.value ? 'Uploading...' : 'Upload'}
        </button>
        <button class={s.refreshBtn} onClick={load} disabled={loading.value}>&#8634;</button>
        <button
          class={s.exportBtn}
          onClick={() => {
            const data = blobs.value.map(b => ({
              key: b.id,
              size: b.size as unknown,
              contentType: b.contentType,
              createdAt: b.createdAt as unknown,
            }))
            exportCSV(data, `blobs.csv`)
          }}
          disabled={blobs.value.length === 0}
          title="Export CSV"
        >CSV</button>
        <button
          class={s.exportBtn}
          onClick={() => exportJSON(blobs.value, `blobs.json`)}
          disabled={blobs.value.length === 0}
          title="Export JSON"
        >JSON</button>
      </div>

      {/* Upload progress bar */}
      {uploading.value && (
        <div class={s.progressBarWrap}>
          <div class={s.progressBar} style={{ width: `${uploadProgress.value}%` }} />
          <span class={s.progressText}>{uploadProgress.value}%</span>
        </div>
      )}

      {/* Drop zone hint when empty */}
      {!unavailable.value && !loading.value && blobs.value.length === 0 && !uploading.value && (
        <div class={s.dropZone} onClick={openFileDialog}>
          <div class={s.dropZoneIcon}>&#8681;</div>
          <div class={s.dropZoneText}>Drag & drop files here or click to upload</div>
        </div>
      )}

      <div class={s.table}>
        <div class={s.thead}>
          <span class={s.col} style={{ flex: 2 }}>Key</span>
          <span class={s.col}>Type</span>
          <span class={s.col}>Size</span>
          <span class={s.col}>Created</span>
          <span class={s.colAction} />
          <span class={s.colAction} />
        </div>

        <div class={s.tbody}>
          {loading.value && <div class={s.msg}>Loading...</div>}
          {!unavailable.value && !loading.value && blobs.value.length === 0 && <div class={s.msg}>No blobs</div>}
          {blobs.value.map(b => {
            const isConfirming = confirmDeleteId.value === b.id
            const isDownloading = downloadingId.value === b.id
            return (
              <div
                key={b.id}
                class={`${s.row} ${selected.value?.id === b.id ? s.rowActive : ''}`}
                onClick={() => { selectionVersion.current++; selected.value = selected.value?.id === b.id ? null : { ...b } }}
              >
                <span class={s.col} style={{ flex: 2 }} title={b.id}>
                  <span class={s.mono}>{b.id.length > 24 ? b.id.slice(0, 24) + '...' : b.id}</span>
                </span>
                <span class={s.col}>
                  <span class={s.contentType}>{b.contentType || '—'}</span>
                </span>
                <span class={s.col}>{formatBytes(b.size)}</span>
                <span class={s.col}>{fmtDate(b.createdAt)}</span>
                <span class={s.colAction}>
                  <button
                    class={s.downloadBtn}
                    onClick={ev => { ev.stopPropagation(); downloadBlob(b) }}
                    disabled={isDownloading}
                    title="Download"
                  >{isDownloading ? '...' : '⤓'}</button>
                </span>
                <span class={s.colAction}>
                  <button
                    class={`${s.deleteBtn} ${isConfirming ? s.deleteBtnConfirm : ''}`}
                    onClick={ev => requestDelete(b, ev)}
                    title={isConfirming ? 'Click again to confirm' : 'Delete blob'}
                  >{isConfirming ? 'Confirm?' : '×'}</button>
                </span>
              </div>
            )
          })}
        </div>
      </div>

      {selected.value && (
        <div class={s.detail}>
          <div class={s.detailTitle}>Blob details</div>
          <div class={s.detailGrid}>
            <span class={s.detailKey}>Key</span>       <span class={s.detailVal}>{selected.value.id}</span>
            <span class={s.detailKey}>Size</span>      <span class={s.detailVal}>{formatBytes(selected.value.size)} ({selected.value.size.toLocaleString()} bytes)</span>
            <span class={s.detailKey}>Type</span>      <span class={s.detailVal}>{selected.value.contentType || 'unknown'}</span>
            <span class={s.detailKey}>Created</span>   <span class={s.detailVal}>{fmtDate(selected.value.createdAt)}</span>
          </div>
          <div class={s.detailActions}>
            <button class={s.detailDownloadBtn} onClick={() => { if (selected.value) downloadBlob(selected.value) }} disabled={downloadingId.value === selected.value.id}>
              {downloadingId.value === selected.value.id ? 'Downloading...' : 'Download'}
            </button>
          </div>
        </div>
      )}

      <div class={s.pagination}>
        <button class={s.pageBtn} onClick={() => { page.value-- }} disabled={page.value === 0}>&larr; Prev</button>
        <span class={s.pageNum}>Page {page.value + 1}</span>
        <button class={s.pageBtn} onClick={() => { page.value++ }} disabled={!hasNextPage}>Next &rarr;</button>
      </div>
    </div>
  )
}

// Parse the JSON array of key strings from BLOB_LIST.
export function parseKeys(cell: unknown): string[] {
  if (cell == null) throw new Error('Blob list unavailable')
  let arr: unknown
  if (typeof cell === 'string') {
    try { arr = JSON.parse(cell) } catch { throw new Error('Invalid blob list') }
  } else {
    arr = cell
  }
  if (!Array.isArray(arr)) throw new Error('Invalid blob list')
  return arr.map(String)
}

// Parse a BLOB_META JSON cell into a BlobEntry (key comes from the caller).
export function parseMeta(key: string, cell: unknown): BlobEntry {
  const base: BlobEntry = { id: key, size: 0, contentType: '', createdAt: 0 }
  if (cell == null) return base
  let obj: Record<string, unknown>
  if (typeof cell === 'string') {
    try { obj = JSON.parse(cell) } catch { return base }
  } else if (typeof cell === 'object') {
    obj = cell as Record<string, unknown>
  } else {
    return base
  }
  return {
    id: key,
    size: Number(obj.size ?? 0),
    contentType: String(obj.content_type ?? ''),
    createdAt: Number(obj.created_at ?? 0),
  }
}

function formatBytes(n: number) {
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MB`
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GB`
}

// createdAt is epoch milliseconds (0 = unknown).
function fmtDate(ms: number) {
  if (!ms) return '—'
  try { return new Date(ms).toLocaleString() } catch { return String(ms) }
}

const queryMutationOrThrow = (sql: string, connectionId: string, params?: unknown[]) => runMutation(sql, connectionId, params, api.query)
