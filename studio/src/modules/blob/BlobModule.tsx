import { useSignal } from '@preact/signals'
import { useEffect, useRef, useCallback } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { exportCSV, exportJSON } from '../../lib/export'
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

  const conn = activeConnection.value!

  // Fetch all keys, then metadata for the visible page.
  async function load() {
    loading.value = true
    try {
      const listRes = await api.query(`SELECT BLOB_LIST('')`, conn.id)
      if (listRes.error) throw new Error(listRes.error)
      const cell = listRes.rows.length > 0 ? listRes.rows[0][0] : null
      const keys = parseKeys(cell)
      allKeys.value = keys
      // Clamp page if the store shrank
      const maxPage = Math.max(0, Math.ceil(keys.length / PAGE_SIZE) - 1)
      if (page.value > maxPage) page.value = maxPage
      await loadPage()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      loading.value = false
    }
  }

  // Load metadata for the current page's keys.
  async function loadPage() {
    const start = page.value * PAGE_SIZE
    const pageKeys = allKeys.value.slice(start, start + PAGE_SIZE)
    const entries: BlobEntry[] = []
    for (const key of pageKeys) {
      try {
        const metaRes = await api.query(
          `SELECT BLOB_META('${key.replace(/'/g, "''")}')`,
          conn.id
        )
        const metaCell = !metaRes.error && metaRes.rows.length > 0 ? metaRes.rows[0][0] : null
        entries.push(parseMeta(key, metaCell))
      } catch {
        entries.push({ id: key, size: 0, contentType: '', createdAt: 0 })
      }
    }
    blobs.value = entries
  }

  useEffect(() => { load() }, [name])
  useEffect(() => { loadPage() }, [page.value])

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (confirmTimerRef.current) clearTimeout(confirmTimerRef.current)
    }
  }, [])

  // Upload a file via multipart form data
  async function uploadFile(file: File) {
    uploading.value = true
    uploadProgress.value = 0
    try {
      const formData = new FormData()
      formData.append('connectionId', conn.id)
      formData.append('file', file)

      await new Promise<void>((resolve, reject) => {
        const xhr = new XMLHttpRequest()
        xhr.open('POST', `${BASE}/blob/upload`)

        xhr.upload.onprogress = (ev) => {
          if (ev.lengthComputable) {
            uploadProgress.value = Math.round((ev.loaded / ev.total) * 100)
          }
        }

        xhr.onload = () => {
          if (xhr.status >= 200 && xhr.status < 300) {
            resolve()
          } else {
            reject(new Error(xhr.responseText || `HTTP ${xhr.status}`))
          }
        }

        xhr.onerror = () => reject(new Error('Upload failed'))
        xhr.send(formData)
      })

      toast('success', `Uploaded ${file.name}`)
      await load()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      uploading.value = false
      uploadProgress.value = 0
    }
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

  // Download blob
  async function downloadBlob(blob: BlobEntry) {
    downloadingId.value = blob.id
    try {
      const res = await fetch(`${BASE}/blob/${encodeURIComponent(blob.id)}/data?connectionId=${encodeURIComponent(conn.id)}`)
      if (!res.ok) {
        const text = await res.text()
        throw new Error(text || `HTTP ${res.status}`)
      }
      const data = await res.blob()
      const url = URL.createObjectURL(data)
      const a = document.createElement('a')
      a.href = url
      a.download = blob.id
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      URL.revokeObjectURL(url)
      toast('success', `Downloaded ${blob.id.slice(0, 16)}...`)
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      downloadingId.value = null
    }
  }

  // Delete with confirmation
  const requestDelete = useCallback((id: string, ev: Event) => {
    ev.stopPropagation()
    if (confirmDeleteId.value === id) {
      if (confirmTimerRef.current) clearTimeout(confirmTimerRef.current)
      confirmDeleteId.value = null
      doDelete(id)
    } else {
      confirmDeleteId.value = id
      if (confirmTimerRef.current) clearTimeout(confirmTimerRef.current)
      confirmTimerRef.current = setTimeout(() => {
        confirmDeleteId.value = null
      }, 3000)
    }
  }, [])

  async function doDelete(id: string) {
    try {
      await api.query(`SELECT BLOB_DELETE('${id.replace(/'/g, "''")}')`, conn.id)
      if (selected.value?.id === id) selected.value = null
      toast('info', `Deleted blob ${id.slice(0, 8)}...`)
      await load()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    }
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
      {!loading.value && blobs.value.length === 0 && !uploading.value && (
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
          {!loading.value && blobs.value.length === 0 && <div class={s.msg}>No blobs</div>}
          {blobs.value.map(b => {
            const isConfirming = confirmDeleteId.value === b.id
            const isDownloading = downloadingId.value === b.id
            return (
              <div
                key={b.id}
                class={`${s.row} ${selected.value?.id === b.id ? s.rowActive : ''}`}
                onClick={() => { selected.value = selected.value?.id === b.id ? null : b }}
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
                    onClick={ev => requestDelete(b.id, ev)}
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
  if (cell == null) return []
  let arr: unknown
  if (typeof cell === 'string') {
    try { arr = JSON.parse(cell) } catch { return [] }
  } else {
    arr = cell
  }
  if (!Array.isArray(arr)) return []
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
