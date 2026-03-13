import { useSignal } from '@preact/signals'
import { useEffect, useRef, useCallback } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { exportCSV, exportJSON } from '../../lib/export'
import s from './BlobModule.module.css'

interface BlobEntry {
  id: string
  size: number
  contentType: string
  createdAt: string
  hash: string
}

interface BlobModuleProps {
  name: string
}

const BASE = '/api'

export function BlobModule({ name }: BlobModuleProps) {
  const blobs = useSignal<BlobEntry[]>([])
  const loading = useSignal(false)
  const selected = useSignal<BlobEntry | null>(null)
  const page = useSignal(0)
  const limit = 50

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

  async function load() {
    loading.value = true
    try {
      const r = await api.query(
        `SELECT id, size, content_type, created_at, hash
         FROM blob_list('${name}', ${limit}, ${page.value * limit})`,
        conn.id
      )
      if (r.error) throw new Error(r.error)
      blobs.value = r.rows.map(row => ({
        id: String(row[0]),
        size: Number(row[1]),
        contentType: String(row[2] ?? ''),
        createdAt: String(row[3] ?? ''),
        hash: String(row[4] ?? ''),
      }))
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      loading.value = false
    }
  }

  useEffect(() => { load() }, [name, page.value])

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
      formData.append('store', name)
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
      const res = await fetch(`${BASE}/blob/${encodeURIComponent(blob.id)}/data?connectionId=${encodeURIComponent(conn.id)}&store=${encodeURIComponent(name)}`)
      if (!res.ok) {
        const text = await res.text()
        throw new Error(text || `HTTP ${res.status}`)
      }
      const data = await res.blob()
      const url = URL.createObjectURL(data)
      const a = document.createElement('a')
      a.href = url
      // Use content type to guess extension, fallback to blob id
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
      await api.query(`SELECT blob_delete('${name}', '${id}')`, conn.id)
      if (selected.value?.id === id) selected.value = null
      toast('info', `Deleted blob ${id.slice(0, 8)}...`)
      await load()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    }
  }

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
        <span class={s.blobCount}>{blobs.value.length} blobs</span>
        <button class={s.uploadBtn} onClick={openFileDialog} disabled={uploading.value} title="Upload blob">
          {uploading.value ? 'Uploading...' : 'Upload'}
        </button>
        <button class={s.refreshBtn} onClick={load} disabled={loading.value}>&#8634;</button>
        <button
          class={s.exportBtn}
          onClick={() => {
            const data = blobs.value.map(b => ({
              id: b.id,
              size: b.size as unknown,
              contentType: b.contentType,
              createdAt: b.createdAt,
              hash: b.hash,
            }))
            exportCSV(data, `blobs-${name}.csv`)
          }}
          disabled={blobs.value.length === 0}
          title="Export CSV"
        >CSV</button>
        <button
          class={s.exportBtn}
          onClick={() => exportJSON(blobs.value, `blobs-${name}.json`)}
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
          <span class={s.col} style={{ flex: 2 }}>ID</span>
          <span class={s.col}>Type</span>
          <span class={s.col}>Size</span>
          <span class={s.col}>Hash</span>
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
                  <span class={s.mono}>{b.id.slice(0, 16)}...</span>
                </span>
                <span class={s.col}>
                  <span class={s.contentType}>{b.contentType || '\u2014'}</span>
                </span>
                <span class={s.col}>{formatBytes(b.size)}</span>
                <span class={s.col} title={b.hash}>
                  <span class={s.mono}>{b.hash.slice(0, 10)}...</span>
                </span>
                <span class={s.col}>{fmtDate(b.createdAt)}</span>
                <span class={s.colAction}>
                  <button
                    class={s.downloadBtn}
                    onClick={ev => { ev.stopPropagation(); downloadBlob(b) }}
                    disabled={isDownloading}
                    title="Download"
                  >{isDownloading ? '...' : '\u2913'}</button>
                </span>
                <span class={s.colAction}>
                  <button
                    class={`${s.deleteBtn} ${isConfirming ? s.deleteBtnConfirm : ''}`}
                    onClick={ev => requestDelete(b.id, ev)}
                    title={isConfirming ? 'Click again to confirm' : 'Delete blob'}
                  >{isConfirming ? 'Confirm?' : '\u00d7'}</button>
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
            <span class={s.detailKey}>ID</span>        <span class={s.detailVal}>{selected.value.id}</span>
            <span class={s.detailKey}>Size</span>      <span class={s.detailVal}>{formatBytes(selected.value.size)} ({selected.value.size.toLocaleString()} bytes)</span>
            <span class={s.detailKey}>Type</span>      <span class={s.detailVal}>{selected.value.contentType || 'unknown'}</span>
            <span class={s.detailKey}>Hash</span>      <span class={s.detailVal}>{selected.value.hash}</span>
            <span class={s.detailKey}>Created</span>   <span class={s.detailVal}>{selected.value.createdAt}</span>
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
        <button class={s.pageBtn} onClick={() => { page.value++ }} disabled={blobs.value.length < limit}>Next &rarr;</button>
      </div>
    </div>
  )
}

function formatBytes(n: number) {
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MB`
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GB`
}

function fmtDate(val: string) {
  if (!val) return '\u2014'
  try { return new Date(val).toLocaleString() } catch { return val }
}
