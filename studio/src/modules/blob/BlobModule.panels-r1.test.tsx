import { beforeEach, afterEach, describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import { act } from 'preact/test-utils'
import { BlobModule } from './BlobModule'
import { activeConnection, toasts } from '../../lib/store'
import type { QueryResult } from '../../lib/types'
vi.mock('../../lib/api', async original => {
  const m = await original<typeof import('../../lib/api')>()
  return { ...m, api: { ...m.api, query: vi.fn(), cancelQuery: vi.fn() }, mutationHeaders: vi.fn() }
})
import { api, mutationHeaders } from '../../lib/api'
const query = vi.mocked(api.query)
const ok = (cell: unknown = 0): QueryResult => ({ columns: ['v'], rows: [[cell]], rowCount: 1, duration: 0 })
function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (reason: unknown) => void
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no })
  return { promise, resolve, reject }
}
const connect = (id: string) => act(() => { activeConnection.value = { id, name: id, url: 'pg://test', isNucleus: true } })
const finish = async (fn: () => void) => { await act(async () => { fn(); await Promise.resolve() }) }
beforeEach(() => {
  connect('c1'); toasts.value = []; query.mockReset()
  vi.mocked(api.cancelQuery).mockReset().mockResolvedValue({} as never)
  vi.mocked(mutationHeaders).mockReset().mockResolvedValue({})
})
afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.unstubAllGlobals(); vi.useRealTimers() })

class UploadXHR {
  static instances: UploadXHR[] = []
  upload = { onprogress: null as ((ev: { lengthComputable: boolean; loaded: number; total: number }) => void) | null }
  onload: (() => void) | null = null
  onerror: (() => void) | null = null
  onabort: (() => void) | null = null
  status = 200
  responseText = ''
  open = vi.fn()
  setRequestHeader = vi.fn()
  send = vi.fn<(body: FormData) => void>()
  abort = vi.fn(() => this.onabort?.())
  constructor() { UploadXHR.instances.push(this) }
}
const base = async (sql: string) => sql.includes('BLOB_LIST') ? ok('["same-key"]') : ok('{"size":3,"content_type":"application/octet-stream","created_at":0}')
const loaded = async () => { render(<BlobModule name="blobs" />); await screen.findByText('same-key') }

describe('Blob actual component request/list/selection ownership', () => {
  it('reloads on connection change and ignores a late old list without fetching its metadata', async () => {
    const old = deferred<QueryResult>()
    query.mockImplementation((sql, conn) => sql.includes('BLOB_LIST') && conn === 'c1' ? old.promise : base(sql))
    render(<BlobModule name="blobs" />)
    await waitFor(() => expect(query).toHaveBeenCalled())
    connect('c2')
    await screen.findByText('same-key')
    await finish(() => old.resolve(ok('["old-key"]')))
    expect(screen.queryByText('old-key')).toBeNull()
    expect(query.mock.calls.some(([sql]) => sql.includes("BLOB_META('old-key')"))).toBe(false)
  })

  it('binds confirmation to connection and sends the confirmed c2 target once', async () => {
    query.mockImplementation(sql => base(sql))
    await loaded()
    fireEvent.click(screen.getByTitle('Delete blob'))
    connect('c2')
    await waitFor(() => expect(screen.getByTitle('Delete blob')).toBeTruthy())
    fireEvent.click(screen.getByTitle('Delete blob'))
    expect(query.mock.calls.filter(([sql]) => sql.includes('BLOB_DELETE'))).toHaveLength(0)
    fireEvent.click(screen.getByTitle('Click again to confirm'))
    await waitFor(() => expect(query.mock.calls.filter(([sql]) => sql.includes('BLOB_DELETE'))).toHaveLength(1))
    expect(query.mock.calls.find(([sql]) => sql.includes('BLOB_DELETE'))?.[1]).toBe('c2')
  })

  it('a deferred delete does not clear a reopened selection or refresh over it', async () => {
    const write = deferred<QueryResult>()
    query.mockImplementation(sql => sql.includes('BLOB_DELETE') ? write.promise : base(sql))
    await loaded()
    const row = screen.getByText('same-key')
    fireEvent.click(row)
    fireEvent.click(screen.getByTitle('Delete blob')); fireEvent.click(screen.getByTitle('Click again to confirm'))
    fireEvent.click(row); fireEvent.click(row)
    const lists = query.mock.calls.filter(([sql]) => sql.includes('BLOB_LIST')).length
    await finish(() => write.resolve(ok()))
    expect(screen.getByText('Blob details')).toBeTruthy()
    expect(query.mock.calls.filter(([sql]) => sql.includes('BLOB_LIST'))).toHaveLength(lists)
  })

  it('page metadata is fenced and stops old sequential continuations', async () => {
    const old = deferred<QueryResult>()
    const keys = Array.from({ length: 51 }, (_, i) => `K${i}`)
    query.mockImplementation(sql => sql.includes('BLOB_LIST') ? Promise.resolve(ok(JSON.stringify(keys))) : sql.includes("BLOB_META('K0')") ? old.promise : base(sql))
    render(<BlobModule name="blobs" />)
    await waitFor(() => expect(query.mock.calls.some(([sql]) => sql.includes("BLOB_META('K0')"))).toBe(true))
    fireEvent.click(screen.getByText(/Next/, { selector: 'button' }))
    await screen.findByText('K50')
    await finish(() => old.resolve(ok('{"size":3}')))
    expect(screen.queryByText('K0')).toBeNull()
    expect(query.mock.calls.some(([sql]) => sql.includes("BLOB_META('K1')"))).toBe(false)
  })

  it('metadata failure renders unavailable instead of a fabricated zero-byte row', async () => {
    query.mockImplementation(async sql => sql.includes('BLOB_LIST') ? ok('["same-key"]') : { ...ok(), error: 'metadata denied' })
    render(<BlobModule name="blobs" />)
    expect((await screen.findByRole('alert')).textContent).toContain('metadata denied')
    expect(screen.queryByText('0 B')).toBeNull(); expect(screen.queryByText('No blobs')).toBeNull()
  })

  it('sends the original binary File and rejects stale auth-header dispatch on unmount', async () => {
    query.mockImplementation(sql => base(sql))
    UploadXHR.instances = []; vi.stubGlobal('XMLHttpRequest', UploadXHR)
    await loaded()
    const bytes = new Uint8Array([0, 255, 128])
    const file = new File([bytes], 'binary.bin', { type: 'application/octet-stream' })
    fireEvent.change(document.querySelector('input[type=file]')!, { target: { files: [file] } })
    await waitFor(() => expect(UploadXHR.instances[0]?.send).toHaveBeenCalledOnce())
    const body = UploadXHR.instances[0].send.mock.calls[0][0] as FormData
    const sent = body.get('file') as File
    expect(new Uint8Array(await sent.arrayBuffer())).toEqual(bytes)
    expect(body.get('connectionId')).toBe('c1'); expect(body.get('store')).toBe('blobs')
    cleanup()
    expect(UploadXHR.instances[0].abort).toHaveBeenCalledOnce()
    const headers = deferred<Record<string, string>>()
    vi.mocked(mutationHeaders).mockReturnValue(headers.promise)
    await loaded()
    fireEvent.change(document.querySelector('input[type=file]')!, { target: { files: [file] } })
    await waitFor(() => expect(UploadXHR.instances).toHaveLength(2))
    cleanup()
    await finish(() => headers.resolve({ 'X-Test': 'session' }))
    expect(UploadXHR.instances[1].send).not.toHaveBeenCalled()
    expect(toasts.value.some(t => t.kind === 'success')).toBe(false)
  })

  it('download byte mismatch refuses artifact creation', async () => {
    query.mockImplementation(sql => base(sql)); await loaded()
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, blob: async () => new Blob([new Uint8Array([1, 2])]) }))
    const create = vi.spyOn(URL, 'createObjectURL')
    fireEvent.click(screen.getByTitle('Download'))
    expect((await screen.findByRole('alert')).textContent).toContain('byte count changed')
    expect(create).not.toHaveBeenCalled()
  })

  it('successful download preserves binary bytes and releases the object URL', async () => {
    query.mockImplementation(sql => base(sql)); await loaded()
    const bytes = new Uint8Array([0, 255, 128])
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, blob: async () => new Blob([bytes]) }))
    const create = vi.spyOn(URL, 'createObjectURL').mockReturnValue('blob:owned')
    const revoke = vi.spyOn(URL, 'revokeObjectURL').mockImplementation(() => {})
    const click = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {})
    fireEvent.click(screen.getByTitle('Download'))
    await waitFor(() => expect(create).toHaveBeenCalledOnce())
    const artifact = create.mock.calls[0][0] as Blob
    expect(new Uint8Array(await artifact.arrayBuffer())).toEqual(bytes)
    expect(click).toHaveBeenCalledOnce(); expect(revoke).toHaveBeenCalledWith('blob:owned')
    expect(document.querySelector('a[download]')).toBeNull()
  })

  it('a malformed upload receipt stays unavailable and does not refresh over selection', async () => {
    query.mockImplementation(sql => base(sql))
    UploadXHR.instances = []; vi.stubGlobal('XMLHttpRequest', UploadXHR)
    await loaded()
    const file = new File([new Uint8Array([0, 255, 128])], 'binary.bin')
    fireEvent.change(document.querySelector('input[type=file]')!, { target: { files: [file] } })
    await waitFor(() => expect(UploadXHR.instances[0]?.send).toHaveBeenCalledOnce())
    fireEvent.click(screen.getByText('same-key'))
    const before = query.mock.calls.length
    UploadXHR.instances[0].responseText = '{}'
    await finish(() => UploadXHR.instances[0].onload?.())
    expect((await screen.findByRole('alert')).textContent).toContain('Upload receipt unavailable')
    expect(screen.getByText('Blob details')).toBeTruthy()
    expect(query.mock.calls).toHaveLength(before)
    expect(toasts.value.some(t => t.kind === 'success')).toBe(false)
  })

  it('late download does not create an artifact after connection ABA', async () => {
    query.mockImplementation(sql => base(sql)); await loaded()
    const body = deferred<Blob>()
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, blob: () => body.promise }))
    const create = vi.spyOn(URL, 'createObjectURL')
    fireEvent.click(screen.getByTitle('Download'))
    await waitFor(() => expect(fetch).toHaveBeenCalled())
    connect('c2'); connect('c1')
    await finish(() => body.resolve(new Blob([new Uint8Array([1, 2, 3])])))
    expect(create).not.toHaveBeenCalled()
    expect(toasts.value.some(t => t.kind === 'success')).toBe(false)
  })
})
