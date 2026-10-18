// E6 OPFS spike worker (card X-6; PLAN-v3 §4 E6, §3.2 OpfsEnv).
//
// Owns the OPFS sync access handles for one database and bridges the
// page's messages to the wasm exports. The async part of OPFS —
// `navigator.storage.getDirectory()` and `createSyncAccessHandle()` — is
// unavoidably promise-based, and the crate is deliberately sync-only
// (no wasm-bindgen-futures dependency), so it happens HERE: this worker
// acquires the handles and hands them to the wasm `open`. All I/O after
// that is synchronous, on this thread, exactly as the future OpfsEnv
// would run inside the engine.
//
// Message protocol: { id, t, ...args } in, { id, ok, result } or
// { id, ok: false, error: string } out. Errors are always strings so the
// page side can forward them through Playwright.

import init, {
  open,
  write_page,
  flush,
  read_page,
  append_wal,
  wal_len,
  page_pattern,
  check_page,
} from './pkg/e6_opfs.js';

const wasm_ready = init();
const encoder = new TextEncoder();

// The sync handles for the currently open database. Kept on the JS side so
// `close` can release them explicitly; the wasm module keeps its own
// references until the next `open` replaces them.
let handles = null;

self.onmessage = (ev) => {
  const { id, t, ...args } = ev.data;
  dispatch(t, args).then(
    (result) => self.postMessage({ id, ok: true, result }),
    (err) =>
      self.postMessage({
        id,
        ok: false,
        error: String((err && err.message) || err),
      }),
  );
};

async function dispatch(t, args) {
  switch (t) {
    case 'open':
      return open_db(args.name);
    case 'close':
      return close_db();
    case 'flush':
      flush();
      return null;
    case 'append_wal':
      append_wal(encoder.encode(String(args.text)));
      return null;
    case 'wal_len':
      return wal_len();
    case 'write_pages':
      return write_pages(args.from, args.to);
    case 'read_check':
      return read_check(args.from, args.to);
    case 'bench_write':
      return bench_write(args.iters);
    case 'bench_flush':
      return bench_flush(args.iters);
    default:
      throw new Error('worker: unknown command ' + t);
  }
}

async function open_db(name) {
  if (handles) {
    throw new Error('worker: a database is already open (close first)');
  }
  await wasm_ready;
  const root = await navigator.storage.getDirectory();
  const spike = await root.getDirectoryHandle('e6-spike', { create: true });
  const db_dir = await spike.getDirectoryHandle(name, { create: true });
  const pages_handle = await db_dir.getFileHandle('pages', { create: true });
  const wal_handle = await db_dir.getFileHandle('wal', { create: true });
  // createSyncAccessHandle exists only in workers; one handle per file.
  const pages = await pages_handle.createSyncAccessHandle();
  const wal = await wal_handle.createSyncAccessHandle();
  open(name, pages, wal); // wasm owns the I/O from here on
  handles = { pages, wal };
  return { name, wal_len: wal_len() };
}

function close_db() {
  if (!handles) return null;
  try {
    handles.pages.close();
    handles.wal.close();
  } finally {
    handles = null;
  }
  return null;
}

function write_pages(from, to) {
  const t0 = performance.now();
  for (let n = from; n < to; n++) {
    // One pattern page generated in wasm, written through the wasm export:
    // the write path and the verification oracle are the same code.
    write_page(n, page_pattern(n));
  }
  return { pages: to - from, ms: performance.now() - t0 };
}

function read_check(from, to) {
  const missing = []; // nothing at that offset (past EOF)
  const corrupt = []; // bytes present but not the expected pattern
  for (let n = from; n < to; n++) {
    const bytes = read_page(n);
    if (bytes.length === 0) {
      missing.push(n);
      continue;
    }
    const bad = check_page(n, bytes);
    if (bad >= 0) corrupt.push({ page: n, first_bad: bad, len: bytes.length });
  }
  return { checked: to - from, missing, corrupt };
}

function bench_write(iters) {
  // Pre-generate the pattern pages so the timed loop contains only the
  // write_page calls (8 KiB per write, over 64 rotating pages).
  const patterns = [];
  for (let i = 0; i < 64; i++) patterns.push(page_pattern(i));
  const times = [];
  for (let i = 0; i < iters; i++) {
    const page = i % 64;
    const t0 = performance.now();
    write_page(page, patterns[page]);
    times.push(performance.now() - t0);
  }
  return times;
}

function bench_flush(iters) {
  // One write then one flush per iteration; only the flush() call is
  // timed. This is the commit-shaped barrier the spike cares about.
  const pattern = page_pattern(0);
  const times = [];
  for (let i = 0; i < iters; i++) {
    write_page(0, pattern);
    const t0 = performance.now();
    flush();
    times.push(performance.now() - t0);
  }
  return times;
}
