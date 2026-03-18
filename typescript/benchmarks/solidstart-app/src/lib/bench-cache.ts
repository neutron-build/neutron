const CACHE_TTL_MS = 30_000;

interface CacheState {
  version: number;
  expiresAt: number;
  payload: any;
}

const state: CacheState = { version: 1, expiresAt: 0, payload: null };

function runWork(seed = 42, iterations = 20000) {
  let v = seed;
  for (let i = 0; i < iterations; i++) v = (v * 1664525 + 1013904223) & 0xffffffff;
  return v >>> 0;
}

export function readBenchCache() {
  const now = Date.now();
  if (state.payload && now < state.expiresAt) return { hit: true, payload: state.payload };
  const value = runWork();
  state.payload = { ok: true, version: state.version, value };
  state.expiresAt = now + CACHE_TTL_MS;
  return { hit: false, payload: state.payload };
}

export function revalidateBenchCache() {
  state.version++;
  state.payload = null;
  state.expiresAt = 0;
  return state.version;
}
