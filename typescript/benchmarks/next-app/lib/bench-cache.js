const CACHE_TTL_MS = 30_000;

function getState() {
  const globalState = globalThis;
  if (!globalState.__benchCacheState) {
    globalState.__benchCacheState = {
      version: 1,
      expiresAt: 0,
      payload: null,
    };
  }
  return globalState.__benchCacheState;
}

function runWork(seed, repeat = 20_000) {
  let acc = seed >>> 0;
  for (let i = 0; i < repeat; i += 1) {
    acc = (acc * 1664525 + 1013904223) >>> 0;
  }
  return acc;
}

export function readBenchCache() {
  const state = getState();
  const now = Date.now();
  if (state.payload && state.expiresAt > now) {
    return { hit: true, payload: state.payload };
  }

  const payload = {
    ok: true,
    version: state.version,
    value: runWork(state.version + now),
  };
  state.payload = payload;
  state.expiresAt = now + CACHE_TTL_MS;
  return { hit: false, payload };
}

export function revalidateBenchCache() {
  const state = getState();
  state.version += 1;
  state.expiresAt = 0;
  state.payload = null;
  return state.version;
}
