export const config = { mode: "app" };

const CACHE_TTL_MS = 30_000;

type CacheState = {
  version: number;
  expiresAt: number;
  payload: { ok: boolean; version: number; value: number } | null;
};

function getState(): CacheState {
  const globalState = globalThis as typeof globalThis & {
    __benchCacheState?: CacheState;
  };
  if (!globalState.__benchCacheState) {
    globalState.__benchCacheState = {
      version: 1,
      expiresAt: 0,
      payload: null,
    };
  }
  return globalState.__benchCacheState;
}

function runWork(seed: number, repeat = 20_000): number {
  let acc = seed >>> 0;
  for (let i = 0; i < repeat; i += 1) {
    acc = (acc * 1664525 + 1013904223) >>> 0;
  }
  return acc;
}

export async function loader() {
  const state = getState();
  const now = Date.now();
  const hit = !!state.payload && state.expiresAt > now;
  if (!hit) {
    state.payload = {
      ok: true,
      version: state.version,
      value: runWork(state.version + now),
    };
    state.expiresAt = now + CACHE_TTL_MS;
  }

  throw new Response(JSON.stringify(state.payload), {
    status: 200,
    headers: {
      "Content-Type": "application/json",
      "x-bench-cache": hit ? "HIT" : "MISS",
    },
  });
}

export async function action() {
  return new Response(JSON.stringify({ ok: false, error: "Method Not Allowed" }), {
    status: 405,
    headers: {
      "Content-Type": "application/json",
      Allow: "GET",
    },
  });
}

export default function ApiCacheRoute() {
  return <main>GET /api/cache</main>;
}
