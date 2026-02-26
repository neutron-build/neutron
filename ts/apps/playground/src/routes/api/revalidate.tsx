export const config = { mode: "app" };

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

export async function loader() {
  throw new Response(JSON.stringify({ ok: false, error: "Method Not Allowed" }), {
    status: 405,
    headers: {
      "Content-Type": "application/json",
      Allow: "POST",
    },
  });
}

export async function action() {
  const state = getState();
  state.version += 1;
  state.expiresAt = 0;
  state.payload = null;
  return new Response(JSON.stringify({ ok: true, version: state.version }), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
}

export default function ApiRevalidateRoute() {
  return <main>POST /api/revalidate</main>;
}
