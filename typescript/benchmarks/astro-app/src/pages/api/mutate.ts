import type { APIRoute } from "astro";

function runMutation(seed = 13, repeat = 6000) {
  let acc = seed >>> 0;
  for (let i = 0; i < repeat; i += 1) {
    acc = (acc * 1664525 + 1013904223) >>> 0;
  }
  return acc;
}

export const GET: APIRoute = async () => {
  return new Response(
    JSON.stringify({ ok: false, error: "Method Not Allowed" }),
    {
      status: 405,
      headers: {
        "Content-Type": "application/json",
        Allow: "POST",
      },
    }
  );
};

export const POST: APIRoute = async ({ request }) => {
  const payload = await request.json().catch(() => ({} as Record<string, unknown>));
  const seed = Number(payload?.seed ?? 13);
  const repeat = Number(payload?.repeat ?? 6000);
  const safeSeed = Number.isFinite(seed) ? seed : 13;
  const safeRepeat = Number.isFinite(repeat) ? Math.max(1, Math.min(50000, repeat)) : 6000;

  return new Response(
    JSON.stringify({
      ok: true,
      seed: safeSeed,
      repeat: safeRepeat,
      value: runMutation(safeSeed, safeRepeat),
    }),
    {
      status: 200,
      headers: { "Content-Type": "application/json" },
    }
  );
};
