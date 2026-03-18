import { json } from "@remix-run/node";

function runMutation(seed = 13, repeat = 6000) {
  let acc = seed >>> 0;
  for (let i = 0; i < repeat; i += 1) {
    acc = (acc * 1664525 + 1013904223) >>> 0;
  }
  return acc;
}

export async function loader() {
  return json({ ok: false, error: "Method Not Allowed" }, { status: 405 });
}

export async function action({ request }) {
  const payload = await request.json().catch(() => ({}));
  const seed = Number(payload?.seed ?? 13);
  const repeat = Number(payload?.repeat ?? 6000);
  const safeSeed = Number.isFinite(seed) ? seed : 13;
  const safeRepeat = Number.isFinite(repeat) ? Math.max(1, Math.min(50000, repeat)) : 6000;

  return json({
    ok: true,
    seed: safeSeed,
    repeat: safeRepeat,
    value: runMutation(safeSeed, safeRepeat),
  });
}
