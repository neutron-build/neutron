function runMutation(seed = 13, repeat = 6000) {
  let acc = seed >>> 0;
  for (let i = 0; i < repeat; i += 1) {
    acc = (acc * 1664525 + 1013904223) >>> 0;
  }
  return acc;
}

export default function handler(req, res) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    res.status(405).json({ ok: false, error: "Method Not Allowed" });
    return;
  }

  const seed = Number(req.body?.seed ?? 13);
  const repeat = Number(req.body?.repeat ?? 6000);
  const safeSeed = Number.isFinite(seed) ? seed : 13;
  const safeRepeat = Number.isFinite(repeat) ? Math.max(1, Math.min(50000, repeat)) : 6000;
  const value = runMutation(safeSeed, safeRepeat);

  res.status(200).json({
    ok: true,
    seed: safeSeed,
    repeat: safeRepeat,
    value,
  });
}
