import { readBenchCache } from "../../lib/bench-cache";

export default function handler(req, res) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    res.status(405).json({ ok: false, error: "Method Not Allowed" });
    return;
  }

  const result = readBenchCache();
  res.setHeader("x-bench-cache", result.hit ? "HIT" : "MISS");
  res.status(200).json(result.payload);
}
