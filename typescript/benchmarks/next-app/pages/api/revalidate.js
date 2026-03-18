import { revalidateBenchCache } from "../../lib/bench-cache";

export default function handler(req, res) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    res.status(405).json({ ok: false, error: "Method Not Allowed" });
    return;
  }

  const version = revalidateBenchCache();
  res.status(200).json({ ok: true, version });
}
