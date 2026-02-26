export default function handler(req, res) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    res.status(405).json({ ok: false, error: "Method Not Allowed" });
    return;
  }

  const authorized = req.headers.authorization === "Bearer valid-token";
  if (!authorized) {
    res.status(401).json({ ok: false, error: "Unauthorized" });
    return;
  }

  res.status(200).json({
    ok: true,
    refreshed: true,
    token: "valid-token",
  });
}
