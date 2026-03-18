export default function handler(req, res) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    res.status(405).json({ ok: false, error: "Method Not Allowed" });
    return;
  }

  res.statusCode = 200;
  res.setHeader("Content-Type", "text/plain; charset=utf-8");
  res.setHeader("Cache-Control", "no-store");
  res.setHeader("Transfer-Encoding", "chunked");
  res.write("stream-start\n");
  res.write("chunk-1\n");
  setTimeout(() => {
    res.write("chunk-2\n");
    setTimeout(() => {
      res.end("chunk-3\nstream-end\n");
    }, 5);
  }, 5);
}
