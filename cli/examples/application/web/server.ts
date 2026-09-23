import { createServer } from "node:http";

type Message = { message: string };
const origin = process.env.API_ORIGIN ?? "http://127.0.0.1:4301";
const server = createServer(async (request, response) => {
  if (request.url === "/health") {
    response.writeHead(200).end("ok");
    return;
  }
  try {
    const upstream = await fetch(`${origin}/message`, { signal: AbortSignal.timeout(2000) });
    if (!upstream.ok) throw new Error(`API returned ${upstream.status}`);
    const data = await upstream.json() as Message;
    if (typeof data.message !== "string") throw new Error("Unexpected API response");
    response.writeHead(200, { "Content-Type": "application/json" });
    response.end(JSON.stringify({ from: "TypeScript", api: data.message }));
  } catch {
    response.writeHead(502).end("API unavailable");
  }
});
server.listen(Number(process.env.PORT ?? 4300), "127.0.0.1");
for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => server.close(() => process.exit(0)));
}
