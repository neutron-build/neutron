// Client-weight metric: for each route, measure the INITIAL JS bytes a browser
// must download before interactive — the entry module scripts plus everything
// they STATICALLY import (transitively). Dynamic import() chunks are excluded
// (that is the payoff of code-splitting). This is the axis the RPS harness never
// measured, and the one where Astro competes.
//
// Usage: node client-weight.mjs <baseUrl> <path1,path2,...>
//   node client-weight.mjs http://127.0.0.1:3000 /,/about,/dashboard,/todos

const [, , baseUrl = "http://127.0.0.1:3000", pathsArg = "/"] = process.argv;
const paths = pathsArg.split(",").map((p) => p.trim()).filter(Boolean);

const STATIC_FROM = /(?:^|[^A-Za-z0-9_$])from\s*["']([^"']+)["']/g;
const STATIC_SIDE_EFFECT = /(?:^|[^A-Za-z0-9_$.])import\s*["']([^"']+)["']/g; // import"x" (not import("x"))

function resolveUrl(spec, fromUrl) {
  try { return new URL(spec, fromUrl).href; } catch { return null; }
}

async function fetchText(url) {
  const res = await fetch(url);
  if (!res.ok) return { ok: false, bytes: 0, text: "" };
  const buf = await res.arrayBuffer();
  return { ok: true, bytes: buf.byteLength, text: Buffer.from(buf).toString("utf8") };
}

function extractStaticImports(code) {
  const specs = new Set();
  for (const re of [STATIC_FROM, STATIC_SIDE_EFFECT]) {
    re.lastIndex = 0;
    let m;
    while ((m = re.exec(code)) !== null) specs.add(m[1]);
  }
  return [...specs];
}

async function measureRoute(path) {
  const pageUrl = new URL(path, baseUrl).href;
  const page = await fetchText(pageUrl);
  const htmlBytes = page.bytes;
  // entry module scripts in the HTML
  const scriptRe = /<script[^>]*type=["']module["'][^>]*src=["']([^"']+)["'][^>]*>/g;
  const entries = [];
  let m;
  while ((m = scriptRe.exec(page.text)) !== null) {
    const u = resolveUrl(m[1], pageUrl);
    if (u) entries.push(u);
  }
  // transitively walk STATIC imports only
  const seen = new Map(); // url -> bytes
  const queue = [...entries];
  while (queue.length) {
    const url = queue.shift();
    if (seen.has(url)) continue;
    const mod = await fetchText(url);
    seen.set(url, mod.bytes);
    if (!mod.ok) continue;
    for (const spec of extractStaticImports(mod.text)) {
      const dep = resolveUrl(spec, url);
      if (dep && !seen.has(dep) && dep.startsWith(new URL(baseUrl).origin)) queue.push(dep);
    }
  }
  const jsBytes = [...seen.values()].reduce((a, b) => a + b, 0);
  return { path, htmlBytes, jsModules: seen.size, initialJsBytes: jsBytes };
}

const rows = [];
for (const p of paths) {
  try { rows.push(await measureRoute(p)); }
  catch (e) { rows.push({ path: p, error: String(e?.message || e) }); }
}

console.log("\nclient weight (initial download before interactive):\n");
console.log("route".padEnd(22) + "html".padStart(9) + "js modules".padStart(12) + "initial JS".padStart(13));
for (const r of rows) {
  if (r.error) { console.log(r.path.padEnd(22) + "  ERROR: " + r.error); continue; }
  console.log(
    r.path.padEnd(22) +
    (r.htmlBytes + "B").padStart(9) +
    String(r.jsModules).padStart(12) +
    ((r.initialJsBytes / 1024).toFixed(1) + "KB").padStart(13)
  );
}
console.log("");
