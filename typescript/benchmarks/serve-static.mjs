import { createReadStream, existsSync } from "node:fs";
import { access, readdir, readFile, stat } from "node:fs/promises";
import { createServer } from "node:http";
import path from "node:path";
import process from "node:process";

const args = parseArgs(process.argv.slice(2));
const rootDir = path.resolve(process.cwd(), args.dir || ".");
const host = args.host || "127.0.0.1";
const port = Number(args.port || "4173");
const memoryMaxKb = Number(args["memory-max-kb"] || "1024");
const memoryMaxBytes = Number.isFinite(memoryMaxKb) && memoryMaxKb > 0
  ? Math.floor(memoryMaxKb * 1024)
  : 0;

await access(rootDir);
const headerRules = await loadHeaderRules(rootDir);
const routeMap = await buildRouteMap(rootDir);

const server = createServer(async (req, res) => {
  const url = new URL(req.url || "/", `http://${host}:${port}`);
  const pathname = decodeURIComponent(url.pathname);
  if (!isSafePath(pathname)) {
    sendText(res, 400, "Bad Request");
    return;
  }

  const route = resolveRoute(routeMap, pathname);
  if (!route) {
    sendText(res, 404, "Not Found");
    return;
  }

  const encodings = parseAcceptEncoding(req.headers["accept-encoding"]);
  const encoded = pickEncodedVariant(route, encodings);
  const selectedVariant = encoded || route.sourceVariant;
  const selectedPath = selectedVariant.filePath;
  const selectedSize = selectedVariant.size;

  res.statusCode = 200;
  res.setHeader("Content-Type", route.contentType);
  applyHeaderRules(res, headerRules, pathname);
  if (!res.hasHeader("Cache-Control")) {
    res.setHeader("Cache-Control", cacheControl(pathname, route.routePath, route.contentType));
  }
  res.setHeader("Vary", "Accept-Encoding");
  if (selectedVariant.encoding) {
    res.setHeader("Content-Encoding", selectedVariant.encoding);
  }
  res.setHeader("Content-Length", String(selectedSize));
  if (selectedVariant.buffer) {
    res.end(selectedVariant.buffer);
  } else {
    createReadStream(selectedPath).pipe(res);
  }
});

server.listen(port, host, () => {
  process.stdout.write(`[serve-static] ${rootDir} -> http://${host}:${port}\n`);
});

function parseArgs(argv) {
  const output = {};
  for (let i = 0; i < argv.length; i += 1) {
    const value = argv[i];
    if (!value.startsWith("--")) continue;
    const key = value.slice(2);
    const next = argv[i + 1];
    if (next && !next.startsWith("--")) {
      output[key] = next;
      i += 1;
    } else {
      output[key] = "1";
    }
  }
  return output;
}

async function buildRouteMap(root) {
  const files = await collectFiles(root);
  const map = new Map();

  for (const absolutePath of files) {
    if (absolutePath.endsWith(".br") || absolutePath.endsWith(".gz")) {
      continue;
    }

    const relativePath = path.relative(root, absolutePath).split(path.sep).join("/");
    const routePaths = toRoutePaths(relativePath);
    if (routePaths.length === 0) {
      continue;
    }

    const sourceVariant = await loadVariant(absolutePath, null, memoryMaxBytes);
    if (!sourceVariant) {
      continue;
    }

    const entry = {
      filePath: absolutePath,
      size: sourceVariant.size,
      routePath: routePaths[0],
      contentType: contentType(absolutePath),
      encodings: {
        br: await loadVariant(`${absolutePath}.br`, "br", memoryMaxBytes),
        gzip: await loadVariant(`${absolutePath}.gz`, "gzip", memoryMaxBytes),
      },
      sourceVariant,
    };

    for (const routePath of routePaths) {
      map.set(routePath, entry);
    }
  }

  return map;
}

async function collectFiles(root) {
  const output = [];
  const entries = await readdir(root, { withFileTypes: true });

  for (const entry of entries) {
    const absolutePath = path.join(root, entry.name);
    if (entry.isDirectory()) {
      output.push(...(await collectFiles(absolutePath)));
      continue;
    }
    if (entry.isFile()) {
      output.push(absolutePath);
    }
  }

  return output;
}

function toRoutePaths(relativePath) {
  if (relativePath === "index.html") {
    return ["/"];
  }

  if (relativePath.endsWith("/index.html")) {
    const basePath = `/${relativePath.slice(0, -"/index.html".length)}`;
    return [basePath, `${basePath}/`];
  }

  if (relativePath.endsWith(".html")) {
    return [`/${relativePath}`];
  }

  return [`/${relativePath}`];
}

function isSafePath(pathname) {
  return pathname.startsWith("/") && !pathname.includes("..");
}

function resolveRoute(routeMap, pathname) {
  const direct = routeMap.get(pathname);
  if (direct) {
    return direct;
  }

  if (pathname !== "/" && pathname.endsWith("/")) {
    return routeMap.get(pathname.slice(0, -1));
  }

  return routeMap.get(`${pathname}/`);
}

function parseAcceptEncoding(header) {
  if (typeof header !== "string" || !header.trim()) {
    return [];
  }

  return header
    .split(",")
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean)
    .map((value) => value.split(";")[0].trim());
}

function pickEncodedVariant(route, acceptedEncodings) {
  if (acceptedEncodings.includes("br") && route.encodings.br) {
    return route.encodings.br;
  }
  if (acceptedEncodings.includes("gzip") && route.encodings.gzip) {
    return route.encodings.gzip;
  }
  return null;
}

async function loadVariant(filePath, encoding, maxInlineBytes) {
  if (!fileExists(filePath)) {
    return null;
  }

  const info = await stat(filePath).catch(() => null);
  if (!info || !info.isFile()) {
    return null;
  }

  let buffer = null;
  if (maxInlineBytes > 0 && info.size <= maxInlineBytes) {
    buffer = await readFile(filePath);
  }

  return { filePath, encoding, size: info.size, buffer };
}

function fileExists(filePath) {
  return existsSync(filePath);
}

function sendText(res, status, body) {
  res.statusCode = status;
  res.setHeader("Content-Type", "text/plain; charset=utf-8");
  res.end(body);
}

function cacheControl(pathname, routePath, contentTypeValue) {
  const ext = path.extname(routePath).toLowerCase();
  if (pathname.startsWith("/_next/") || pathname.startsWith("/assets/")) {
    return "public, max-age=31536000, immutable";
  }

  if (ext === ".html" || String(contentTypeValue || "").startsWith("text/html")) {
    return "public, max-age=0, must-revalidate";
  }

  return "public, max-age=3600";
}

function contentType(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  switch (ext) {
    case ".html":
      return "text/html; charset=utf-8";
    case ".js":
      return "application/javascript; charset=utf-8";
    case ".css":
      return "text/css; charset=utf-8";
    case ".json":
      return "application/json; charset=utf-8";
    case ".svg":
      return "image/svg+xml";
    case ".png":
      return "image/png";
    case ".jpg":
    case ".jpeg":
      return "image/jpeg";
    case ".webp":
      return "image/webp";
    case ".ico":
      return "image/x-icon";
    case ".txt":
      return "text/plain; charset=utf-8";
    default:
      return "application/octet-stream";
  }
}

async function loadHeaderRules(root) {
  const headersPath = path.join(root, "_headers");
  if (!fileExists(headersPath)) {
    return [];
  }

  const raw = await readFile(headersPath, "utf-8");
  return parseHeaderRules(raw);
}

function parseHeaderRules(raw) {
  const rules = [];
  let current = null;

  for (const line of raw.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed) {
      current = null;
      continue;
    }

    if (!line.startsWith(" ") && !line.startsWith("\t")) {
      current = {
        source: trimmed,
        regex: toSourceRegex(trimmed),
        headers: [],
      };
      rules.push(current);
      continue;
    }

    if (!current) {
      continue;
    }

    const separator = trimmed.indexOf(":");
    if (separator <= 0) {
      continue;
    }
    const name = trimmed.slice(0, separator).trim();
    const value = trimmed.slice(separator + 1).trim();
    if (!name || !value) {
      continue;
    }
    current.headers.push({ name, value });
  }

  return rules;
}

function toSourceRegex(source) {
  const escaped = source
    .replace(/[.+?^${}()|[\]\\]/g, "\\$&")
    .replace(/\*/g, ".*");
  return new RegExp(`^${escaped}$`);
}

function applyHeaderRules(res, rules, pathname) {
  for (const rule of rules) {
    if (!rule.regex.test(pathname)) {
      continue;
    }
    for (const header of rule.headers) {
      res.setHeader(header.name, header.value);
    }
  }
}
