import * as fs from "node:fs";
import * as path from "node:path";
import { assertServerFeaturesServable, type AdapterBuildContext, type NeutronAdapter } from "./adapter.js";

export interface DockerAdapterOptions {
  nodeVersion?: string;
  port?: number;
  imageName?: string;
}

export function adapterDocker(options: DockerAdapterOptions = {}): NeutronAdapter {
  return {
    name: "docker",
    async adapt(context: AdapterBuildContext) {
      assertServerFeaturesServable("docker", context, context.routes.app > 0);
      // Public membership comes from producers, never directory discovery or
      // a reserved-name denylist. Third-party callers must supply this contract.
      if (!context.publicArtifacts) throw new Error("Docker adapter requires producer-issued publicArtifacts");
      const publicRoot = path.join(context.outDir, ".neutron-public");
      fs.mkdirSync(publicRoot, { recursive: true });
      const publicFiles: string[] = [];
      const realOutDir = fs.realpathSync(context.outDir);
      for (const artifact of new Set(context.publicArtifacts)) {
        if (!artifact || artifact.includes("\\") || artifact.includes("\0") || path.isAbsolute(artifact) || artifact.split("/").some(part => !part || part === "." || part === ".." || part.startsWith(".")) || ["server", "server.mjs", "Dockerfile", "package.json"].includes(artifact.split("/")[0])) throw new Error(`Invalid public artifact: ${artifact}`);
        const source = path.join(context.outDir, artifact);
        // Reject symlinks at every component; realpath also fences containment.
        let component = context.outDir;
        for (const part of artifact.split("/")) {
          component = path.join(component, part);
          if (fs.lstatSync(component).isSymbolicLink()) throw new Error(`Public artifact is a symlink: ${artifact}`);
        }
        const realSource = fs.realpathSync(source);
        if (!realSource.startsWith(realOutDir + path.sep) || !fs.statSync(realSource).isFile()) throw new Error(`Invalid public artifact: ${artifact}`);
        const destination = path.join(publicRoot, artifact);
        fs.mkdirSync(path.dirname(destination), { recursive: true });
        fs.copyFileSync(realSource, destination);
        // Its precompressed siblings travel with it (copied after it, so not
        // older), but stay off the addressable file list.
        for (const suffix of [".br", ".gz"]) {
          const sibling = realSource + suffix;
          if (fs.existsSync(sibling) && !fs.lstatSync(sibling).isSymbolicLink() && fs.statSync(sibling).isFile()) {
            fs.copyFileSync(sibling, destination + suffix);
          }
        }
        publicFiles.push(artifact);
      }
      const hasAppRoutes = context.routes.app > 0;
      let runtimeEntryImport: string | null = null;

      if (hasAppRoutes) {
        if (!context.ensureRuntimeBundle) {
          throw new Error("Docker adapter requires runtime bundle support from neutron build.");
        }

        // The image context is dist/ alone: the bundle ships its runtime
        // dependencies into dist/node_modules (real directories).
        const runtimeBundle = await context.ensureRuntimeBundle("node", { selfContained: true });
        runtimeEntryImport = `./${runtimeBundle.entryRelativePath}`.replace(/\\/g, "/");
        context.log(`Docker runtime bundle: ${runtimeBundle.entryRelativePath}`);
      }

      const serverPath = path.join(context.outDir, "server.mjs");
      fs.writeFileSync(serverPath, buildDockerServerSource(runtimeEntryImport, publicFiles), "utf-8");
      context.log(`Docker server entry written: ${path.relative(context.rootDir, serverPath)}`);

      const dockerfilePath = path.join(context.outDir, "Dockerfile");
      fs.writeFileSync(
        dockerfilePath,
        buildDockerfile({
          nodeVersion: options.nodeVersion || "20",
          port: options.port || 3000,
        }),
        "utf-8"
      );
      context.log(`Dockerfile written: ${path.relative(context.rootDir, dockerfilePath)}`);

      const dockerIgnorePath = path.join(context.outDir, ".dockerignore");
      fs.writeFileSync(
        dockerIgnorePath,
        [
          // No node_modules here: dist/node_modules is the image's
          // dependency set, shipped by the build.
          ".git",
          // `neutron-ts start`'s server bundle: unused by the image (it runs
          // server.mjs), and its node_modules links point into the build
          // machine's package store.
          "server/start",
          ".neutron-server-bundle.json",
          ".DS_Store",
          "npm-debug.log*",
          "pnpm-debug.log*",
        ].join("\n") + "\n",
        "utf-8"
      );
      context.log(`.dockerignore written: ${path.relative(context.rootDir, dockerIgnorePath)}`);

      const metadata = {
        adapter: "docker",
        imageName: options.imageName || path.basename(context.rootDir).toLowerCase(),
        routes: context.routes,
        generatedAt: new Date().toISOString(),
      };
      const metadataPath = path.join(context.outDir, ".neutron-adapter-docker.json");
      fs.writeFileSync(metadataPath, JSON.stringify(metadata, null, 2), "utf-8");
      context.log(`Docker adapter metadata written: ${path.relative(context.rootDir, metadataPath)}`);
    },
  };
}

function buildDockerfile(options: { nodeVersion: string; port: number }): string {
  return `FROM node:${options.nodeVersion}-alpine

WORKDIR /app
COPY . .

ENV NODE_ENV=production
ENV PORT=${options.port}

EXPOSE ${options.port}
# SIGTERM is Docker's default stop signal; the server drains in-flight requests on it.
STOPSIGNAL SIGTERM
CMD ["node", "server.mjs"]
`;
}

function buildDockerServerSource(runtimeEntryImport: string | null, publicFiles: string[]): string {
  const runtimeImport = runtimeEntryImport
    ? `import * as __runtime from "${runtimeEntryImport}";
const { handleNeutronRequest } = __runtime;
// Generated runtimes export both; a runtime bundle from a custom
// ensureRuntimeBundle may not, and then files are served as before.
const handlesPath = __runtime.handlesPath ?? (() => false);
const staticHeaders = __runtime.staticHeaders ?? (() => ({ "X-Content-Type-Options": "nosniff" }));`
    : `const handlesPath = () => false;
const staticHeaders = () => ({ "X-Content-Type-Options": "nosniff" });`;
  const runtimeFallback = runtimeEntryImport
    ? `  const response = await handleNeutronRequest(webRequest, { remoteAddress: req.socket?.remoteAddress });
  await writeWebResponse(res, response);`
    : `  res.statusCode = 404;
  res.end("Not Found");`;

  return `import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { createServer } from "node:http";
import { createReadStream, existsSync, statSync, realpathSync } from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
${runtimeImport}

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DIST_DIR = path.join(__dirname, ".neutron-public");
const PUBLIC_FILES = new Set(${JSON.stringify(publicFiles)});
const PORT = Number(process.env.PORT || 3000);

const MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".mjs": "application/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".webp": "image/webp",
  ".ico": "image/x-icon",
  ".txt": "text/plain; charset=utf-8",
};

// The server's default (ServerOptions.maxRequestBodyBytes). 0 turns it off.
const MAX_BODY_BYTES = process.env.NEUTRON_MAX_BODY_BYTES === undefined ? 10 * 1024 * 1024 : Number(process.env.NEUTRON_MAX_BODY_BYTES);

const server = createServer(async (req, res) => {
  try {
    const url = new URL(req.url || "/", \`http://\${req.headers.host || "localhost"}\`);
    // The server's own rule (core/route-path.ts): one spelling of every path,
    // so middleware matching on the URL and the router cannot disagree.
    const canonical = url.pathname
      .replace(/\\/{2,}/g, "/")
      .replace(/%([0-9a-fA-F]{2})/g, (escape, hex) => {
        const char = String.fromCharCode(parseInt(hex, 16));
        return /[A-Za-z0-9\\-._~]/.test(char) ? char : escape.toUpperCase();
      });
    if (canonical !== url.pathname) {
      res.statusCode = 308;
      res.setHeader("Location", canonical + url.search);
      res.end();
      return;
    }
    if (MAX_BODY_BYTES > 0 && Number(req.headers["content-length"]) > MAX_BODY_BYTES) {
      res.statusCode = 413;
      res.end("Request body too large");
      return;
    }
    const pathname = decodeURIComponent(url.pathname);

    // A prebuilt HTML file answers only where no app route claims the path
    // (start's rule): dist/index.html is the client build's shell, and an
    // app route at "/" must render, not be shadowed by it.
    const staticPath = resolveStaticPath(pathname);
    if (staticPath && !(staticPath.endsWith(".html") && handlesPath(pathname))) {
      return streamFile(req, res, staticPath, pathname);
    }

    const webRequest = toWebRequest(req);
${runtimeFallback}
  } catch (error) {
    res.statusCode = 500;
    res.setHeader("Content-Type", "text/plain; charset=utf-8");
    res.end("Internal Server Error");
  }
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(\`Neutron Docker server listening on http://0.0.0.0:\${server.address().port}\`);
});

// Graceful shutdown: stop accepting new connections and let in-flight requests
// finish (bounded), so a container rollout (docker/k8s sends SIGTERM) never
// drops requests. Mirrors the drain in \`neutron start\`.
const SHUTDOWN_TIMEOUT_MS = Number(process.env.NEUTRON_SHUTDOWN_TIMEOUT_MS || 30000);
let shuttingDown = false;
function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(\`Received \${signal}, draining in-flight requests…\`);
  const forceExit = setTimeout(() => {
    console.error("Drain timeout exceeded; forcing exit.");
    process.exit(1);
  }, SHUTDOWN_TIMEOUT_MS);
  forceExit.unref();
  server.close((err) => {
    clearTimeout(forceExit);
    if (err) {
      console.error("Error during shutdown:", err);
      process.exit(1);
    }
    process.exit(0);
  });
}
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

function resolveStaticPath(pathname) {
  if (pathname.includes(String.fromCharCode(0)) || pathname.includes(String.fromCharCode(92)) || pathname.split("/").some(segment => segment.startsWith("."))) {
    return null;
  }
  const cleaned = pathname === "/" ? "/index.html" : pathname;
  const resolved = path.resolve(DIST_DIR, cleaned.replace(/^\\/+/, ""));
  // Containment must use a path-separator boundary; a bare startsWith() prefix
  // check would also accept sibling dirs like "<DIST_DIR>-secret".
  if (resolved !== DIST_DIR && !resolved.startsWith(DIST_DIR + path.sep)) {
    return null;
  }

  if (PUBLIC_FILES.has(path.relative(DIST_DIR, resolved).split(path.sep).join("/")) && existsSync(resolved) && realpathSync(resolved).startsWith(realpathSync(DIST_DIR) + path.sep) && statSync(resolved).isFile()) {
    return resolved;
  }

  const nestedIndex = path.join(resolved, "index.html");
  if ((nestedIndex === DIST_DIR || nestedIndex.startsWith(DIST_DIR + path.sep)) && PUBLIC_FILES.has(path.relative(DIST_DIR, nestedIndex).split(path.sep).join("/")) && existsSync(nestedIndex) && realpathSync(nestedIndex).startsWith(realpathSync(DIST_DIR) + path.sep) && statSync(nestedIndex).isFile()) {
    return nestedIndex;
  }

  return null;
}

// The .br / .gz siblings neutron-ts build writes, when the client accepts
// one (br first; q=0 refuses) and it is not older than its source.
function precompressedSibling(acceptEncoding, filePath) {
  if (!acceptEncoding) return null;
  const accepted = new Map();
  for (const part of String(acceptEncoding).toLowerCase().split(",")) {
    const [name, ...params] = part.split(";").map((piece) => piece.trim());
    let q = 1;
    for (const param of params) {
      const [key, value] = param.split("=").map((piece) => piece.trim());
      if (key === "q") q = Number(value) || 0;
    }
    if (name) accepted.set(name, q);
  }
  for (const [coding, suffix] of [["br", ".br"], ["gzip", ".gz"]]) {
    const q = accepted.has(coding) ? accepted.get(coding) : accepted.get("*");
    if (!q) continue;
    try {
      const sibling = statSync(filePath + suffix);
      if (sibling.isFile() && sibling.mtimeMs >= statSync(filePath).mtimeMs) {
        return { coding, file: filePath + suffix, size: sibling.size };
      }
    } catch {}
  }
  return null;
}

function streamFile(req, res, filePath, pathname) {
  const ext = path.extname(filePath).toLowerCase();
  res.statusCode = 200;
  // routes.headers and the security defaults, as start sends on files.
  for (const [name, value] of Object.entries(staticHeaders(pathname))) {
    res.setHeader(name, value);
  }
  res.setHeader("Content-Type", MIME_TYPES[ext] || "application/octet-stream");
  // Prevent MIME sniffing of static assets (e.g. a .txt/.json that contains
  // HTML being interpreted as HTML).
  res.setHeader("X-Content-Type-Options", "nosniff");
  if (pathname.startsWith("/assets/") && !res.hasHeader("Cache-Control")) {
    res.setHeader("Cache-Control", "public, max-age=31536000, immutable");
  }
  let source = filePath;
  if (!req.headers.range) {
    const sibling = precompressedSibling(req.headers["accept-encoding"], filePath);
    if (sibling) {
      source = sibling.file;
      res.setHeader("Content-Encoding", sibling.coding);
      res.setHeader("Content-Length", String(sibling.size));
    }
    if (existsSync(filePath + ".br") || existsSync(filePath + ".gz")) {
      res.setHeader("Vary", "Accept-Encoding");
    }
  }
  const stream = createReadStream(source);
  stream.pipe(res);
  stream.on("error", () => {
    if (!res.headersSent) {
      res.statusCode = 500;
      res.setHeader("Content-Type", "text/plain; charset=utf-8");
    }
    res.end("File stream error");
  });
}

// A body with no Content-Length, or a false one: the read fails past the cap.
async function* capBody(stream) {
  let read = 0;
  for await (const chunk of stream) {
    read += chunk.length;
    if (MAX_BODY_BYTES > 0 && read > MAX_BODY_BYTES) throw new Error("Request body too large");
    yield chunk;
  }
}

function toWebRequest(req) {
  const url = new URL(req.url || "/", \`http://\${req.headers.host || "localhost"}\`);
  const headers = new Headers();
  for (const [key, value] of Object.entries(req.headers)) {
    if (!value) continue;
    if (Array.isArray(value)) {
      for (const item of value) {
        headers.append(key, item);
      }
    } else {
      headers.set(key, String(value));
    }
  }

  const method = (req.method || "GET").toUpperCase();
  const hasBody = method !== "GET" && method !== "HEAD";
  return new Request(url.toString(), {
    method,
    headers,
    body: hasBody ? capBody(req) : undefined,
    duplex: hasBody ? "half" : undefined,
  });
}

async function writeWebResponse(res, response) {
  res.statusCode = response.status;
  const setCookies = typeof response.headers.getSetCookie === "function"
    ? response.headers.getSetCookie()
    : null;
  response.headers.forEach((value, key) => {
    if (key.toLowerCase() === "set-cookie") return;
    res.setHeader(key, value);
  });
  if (setCookies && setCookies.length > 0) {
    res.setHeader("set-cookie", setCookies);
  }

  if (!response.body) {
    res.end();
    return;
  }

  await pipeline(Readable.fromWeb(response.body), res);
}
`;
}
