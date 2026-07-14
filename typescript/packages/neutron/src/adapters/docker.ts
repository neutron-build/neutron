import * as fs from "node:fs";
import * as path from "node:path";
import type { AdapterBuildContext, NeutronAdapter } from "./adapter.js";

export interface DockerAdapterOptions {
  nodeVersion?: string;
  port?: number;
  imageName?: string;
}

export function adapterDocker(options: DockerAdapterOptions = {}): NeutronAdapter {
  return {
    name: "docker",
    async adapt(context: AdapterBuildContext) {
      const hasAppRoutes = context.routes.app > 0;
      let runtimeEntryImport: string | null = null;

      if (hasAppRoutes) {
        if (!context.ensureRuntimeBundle) {
          throw new Error("Docker adapter requires runtime bundle support from neutron build.");
        }

        const runtimeBundle = await context.ensureRuntimeBundle("node");
        runtimeEntryImport = `./${runtimeBundle.entryRelativePath}`.replace(/\\/g, "/");
        context.log(`Docker runtime bundle: ${runtimeBundle.entryRelativePath}`);
      }

      const serverPath = path.join(context.outDir, "server.mjs");
      fs.writeFileSync(serverPath, buildDockerServerSource(runtimeEntryImport), "utf-8");
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
          "node_modules",
          ".git",
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

function buildDockerServerSource(runtimeEntryImport: string | null): string {
  const runtimeImport = runtimeEntryImport
    ? `import { handleNeutronRequest } from "${runtimeEntryImport}";`
    : "";
  const runtimeFallback = runtimeEntryImport
    ? `  const response = await handleNeutronRequest(webRequest);
  await writeWebResponse(res, response);`
    : `  res.statusCode = 404;
  res.end("Not Found");`;

  return `import { createServer } from "node:http";
import { createReadStream, existsSync, statSync } from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
${runtimeImport}

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DIST_DIR = __dirname;
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

const server = createServer(async (req, res) => {
  try {
    const url = new URL(req.url || "/", \`http://\${req.headers.host || "localhost"}\`);
    const pathname = decodeURIComponent(url.pathname);

    const staticPath = resolveStaticPath(pathname);
    if (staticPath) {
      return streamFile(res, staticPath);
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
  console.log(\`Neutron Docker server listening on http://0.0.0.0:\${PORT}\`);
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
  if (pathname.includes(String.fromCharCode(0))) {
    return null;
  }
  const cleaned = pathname === "/" ? "/index.html" : pathname;
  const resolved = path.resolve(DIST_DIR, cleaned.replace(/^\\/+/, ""));
  // Containment must use a path-separator boundary; a bare startsWith() prefix
  // check would also accept sibling dirs like "<DIST_DIR>-secret".
  if (resolved !== DIST_DIR && !resolved.startsWith(DIST_DIR + path.sep)) {
    return null;
  }

  if (existsSync(resolved) && statSync(resolved).isFile()) {
    return resolved;
  }

  const nestedIndex = path.join(resolved, "index.html");
  if ((nestedIndex === DIST_DIR || nestedIndex.startsWith(DIST_DIR + path.sep)) && existsSync(nestedIndex) && statSync(nestedIndex).isFile()) {
    return nestedIndex;
  }

  return null;
}

function streamFile(res, filePath) {
  const ext = path.extname(filePath).toLowerCase();
  res.statusCode = 200;
  res.setHeader("Content-Type", MIME_TYPES[ext] || "application/octet-stream");
  // Prevent MIME sniffing of static assets (e.g. a .txt/.json that contains
  // HTML being interpreted as HTML).
  res.setHeader("X-Content-Type-Options", "nosniff");
  const stream = createReadStream(filePath);
  stream.pipe(res);
  stream.on("error", () => {
    if (!res.headersSent) {
      res.statusCode = 500;
      res.setHeader("Content-Type", "text/plain; charset=utf-8");
    }
    res.end("File stream error");
  });
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
    body: hasBody ? req : undefined,
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

  const reader = response.body.getReader();
  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    res.write(Buffer.from(value));
  }
  res.end();
}
`;
}
