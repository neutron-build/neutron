import * as http from "node:http";
import * as fs from "node:fs";
import * as path from "node:path";
import {
  compileRouteRules,
  discoverRoutes,
  prepareContentCollections,
  resolveRouteRuleHeaders,
  resolveRouteRuleRedirect,
  resolveRouteRuleRewrite,
  resolveRuntime,
} from "neutron";
import { startServer } from "neutron/server";
import type { NeutronConfig } from "neutron";
import { loadConfigFromFile, loadEnv } from "vite";

export async function preview(): Promise<void> {
  const cwd = process.cwd();
  applyEnv(cwd, "production");
  const neutronConfig = await loadNeutronConfig(cwd);
  await prepareContentCollections({
    rootDir: cwd,
    writeManifest: false,
    writeTypes: false,
  });
  const distDir = path.resolve(cwd, "dist");
  const routesDir = path.resolve(cwd, "src/routes");
  const port = neutronConfig.server?.port || 4173;
  const compiledRouteRules = compileRouteRules(neutronConfig.routes);

  if (!fs.existsSync(distDir)) {
    console.error(`Build output not found: ${distDir}`);
    console.error(`Run 'neutron build' first.`);
    process.exit(1);
  }

  const hasAppRoutes = detectAppRoutes(routesDir);
  if (hasAppRoutes) {
    console.log("\nDetected app routes. Starting Neutron production preview server...\n");
    await startServer({
      ...neutronConfig.server,
      routes: neutronConfig.routes,
      runtime: resolveRuntime(neutronConfig),
      rootDir: cwd,
      distDir: "dist",
      routesDir: "src/routes",
      host: neutronConfig.server?.host || "0.0.0.0",
      port,
    });
    return;
  }

  console.log("\nDetected static-only build. Starting static preview server...\n");

  const server = http.createServer((req, res) => {
    const requestUrl = resolveRequestUrl(req);
    if (!requestUrl) {
      res.statusCode = 400;
      res.setHeader("Content-Type", "text/plain");
      res.end("Bad Request");
      return;
    }

    const pathname = normalizePathname(requestUrl.pathname);
    if (!pathname) {
      res.statusCode = 400;
      res.setHeader("Content-Type", "text/plain");
      res.end("Bad Request");
      return;
    }

    const redirect = resolveRouteRuleRedirect(compiledRouteRules, pathname, requestUrl.search);
    if (redirect) {
      res.statusCode = redirect.status;
      res.setHeader("Location", redirect.location);
      res.end();
      return;
    }

    const rewrite = resolveRouteRuleRewrite(compiledRouteRules, pathname);
    const effectivePathname = normalizePathname(rewrite?.pathname || pathname);
    if (!effectivePathname) {
      res.statusCode = 400;
      res.setHeader("Content-Type", "text/plain");
      res.end("Bad Request");
      return;
    }

    // Serve static files
    let filePath = resolveDistFilePath(distDir, effectivePathname);
    if (!filePath) {
      res.statusCode = 403;
      res.setHeader("Content-Type", "text/plain");
      res.end("Forbidden");
      return;
    }

    // Try index.html for directories
    if (fs.existsSync(filePath) && fs.statSync(filePath).isDirectory()) {
      filePath = path.join(filePath, "index.html");
      if (!isWithinDirectory(distDir, filePath)) {
        res.statusCode = 403;
        res.setHeader("Content-Type", "text/plain");
        res.end("Forbidden");
        return;
      }
    }

    // Try .html extension
    if (!fs.existsSync(filePath)) {
      const htmlPath = filePath + ".html";
      if (fs.existsSync(htmlPath) && isWithinDirectory(distDir, htmlPath)) {
        filePath = htmlPath;
      }
    }

    if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
      const ext = path.extname(filePath);
      const types: Record<string, string> = {
        ".html": "text/html",
        ".css": "text/css",
        ".js": "application/javascript",
        ".mjs": "application/javascript",
        ".json": "application/json",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".svg": "image/svg+xml",
        ".ico": "image/x-icon",
        ".woff": "font/woff",
        ".woff2": "font/woff2",
      };

      res.setHeader("Content-Type", types[ext] || "application/octet-stream");
      applyRouteRuleHeaders(
        res,
        resolveRouteRuleHeaders(compiledRouteRules, pathname)
      );
      
      const stream = fs.createReadStream(filePath);
      stream.pipe(res);
      stream.on("error", () => {
        res.statusCode = 500;
        res.end("Internal Server Error");
      });
    } else {
      res.statusCode = 404;
      res.setHeader("Content-Type", "text/plain");
      applyRouteRuleHeaders(
        res,
        resolveRouteRuleHeaders(compiledRouteRules, pathname)
      );
      res.end("Not Found");
    }
  });

  server.listen(port, "0.0.0.0", () => {
    console.log(`\n  Preview server running:\n`);
    console.log(`  Local:   http://localhost:${port}\n`);
    console.log(`  Press Ctrl+C to stop\n`);
  });

  process.on("SIGTERM", () => {
    console.log("\nShutting down...");
    server.close();
    process.exit(0);
  });

  process.on("SIGINT", () => {
    console.log("\nShutting down...");
    server.close();
    process.exit(0);
  });
}

function detectAppRoutes(routesDir: string): boolean {
  if (!fs.existsSync(routesDir)) {
    return false;
  }

  const routes = discoverRoutes({ routesDir });
  return routes.some((route) => !route.file.includes("_layout") && route.config.mode === "app");
}

async function loadNeutronConfig(cwd: string): Promise<NeutronConfig> {
  const candidates = [
    "neutron.config.ts",
    "neutron.config.js",
    "neutron.config.mjs",
    "neutron.config.cjs",
  ];

  for (const file of candidates) {
    const fullPath = path.resolve(cwd, file);
    if (!fs.existsSync(fullPath)) {
      continue;
    }

    const loaded = await loadConfigFromFile(
      { command: "serve", mode: "production" },
      fullPath,
      cwd
    );
    if (loaded?.config) {
      return loaded.config as NeutronConfig;
    }
  }

  return {};
}

function applyEnv(cwd: string, mode: string): void {
  const env = loadEnv(mode, cwd, "");
  for (const [key, value] of Object.entries(env)) {
    if (process.env[key] === undefined) {
      process.env[key] = value;
    }
  }
}

function normalizePathname(pathname: string): string {
  if (!pathname) {
    return "/";
  }

  let decoded: string;
  try {
    decoded = decodeURIComponent(pathname);
  } catch {
    return "";
  }

  if (!decoded.startsWith("/") || decoded.includes("..")) {
    return "";
  }

  if (decoded.length > 1 && decoded.endsWith("/")) {
    return decoded.slice(0, -1);
  }
  return decoded;
}

function applyRouteRuleHeaders(
  response: http.ServerResponse,
  matches: Array<{ headers: Record<string, string> }>
): void {
  for (const match of matches) {
    for (const [name, value] of Object.entries(match.headers || {})) {
      if (!response.hasHeader(name)) {
        response.setHeader(name, String(value));
      }
    }
  }
}

function resolveRequestUrl(req: http.IncomingMessage): URL | null {
  const host = req.headers.host || "localhost";
  try {
    return new URL(req.url || "/", `http://${host}`);
  } catch {
    return null;
  }
}

function resolveDistFilePath(distDir: string, pathname: string): string | null {
  const resolved = path.resolve(distDir, `.${pathname}`);
  return isWithinDirectory(distDir, resolved) ? resolved : null;
}

function isWithinDirectory(baseDir: string, candidatePath: string): boolean {
  const relative = path.relative(baseDir, candidatePath);
  return (
    relative === "" ||
    (!relative.startsWith("..") && !path.isAbsolute(relative))
  );
}
