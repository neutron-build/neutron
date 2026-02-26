import { Hono } from "hono";
import { serveStatic } from "@hono/node-server/serve-static";
import { compress } from "hono/compress";
import type { Context, MiddlewareHandler } from "hono";

export interface NeutronHonoOptions {
  distDir?: string;
  routesDir?: string;
  compress?: boolean;
}

export function createNeutronHono(options: NeutronHonoOptions = {}): Hono {
  const {
    distDir = "./dist",
    routesDir = "./src/routes",
    compress: enableCompress = true,
  } = options;

  const app = new Hono();

  // Compression middleware
  if (enableCompress) {
    app.use("*", compress());
  }

  // Static files from dist/public
  app.use(
    "/*",
    serveStatic({
      root: `${distDir}/public`,
      rewriteRequestPath: (path) => path,
    })
  );

  // Assets with cache headers
  app.use(
    "/assets/*",
    serveStatic({
      root: distDir,
      rewriteRequestPath: (path) => path,
    })
  );

  // Pre-rendered static HTML pages
  app.use("/*", serveStatic({ root: distDir }));

  return app;
}
