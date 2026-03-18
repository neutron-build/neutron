import * as fs from "node:fs";
import * as path from "node:path";
import type { AdapterBuildContext, NeutronAdapter } from "./adapter.js";
import {
  readStaticHeadersMap,
  toVercelHeaders,
} from "./headers.js";

export interface VercelAdapterOptions {
  allowAppRoutes?: boolean;
  cleanUrls?: boolean;
  trailingSlash?: boolean;
}

export function adapterVercel(
  options: VercelAdapterOptions = {}
): NeutronAdapter {
  return {
    name: "vercel",
    async adapt(context: AdapterBuildContext) {
      const hasAppRoutes = context.routes.app > 0;
      if (hasAppRoutes && !context.ensureRuntimeBundle) {
        if (!options.allowAppRoutes) {
          throw new Error("Vercel adapter requires runtime bundle support from neutron build.");
        }
        context.log(
          `Warning: ${context.routes.app} app route(s) found but no runtime bundler available. ` +
          "App routes will not be functional in the deployed output."
        );
      }

      const headersByRoute = readStaticHeadersMap(context.outDir);
      const vercelHeaders = toVercelHeaders(headersByRoute);
      const vercelConfig: {
        version: number;
        cleanUrls: boolean;
        trailingSlash: boolean;
        headers?: Array<{ source: string; headers: Array<{ key: string; value: string }> }>;
        routes?: Array<{ handle?: "filesystem"; src?: string; dest?: string }>;
      } = {
        version: 2,
        cleanUrls: options.cleanUrls ?? true,
        trailingSlash: options.trailingSlash ?? false,
      };
      if (vercelHeaders.length > 0) {
        vercelConfig.headers = vercelHeaders;
      }

      if (hasAppRoutes && context.ensureRuntimeBundle) {
        const runtimeBundle = await context.ensureRuntimeBundle("node");
        const apiDir = path.join(context.outDir, "api");
        const apiHandlerPath = path.join(apiDir, "__neutron.mjs");
        fs.mkdirSync(apiDir, { recursive: true });

        const runtimeImport = path
          .relative(path.dirname(apiHandlerPath), runtimeBundle.entryPath)
          .split(path.sep)
          .join("/");
        const normalizedImport = runtimeImport.startsWith(".")
          ? runtimeImport
          : `./${runtimeImport}`;
        fs.writeFileSync(apiHandlerPath, buildVercelApiHandlerSource(normalizedImport), "utf-8");
        context.log(
          `Vercel API handler written: ${path.relative(context.rootDir, apiHandlerPath)}`
        );

        vercelConfig.routes = [
          { handle: "filesystem" },
          { src: "/.*", dest: "/api/__neutron" },
        ];
      }

      const vercelConfigPath = path.join(context.outDir, "vercel.json");
      fs.writeFileSync(vercelConfigPath, JSON.stringify(vercelConfig, null, 2), "utf-8");
      context.log(
        `Vercel config written: ${path.relative(context.rootDir, vercelConfigPath)}`
      );

      const metadata = {
        adapter: "vercel",
        routes: context.routes,
        generatedAt: new Date().toISOString(),
      };
      const metadataPath = path.join(context.outDir, ".neutron-adapter-vercel.json");
      fs.writeFileSync(metadataPath, JSON.stringify(metadata, null, 2), "utf-8");
      context.log(
        `Vercel adapter metadata written: ${path.relative(context.rootDir, metadataPath)}`
      );
    },
  };
}

function buildVercelApiHandlerSource(runtimeImportPath: string): string {
  return `import { handleNeutronRequest } from "${runtimeImportPath}";

function toWebRequest(req) {
  const host = req.headers?.host || "localhost";
  const url = new URL(req.url || "/", \`http://\${host}\`);
  const headers = new Headers();
  for (const [key, value] of Object.entries(req.headers || {})) {
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

async function writeNodeResponse(res, response) {
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

export default async function handler(req, res) {
  const request = toWebRequest(req);
  const response = await handleNeutronRequest(request);
  await writeNodeResponse(res, response);
}
`;
}
