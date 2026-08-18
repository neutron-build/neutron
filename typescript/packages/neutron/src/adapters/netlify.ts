import * as fs from "node:fs";
import * as path from "node:path";
import type { AdapterBuildContext, NeutronAdapter } from "./adapter.js";
import {
  readStaticHeadersMap,
  toCloudflareHeadersFile,
} from "./headers.js";

export interface NetlifyAdapterOptions {
  allowAppRoutes?: boolean;
  functionName?: string;
}

export function adapterNetlify(
  options: NetlifyAdapterOptions = {}
): NeutronAdapter {
  return {
    name: "netlify",
    async adapt(context: AdapterBuildContext) {
      const hasAppRoutes = context.routes.app > 0;
      if (hasAppRoutes && !context.ensureRuntimeBundle) {
        if (!options.allowAppRoutes) {
          throw new Error("Netlify adapter requires runtime bundle support from neutron build.");
        }
        context.log(
          `Warning: ${context.routes.app} app route(s) found but no runtime bundler available. ` +
          "App routes will not be functional in the deployed output."
        );
      }

      // Netlify reads `_headers` from the publish directory (the file format
      // originated on Netlify; toCloudflareHeadersFile emits it).
      const headersByRoute = readStaticHeadersMap(context.outDir);
      const headersFile = toCloudflareHeadersFile(headersByRoute);
      if (headersFile.trim().length > 0) {
        const headersPath = path.join(context.outDir, "_headers");
        fs.writeFileSync(headersPath, headersFile, "utf-8");
        context.log(
          `Netlify headers file written: ${path.relative(context.rootDir, headersPath)}`
        );
      }

      const functionName = options.functionName || "__neutron";
      if (hasAppRoutes && context.ensureRuntimeBundle) {
        const runtimeBundle = await context.ensureRuntimeBundle("node");
        const functionsDir = path.join(context.outDir, "functions");
        const functionPath = path.join(functionsDir, `${functionName}.mjs`);
        fs.mkdirSync(functionsDir, { recursive: true });

        const runtimeImport = path
          .relative(path.dirname(functionPath), runtimeBundle.entryPath)
          .split(path.sep)
          .join("/");
        const normalizedImport = runtimeImport.startsWith(".")
          ? runtimeImport
          : `./${runtimeImport}`;
        fs.writeFileSync(functionPath, buildNetlifyFunctionSource(normalizedImport), "utf-8");
        context.log(
          `Netlify function written: ${path.relative(context.rootDir, functionPath)}`
        );

        // Netlify reads `_redirects` from the publish directory. A non-forced
        // rewrite only applies when no static file matches the request path,
        // so prerendered/static routes keep being served as files and
        // everything else falls through to the SSR function.
        const redirectsPath = path.join(context.outDir, "_redirects");
        fs.writeFileSync(
          redirectsPath,
          `/*  /.netlify/functions/${functionName}  200\n`,
          "utf-8"
        );
        context.log(
          `Netlify redirects file written: ${path.relative(context.rootDir, redirectsPath)}`
        );
      }

      const publishDir = path.basename(context.outDir);
      const netlifyConfigPath = path.join(context.outDir, "netlify.toml");
      fs.writeFileSync(
        netlifyConfigPath,
        buildNetlifyTomlSource({ publishDir, functionName, hasAppRoutes }),
        "utf-8"
      );
      context.log(
        `Netlify config written: ${path.relative(context.rootDir, netlifyConfigPath)} (copy to project root for Netlify's git integration)`
      );

      const metadata = {
        adapter: "netlify",
        routes: context.routes,
        generatedAt: new Date().toISOString(),
      };
      const metadataPath = path.join(context.outDir, ".neutron-adapter-netlify.json");
      fs.writeFileSync(metadataPath, JSON.stringify(metadata, null, 2), "utf-8");
      context.log(
        `Netlify adapter metadata written: ${path.relative(context.rootDir, metadataPath)}`
      );
    },
  };
}

function buildNetlifyFunctionSource(runtimeImportPath: string): string {
  return `import { handleNeutronRequest } from "${runtimeImportPath}";

export default async function handler(request) {
  return handleNeutronRequest(request);
};
`;
}

function buildNetlifyTomlSource(options: {
  publishDir: string;
  functionName: string;
  hasAppRoutes: boolean;
}): string {
  const lines = [
    "[build]",
    '  command = "npx @neutron-build/cli build --preset netlify"',
    `  publish = "${options.publishDir}"`,
    "",
    "[functions]",
    `  directory = "${options.publishDir}/functions"`,
  ];
  if (options.hasAppRoutes) {
    lines.push(
      "",
      "[[redirects]]",
      '  from = "/*"',
      `  to = "/.netlify/functions/${options.functionName}"`,
      "  status = 200"
    );
  }
  return lines.join("\n") + "\n";
}
