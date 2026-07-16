import { parse } from "@babel/parser";
import MagicString from "magic-string";

const SERVER_EXPORT_NAMES = new Set([
  "loader",
  "action",
  "middleware",
  "headers",
  "getStaticPaths",
  // HTTP-method handlers for API routes (e.g. rss.xml.ts) are server-only.
  // Without stripping these from the client route module, their server-side
  // imports (e.g. getCollection, which reads the filesystem) leak into the
  // browser bundle and break the build.
  "GET",
  "POST",
  "PUT",
  "PATCH",
  "DELETE",
  "HEAD",
  "OPTIONS",
]);

const SERVER_FILE_RE = /\.server(?:\.[^/\\?#]+)?(?:[?#].*)?$/i;

interface ImportRecord {
  source: string;
}

interface ParseNode {
  type: string;
  start: number;
  end: number;
  source?: { value?: string };
  specifiers?: Array<{
    type: string;
    start: number;
    end: number;
    local?: { name?: string };
    exported?: { name?: string };
  }>;
  declaration?: {
    type: string;
    id?: { name?: string };
    kind?: string;
    declarations?: Array<{
      id?: { name?: string };
      start: number;
      end: number;
    }>;
  };
}

interface ParseProgram {
  body: ParseNode[];
  program?: {
    body: ParseNode[];
  };
}

function getProgramBody(ast: ParseProgram): ParseNode[] {
  if (Array.isArray(ast.body)) {
    return ast.body;
  }
  if (ast.program && Array.isArray(ast.program.body)) {
    return ast.program.body;
  }
  return [];
}

export function isServerOnlySpecifier(specifier: string): boolean {
  const clean = specifier.split("?")[0].split("#")[0];
  return SERVER_FILE_RE.test(clean);
}

// The content runtime (getCollection/getEntry/prepareContentCollections/…)
// reads the filesystem, hashes with node:crypto, and compiles MDX — it is
// server/build-time only and must never be bundled for the browser. Content is
// loaded during SSR / getStaticPaths and passed to components/islands as props.
// A component that merely *imports* a content API (e.g. a docs shell that also
// exports a data-loading helper) otherwise drags this whole module — and its
// node: builtins — into the client bundle and breaks the build. Matched cases:
// the public subpath, a fully-resolved module path (npm dist, pnpm store, or
// workspace src), and the root barrel's relative re-export of it.
const CONTENT_MODULE_RE =
  /(?:@neutron-build[/+]core|packages[/\\]neutron)[/\\](?:dist|src)[/\\]content[/\\]index\.[cm]?[jt]sx?$/;
const CORE_INDEX_RE =
  /(?:@neutron-build[/+]core|packages[/\\]neutron)[/\\](?:dist|src)[/\\]index\.[cm]?[jt]sx?$/;

export function isContentModuleId(id: string, importer?: string): boolean {
  const clean = id.split("?")[0].split("#")[0];
  if (clean === "@neutron-build/core/content") return true;
  if (CONTENT_MODULE_RE.test(clean)) return true;
  // Root barrel re-export: `export … from "./content/index.js"` inside the
  // core package's own index module.
  if (
    importer &&
    /[/\\]content[/\\]index\.[cm]?[jt]sx?$/.test(clean) &&
    CORE_INDEX_RE.test(importer.split("?")[0].split("#")[0])
  ) {
    return true;
  }
  return false;
}

// Node built-in modules (import { readFileSync } from "node:fs" — or the bare
// "fs"/"path"/... aliases). Used ONLY when stripping the CLIENT half of a
// route module: such an import survives only because a now-stripped server
// export (loader/action) used it, and left in place it breaks the browser
// bundle. This is deliberately NOT part of isServerOnlySpecifier — that would
// make every client node: import silently resolve to empty and mask genuine
// mistakes; here it is scoped to the route-client transform.
const NODE_BUILTIN_RE = /^node:/;
const BARE_NODE_BUILTINS = new Set([
  "assert", "async_hooks", "buffer", "child_process", "cluster", "console",
  "crypto", "dgram", "diagnostics_channel", "dns", "domain", "events", "fs",
  "http", "http2", "https", "inspector", "module", "net", "os", "path",
  "perf_hooks", "process", "punycode", "querystring", "readline", "repl",
  "stream", "string_decoder", "timers", "tls", "trace_events", "tty", "url",
  "util", "v8", "vm", "wasi", "worker_threads", "zlib",
]);

function isNodeBuiltinImport(specifier: string): boolean {
  const clean = specifier.split("?")[0].split("#")[0];
  if (NODE_BUILTIN_RE.test(clean)) return true;
  const root = clean.split("/")[0];
  return BARE_NODE_BUILTINS.has(root);
}

export function parseImports(code: string): ImportRecord[] {
  const ast = parse(code, {
    sourceType: "module",
    plugins: ["typescript", "jsx"],
    errorRecovery: true,
  }) as unknown as ParseProgram;

  const imports: ImportRecord[] = [];
  for (const node of getProgramBody(ast)) {
    if (node.type !== "ImportDeclaration") {
      continue;
    }
    const source = typeof node.source?.value === "string" ? node.source.value : "";
    imports.push({ source });
  }
  return imports;
}

export function hasServerOnlyImport(code: string): boolean {
  try {
    return parseImports(code).some((record) => isServerOnlySpecifier(record.source));
  } catch {
    // Fallback: code may have been pre-transformed by another plugin (e.g. prefresh)
    // making it unparseable. Use regex heuristic instead.
    return /import\s+.*?from\s+['"]([^'"]*\.server(?:\.[^'"]*)?)['"]/m.test(code);
  }
}

export function stripServerOnlyRouteModule(code: string): string {
  const ast = parse(code, {
    sourceType: "module",
    plugins: ["typescript", "jsx"],
  }) as unknown as ParseProgram;
  const magic = new MagicString(code);

  for (const node of getProgramBody(ast)) {
    if (node.type === "ImportDeclaration") {
      const source = typeof node.source?.value === "string" ? node.source.value : "";
      if (isServerOnlySpecifier(source) || isNodeBuiltinImport(source)) {
        magic.remove(node.start, node.end);
      }
      continue;
    }

    if (node.type !== "ExportNamedDeclaration") {
      continue;
    }

    if (node.declaration) {
      const declaration = node.declaration;

      if (declaration.type === "FunctionDeclaration") {
        const exportName = declaration.id?.name;
        if (exportName && SERVER_EXPORT_NAMES.has(exportName)) {
          magic.remove(node.start, node.end);
        }
        continue;
      }

      if (declaration.type === "VariableDeclaration") {
        const declarators = declaration.declarations || [];
        const kept = declarators.filter((decl) => {
          const name = decl.id?.name;
          return !name || !SERVER_EXPORT_NAMES.has(name);
        });

        if (kept.length === declarators.length) {
          continue;
        }

        if (kept.length === 0) {
          magic.remove(node.start, node.end);
          continue;
        }

        const kind = declaration.kind || "const";
        const rebuilt = kept
          .map((decl) => code.slice(decl.start, decl.end))
          .join(", ");
        magic.overwrite(node.start, node.end, `export ${kind} ${rebuilt};`);
      }

      continue;
    }

    const specifiers = node.specifiers || [];
    if (specifiers.length === 0) {
      continue;
    }

    const kept = specifiers.filter((specifier) => {
      const exportedName = specifier.exported?.name || specifier.local?.name || "";
      const localName = specifier.local?.name || "";
      return (
        !SERVER_EXPORT_NAMES.has(exportedName) &&
        !SERVER_EXPORT_NAMES.has(localName)
      );
    });

    if (kept.length === specifiers.length) {
      continue;
    }

    if (kept.length === 0) {
      magic.remove(node.start, node.end);
      continue;
    }

    const rebuilt = kept.map((specifier) => code.slice(specifier.start, specifier.end)).join(", ");
    magic.overwrite(node.start, node.end, `export { ${rebuilt} };`);
  }

  return magic.toString();
}

export function stripQueryFromId(id: string): string {
  return id.split("?")[0];
}
