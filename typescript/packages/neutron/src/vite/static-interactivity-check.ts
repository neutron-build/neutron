/**
 * Build-time diagnostic for interactivity that will not run.
 *
 * A `mode: "static"` route ships no router, so its component tree is not
 * hydrated — only islands are. That is the right architecture (it is why a
 * static page can cost zero JS), but it has one failure mode that is invisible
 * at runtime: a component in a static route that uses hooks renders on the
 * server, arrives as HTML, and then simply never becomes interactive. No error,
 * no warning, no clue. The button is just dead.
 *
 * Prior to the tiering change the router hydrated every route, so this pattern
 * worked. This check exists so that upgrade shows up at build time, naming the
 * file, instead of as a bug report about a button that does nothing.
 *
 * ## Deliberately conservative
 *
 * It only reports a route when the route's own transitive project-local import
 * graph uses hooks AND the route declares no `<Island>` at all. A route that
 * uses islands is skipped entirely: determining *which* components are inside
 * an island requires real JSX analysis, and a false positive here costs an
 * author a confusing warning about correct code. Under-reporting is the right
 * error direction for a heuristic that runs on every build.
 */

import * as fs from "node:fs";
import * as path from "node:path";

/** Preact/React hooks whose presence implies the component must hydrate. */
const HOOK_RE =
  /\buse(State|Effect|LayoutEffect|Reducer|Ref|Context|Memo|Callback|Id|SyncExternalStore|Transition)\s*\(/;

const SCRIPT_EXT = [".tsx", ".ts", ".jsx", ".js", ".mjs"];
const MAX_DEPTH = 12;

export interface StaticInteractivityFinding {
  routeFile: string;
  /** The file whose hook usage will not run. */
  sourceFile: string;
}

function resolveLocalImport(spec: string, fromFile: string): string | null {
  if (!spec.startsWith(".") && !spec.startsWith("/src/") && !spec.startsWith("~/")) {
    return null;
  }
  const base = spec.startsWith(".")
    ? path.resolve(path.dirname(fromFile), spec)
    : null;
  if (!base) return null;

  if (SCRIPT_EXT.some((ext) => base.endsWith(ext)) && fs.existsSync(base)) {
    return base;
  }
  for (const ext of SCRIPT_EXT) {
    if (fs.existsSync(base + ext)) return base + ext;
    const indexed = path.join(base, "index" + ext);
    if (fs.existsSync(indexed)) return indexed;
  }
  return null;
}

function readImports(source: string): string[] {
  const specs: string[] = [];
  const re = /(?:import|export)[^"']*?["']([^"']+)["']/g;
  let match: RegExpExecArray | null;
  while ((match = re.exec(source)) !== null) {
    specs.push(match[1]);
  }
  return specs;
}

/**
 * Walk a route's project-local import graph looking for hook usage.
 * Returns the first offending file, or null.
 */
function findHookUsage(routeFile: string): string | null {
  const seen = new Set<string>();
  const queue: Array<{ file: string; depth: number }> = [{ file: routeFile, depth: 0 }];

  while (queue.length > 0) {
    const { file, depth } = queue.shift()!;
    if (seen.has(file) || depth > MAX_DEPTH) continue;
    seen.add(file);

    let source: string;
    try {
      source = fs.readFileSync(file, "utf-8");
    } catch {
      continue;
    }

    // An island declaration anywhere in the graph means this route has opted
    // into explicit hydration; stop analysing it (see the conservatism note).
    if (source.includes("<Island")) return null;
    if (HOOK_RE.test(source)) return file;

    for (const spec of readImports(source)) {
      const resolved = resolveLocalImport(spec, file);
      if (resolved) queue.push({ file: resolved, depth: depth + 1 });
    }
  }
  return null;
}

/**
 * Report static routes whose interactivity will not run.
 *
 * `routes` is the discovered route list; only non-layout routes with
 * `mode: "static"` and no explicit `hydrate: true` are considered.
 */
export function findStaticInteractivity(
  routes: Array<{
    file: string;
    isLayout?: boolean;
    config: { mode: "static" | "app"; hydrate?: boolean };
  }>
): StaticInteractivityFinding[] {
  const findings: StaticInteractivityFinding[] = [];
  for (const route of routes) {
    if (route.isLayout) continue;
    if (route.config.mode !== "static") continue;
    // `hydrate: true` is the documented escape hatch: it pulls the route up to
    // the full tier, which hydrates everything. Nothing to warn about.
    if (route.config.hydrate === true) continue;

    const offender = findHookUsage(route.file);
    if (offender) {
      findings.push({ routeFile: route.file, sourceFile: offender });
    }
  }
  return findings;
}

/** Human-readable warning, or an empty string when there is nothing to say. */
export function formatStaticInteractivityWarning(
  findings: StaticInteractivityFinding[],
  rootDir: string
): string {
  if (findings.length === 0) return "";
  const rel = (p: string) => path.relative(rootDir, p).replace(/\\/g, "/");

  const lines = [
    "",
    "[neutron] Interactivity in a static route will not run.",
    "",
    "  A `mode: \"static\"` route ships no client router, so its components are",
    "  not hydrated — only islands are. These routes use hooks outside an island,",
    "  so they will render as HTML and then stay inert:",
    "",
  ];
  for (const f of findings) {
    lines.push(`    ${rel(f.routeFile)}`);
    if (f.sourceFile !== f.routeFile) {
      lines.push(`      via ${rel(f.sourceFile)}`);
    }
  }
  lines.push(
    "",
    "  Two ways to fix it:",
    "",
    "    1. Wrap the interactive component in <Island> — ships only that",
    "       component's code, and is the reason a static page can cost zero JS.",
    "    2. Add `hydrate: true` to the route's config to hydrate the whole tree,",
    "       which is what every static route did before client tiering.",
    ""
  );
  return lines.join("\n");
}
