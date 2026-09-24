import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

// I03 review LOW-1/MINOR-2: root-export purity pinned by a tracked test.
// The root entry (`@neutron-build/data`) must stay compilable WITHOUT any
// drizzle-orm/driver types: its emitted declaration tree (dist/index.d.ts and
// every local .d.ts it reaches transitively) may not reference drizzle-orm,
// postgres or @libsql/client. The typed surface lives behind /drizzle
// (dist/drizzle.d.ts), which DOES reference them — checked here as a positive
// control so a broken scan cannot pass vacuously.
//
// Runs against the emitted dist (node --test "dist/**/*.test.js").

const distDir = dirname(fileURLToPath(import.meta.url));

/** Module specifiers that must never appear in the root declaration tree. */
const LEAKED_MODULES = ["drizzle-orm", "postgres", "@libsql/client"];

function moduleSpecifiers(text: string): string[] {
  const specs: string[] = [];
  for (const m of text.matchAll(/from\s+"([^"]+)"/g)) specs.push(m[1]);
  for (const m of text.matchAll(/import\(\s*"([^"]+)"\s*\)/g)) specs.push(m[1]);
  return specs;
}

/** Resolve a relative .js specifier from a .d.ts file to its sibling .d.ts. */
function resolveDeclaration(fromFile: string, specifier: string): string | null {
  const base = resolve(dirname(fromFile), specifier);
  for (const candidate of [base.replace(/\.js$/, ".d.ts"), `${base}.d.ts`]) {
    try {
      readFileSync(candidate);
      return candidate;
    } catch {
      // try the next form
    }
  }
  return null;
}

/** The set of local declaration files reachable from `entry` (inclusive). */
function declarationClosure(entry: string): string[] {
  const seen = new Set<string>();
  const queue = [entry];
  while (queue.length > 0) {
    const file = queue.pop()!;
    if (seen.has(file)) continue;
    seen.add(file);
    const text = readFileSync(file, "utf8");
    for (const spec of moduleSpecifiers(text)) {
      if (!spec.startsWith("./") && !spec.startsWith("../")) continue;
      const resolved = resolveDeclaration(file, spec);
      if (resolved !== null && !seen.has(resolved)) queue.push(resolved);
    }
  }
  return [...seen].sort();
}

test("root-export purity: dist/index.d.ts tree references no drizzle-orm/driver modules", () => {
  const rootEntry = resolve(distDir, "index.d.ts");
  const closure = declarationClosure(rootEntry);
  assert.ok(closure.length > 0, "the root declaration closure must not be empty");
  const offenders: string[] = [];
  for (const file of closure) {
    const text = readFileSync(file, "utf8");
    for (const spec of moduleSpecifiers(text)) {
      const leaked = LEAKED_MODULES.some(
        (m) => spec === m || spec.startsWith(`${m}/`),
      );
      if (leaked) offenders.push(`${file}: ${spec}`);
    }
  }
  assert.deepEqual(
    offenders,
    [],
    `the root export must stay usable without drizzle-orm/driver types installed — leaked references:\n${offenders.join("\n")}`,
  );
});

test("root-export purity: positive control — the /drizzle declaration does reference drizzle-orm", () => {
  // dist/drizzle.d.ts is a thin re-export shim; the drizzle-orm references
  // live in the declaration tree it reaches (dist/db/drizzle.d.ts).
  const closure = declarationClosure(resolve(distDir, "drizzle.d.ts"));
  const specs = closure.flatMap((file) => moduleSpecifiers(readFileSync(file, "utf8")));
  assert.ok(
    specs.some((s) => s === "drizzle-orm/postgres-js" || s.startsWith("drizzle-orm/")),
    "dist/drizzle.d.ts is expected to reference drizzle-orm modules — if it stopped doing so, this purity scan's leak detection needs revisiting",
  );
});
