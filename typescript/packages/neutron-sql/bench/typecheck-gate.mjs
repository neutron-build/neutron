#!/usr/bin/env node
// @neutron-build/sql — consumer type-check cost gate (orm-program R01, V16).
//
//   node bench/typecheck-gate.mjs [--runs N] [--tsc path/to/typescript] [--out file.json] [--gate]
//
// Generates a deterministic consumer project — 100 tables (12 columns each,
// the column kinds a typical app uses), a parent/child chain between
// neighbours plus two named references per table to the same target
// (repeated targets), and one depth-3 relational query per table with nested
// filters, per-child column subsets, to-one leaves and exact-type assertions
// — and type-checks it with `tsc --extendedDiagnostics` against the built
// declarations (dist/*.d.ts), exactly what an application compiles against.
//
// Reported: check time, total time, memory, type count and instantiation
// count (medians over --runs). Types and instantiations are deterministic for
// a given TypeScript version, so --gate enforces them against
// bench/budgets.json (baseline + the V16 20% allowance) on any machine; time
// and memory depend on the machine and are reported, not gated.

import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PKG = path.resolve(HERE, "..");
const require_ = createRequire(path.join(PKG, "package.json"));

const TABLES = 100;
const argv = process.argv.slice(2);
const opt = (name, fallback) => {
  const i = argv.indexOf(name);
  return i >= 0 ? argv[i + 1] : fallback;
};
const RUNS = Number(opt("--runs", "3"));
const GATE = argv.includes("--gate");
const OUT = opt("--out", null);
const tsDir = opt("--tsc", path.dirname(require_.resolve("typescript/package.json")));
const tscJs = path.join(tsDir, "lib", "tsc.js");
const tsVersion = JSON.parse(readFileSync(path.join(tsDir, "package.json"), "utf8")).version;
const declarations = path.join(PKG, "dist", "index.d.ts");
try {
  readFileSync(declarations);
} catch {
  console.error("typecheck-gate: dist/index.d.ts missing — run `pnpm build` first");
  process.exit(2);
}

const pad = (i) => String(i).padStart(3, "0");

function schemaSource() {
  const out = [`import { pgTable, serial, integer, bigint, text, numeric, boolean, timestamptz, date, jsonb, relations } from "@neutron-build/sql";`, ""];
  for (let i = 0; i < TABLES; i++) {
    out.push(`export const t${pad(i)} = pgTable("t${pad(i)}", {
  id: serial("id").primaryKey(),
  parentId: integer("parent_id"),
  ownerId: integer("owner_id").notNull(),
  reviewerId: integer("reviewer_id"),
  name: text("name").notNull(),
  slug: text("slug").notNull().unique(),
  amount: numeric("amount"),
  quantity: bigint("quantity"),
  active: boolean("active").notNull().default(true),
  createdAt: timestamptz("created_at").notNull().defaultNow(),
  day: date("day"),
  meta: jsonb("meta"),
});`);
  }
  out.push("");
  for (let i = 0; i < TABLES; i++) {
    const t = `t${pad(i)}`;
    const parts = [];
    if (i > 0) parts.push(`parent: one(t${pad(i - 1)}, { fields: [${t}.parentId], references: [t${pad(i - 1)}.id] })`);
    if (i < TABLES - 1) parts.push(`children: many(t${pad(i + 1)})`);
    // Two references from every table to the same target (t000): repeated
    // targets need named relations and separate aliases.
    parts.push(`owner: one(t000, { fields: [${t}.ownerId], references: [t000.id], relationName: "owner_${pad(i)}" })`);
    parts.push(`reviewer: one(t000, { fields: [${t}.reviewerId], references: [t000.id], relationName: "reviewer_${pad(i)}" })`);
    out.push(`export const ${t}Relations = relations(${t}, ({ one, many }) => ({\n  ${parts.join(",\n  ")},\n}));`);
  }
  out.push("");
  out.push(`export const tables = { ${Array.from({ length: TABLES }, (_, i) => `t${pad(i)}`).join(", ")} };`);
  out.push(`export const relationMap = { ${Array.from({ length: TABLES }, (_, i) => `t${pad(i)}: t${pad(i)}Relations`).join(", ")} };`);
  return out.join("\n") + "\n";
}

function queriesSource() {
  const out = [
    `import { createDatabase, and, eq, gt, asc, desc } from "@neutron-build/sql";`,
    `import * as s from "./schema.js";`,
    "",
    `type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2) ? true : false;`,
    `function assertType<T extends true>(): T | undefined { return undefined; }`,
    "",
    `export async function run(url: string) {`,
    `  const db = await createDatabase({ url, tables: s.tables, relations: s.relationMap });`,
  ];
  for (let i = 0; i < TABLES - 3; i++) {
    const t = `t${pad(i)}`;
    const c1 = `t${pad(i + 1)}`;
    const c2 = `t${pad(i + 2)}`;
    out.push(`  {
    const rows = await db.query.${t}.findMany({
      columns: ["id", "name", "amount", "createdAt"],
      where: and(gt(s.${t}.id, 0), eq(s.${t}.active, true)),
      orderBy: [asc(s.${t}.id)],
      limit: 10,
      with: {
        owner: { columns: ["id", "name"] },
        children: {
          columns: ["id", "slug", "quantity"],
          where: gt(s.${c1}.id, 1),
          orderBy: [desc(s.${c1}.createdAt)],
          limit: 5,
          with: {
            reviewer: true,
            children: { columns: ["id", "day"], with: { owner: { columns: ["name"] } } },
          },
        },
      },
    });
    const first = rows[0];
    if (first) {
      assertType<Equal<typeof first.amount, string | null>>();
      assertType<Equal<typeof first.owner, { id: number; name: string } | null>>();
      const child = first.children[0];
      if (child) {
        assertType<Equal<typeof child.quantity, bigint | null>>();
        const grand = child.children[0];
        if (grand) assertType<Equal<typeof grand.owner, { name: string } | null>>();
      }
    }
    await db.insert(s.${c2}).values({ ownerId: 1, name: "n", slug: "s${i}" });
    await db.update(s.${c2}).set({ amount: "1.50" }).where(eq(s.${c2}.id, 1));
  }`);
  }
  out.push("  await db.close();", "}");
  return out.join("\n") + "\n";
}

function parseDiagnostics(text) {
  const num = (label) => {
    const m = new RegExp(`^${label}:\\s+([\\d.]+)`, "m").exec(text);
    if (!m) throw new Error(`tsc --extendedDiagnostics output lacks "${label}"`);
    return Number(m[1]);
  };
  return { types: num("Types"), instantiations: num("Instantiations"), memoryKiB: num("Memory used"), checkSec: num("Check time"), totalSec: num("Total time") };
}

const dir = mkdtempSync(path.join(os.tmpdir(), "nsql-typecheck-"));
let result;
try {
  writeFileSync(path.join(dir, "schema.ts"), schemaSource());
  writeFileSync(path.join(dir, "queries.ts"), queriesSource());
  writeFileSync(
    path.join(dir, "tsconfig.json"),
    JSON.stringify({
      compilerOptions: {
        target: "ES2022",
        module: "ESNext",
        moduleResolution: "bundler",
        lib: ["ES2022"],
        strict: true,
        skipLibCheck: true,
        noEmit: true,
        types: [],
        paths: { "@neutron-build/sql": [declarations] },
      },
      include: ["*.ts"],
    }),
  );
  const runs = [];
  for (let i = 0; i < RUNS; i++) {
    let text;
    try {
      text = execFileSync(process.execPath, [tscJs, "-p", path.join(dir, "tsconfig.json"), "--extendedDiagnostics", "--pretty", "false"], { encoding: "utf8" });
    } catch (err) {
      console.error(`typecheck-gate: the generated consumer project does not type-check with TypeScript ${tsVersion}:\n${(err.stdout ?? "").split("\n").slice(0, 30).join("\n")}`);
      process.exit(1);
    }
    runs.push(parseDiagnostics(text));
  }
  const median = (key) => {
    const v = runs.map((r) => r[key]).sort((a, b) => a - b);
    return v[Math.floor(v.length / 2)];
  };
  result = {
    tool: "neutron-sql typecheck-gate",
    typescript: tsVersion,
    node: process.version,
    platform: `${process.platform}-${process.arch}`,
    cpu: os.cpus()[0]?.model ?? "unknown",
    tables: TABLES,
    queries: TABLES - 3,
    runs: RUNS,
    types: median("types"),
    instantiations: median("instantiations"),
    memoryKiB: median("memoryKiB"),
    checkSec: median("checkSec"),
    totalSec: median("totalSec"),
    samples: runs,
    failures: [],
  };
} finally {
  rmSync(dir, { recursive: true, force: true });
}

const budget = JSON.parse(readFileSync(path.join(HERE, "budgets.json"), "utf8")).typecheck?.[result.typescript];
if (budget) {
  const pct = budget.regressionPct ?? 20;
  for (const key of ["types", "instantiations"]) {
    const max = Math.floor(budget.baseline[key] * (1 + pct / 100));
    result[`${key}Max`] = max;
    if (result[key] > max) result.failures.push(`${key} ${result[key]} exceeds ${max} (baseline ${budget.baseline[key]} + ${pct}%) — investigate before re-recording`);
  }
} else if (GATE) {
  result.failures.push(`no typecheck baseline recorded for TypeScript ${result.typescript} in bench/budgets.json`);
}

const json = JSON.stringify(result, null, 2);
if (OUT) writeFileSync(OUT, json + "\n");
else process.stdout.write(json + "\n");
console.error(
  `typecheck-gate: TypeScript ${result.typescript}: ${result.types} types, ${result.instantiations} instantiations, check ${result.checkSec}s, total ${result.totalSec}s, ${Math.round(result.memoryKiB / 1024)} MiB`,
);
for (const f of result.failures) console.error(`typecheck-gate FAIL ${f}`);
if (GATE && result.failures.length > 0) process.exit(1);
