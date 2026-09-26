#!/usr/bin/env node
// @neutron-build/sql — installed-artifact gate (orm-program R02, VERIFICATION V17).
//
//   node scripts/installed-gate.mjs [--ts 5.7.2,5.9.3] [--keep-tarball dir] [--keep-work] [--out file.json]
//
// Packs the package exactly as `pnpm publish` would (fresh build, then
// `pnpm pack`) into a temporary directory and checks the artifact, then
// installs it into two clean consumer projects OUTSIDE the monorepo — one
// with only `pg`, one with only `postgres` — and checks what an application
// actually gets:
//
//   tarball   only package.json, README, LICENSE and dist/ (no tests, live
//             harness, bench/, scripts/ or sources); every `exports` target
//             present; every relative import in dist/ resolves inside the
//             tarball; no workspace: specifiers; engines.node >=22; both
//             drivers optional peers; no runtime dependencies; every
//             subpath the README imports is exported.
//   install   the other driver, drizzle-orm and any dependency of the
//             package are absent from node_modules (no optional-peer or
//             dependency leakage).
//   types     tsc (minimum and current TypeScript, NodeNext and Bundler
//             resolution, skipLibCheck off) over: every exported subpath;
//             the package's own consumer type fixture
//             (types.consumer/consumer-types.ts, positive and
//             @ts-expect-error cases) against the INSTALLED declarations;
//             and every TypeScript/JavaScript example in the installed
//             README — verbatim, with only the declarations the prose
//             assumes supplied per example (EXAMPLE_CONTEXT below). An
//             example not registered there fails the gate.
//   runtime   every subpath imports under this Node with the other driver
//             absent; asking for the absent driver is a MissingDriverError;
//             with NEUTRON_TEST_DATABASE_URL, driver auto-detection connects
//             and int8/numeric values round-trip exactly (checked with an
//             independent pg connection), in a throwaway database per
//             consumer.
//
// NEUTRON_LIVE_REQUIRED=1 turns a missing database URL into a failure.

import { execFileSync } from "node:child_process";
import { cpSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PKG = path.resolve(HERE, "..");
const REPO = path.resolve(PKG, "..", "..", "..");
const require_ = createRequire(path.join(PKG, "package.json"));
const pkgJson = JSON.parse(readFileSync(path.join(PKG, "package.json"), "utf8"));

const argv = process.argv.slice(2);
const opt = (name, fallback) => {
  const i = argv.indexOf(name);
  return i >= 0 ? argv[i + 1] : fallback;
};
const TS_VERSIONS = opt("--ts", "5.7.2,5.9.3").split(",");
const OUT = opt("--out", null);
const KEEP = opt("--keep-tarball", null);
const KEEP_WORK = argv.includes("--keep-work");
const TEST_URL = process.env.NEUTRON_TEST_DATABASE_URL || process.env.NEUTRON_SQL_TEST_URL || "";
const LIVE_REQUIRED = process.env.NEUTRON_LIVE_REQUIRED === "1";
if (!TEST_URL && LIVE_REQUIRED) {
  console.error("installed-gate: NEUTRON_LIVE_REQUIRED=1 but NEUTRON_TEST_DATABASE_URL is not set; failing instead of skipping");
  process.exit(1);
}

// The drivers and typings the package is developed and live-tested with.
const versionOf = (name) => JSON.parse(readFileSync(path.join(PKG, "node_modules", name, "package.json"), "utf8")).version;
const DRIVERS = {
  pg: { install: [`pg@${versionOf("pg")}`, `@types/pg@${versionOf("@types/pg")}`], other: "postgres" },
  postgres: { install: [`postgres@${versionOf("postgres")}`], other: "pg" },
};
const TYPES_NODE = `@types/node@${versionOf("@types/node")}`;
const isWin = process.platform === "win32";
const npm = isWin ? "npm.cmd" : "npm";
const pnpm = isWin ? "pnpm.cmd" : "pnpm";

const results = [];
function verdict(id, pass, detail = "") {
  results.push({ id, pass, detail });
  console.error(`${pass ? "PASS" : "FAIL"} ${id}${detail ? ` :: ${detail}` : ""}`);
}
function run(cmd, args, cwd, env = {}) {
  return execFileSync(cmd, args, { cwd, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, ...env }, shell: isWin });
}
function tryRun(cmd, args, cwd, env) {
  try {
    return { ok: true, out: run(cmd, args, cwd, env) };
  } catch (e) {
    return { ok: false, out: `${e.stdout ?? ""}${e.stderr ?? ""}` || String(e) };
  }
}

// README examples: the first line of each ```ts/```js block -> the file it
// is written to and the declarations its surrounding prose assumes. The
// block body is compiled verbatim after this preamble. Context tables live
// in fixture.ts, which imports the README's own schema.ts.
const EXAMPLE_CONTEXT = {
  "// schema.ts": { file: "schema.ts", preamble: "" },
  "// app.ts": { file: "app.ts", preamble: "" },
  "// export-schema.mjs": { file: "export-schema.mjs", preamble: "" },
  "await db.transaction(": { preamble: `import { db, posts } from "./fixture.js";` },
  "// any statement, both drivers:": {
    preamble: `import { db, users } from "./fixture.js";\ndeclare const id: number;\ndeclare const req: import("node:http").IncomingMessage;`,
  },
  'import { pgVector, l2Distance, cosineDistance, pgvectorExtension } from "@neutron-build/sql/pgvector";': {
    preamble: `import { pgTable, serial, text, index, asc } from "@neutron-build/sql";\nimport { db } from "./fixture.js";`,
  },
  'import { tsvector, toTsvector, websearchToTsquery, tsRank, matches } from "@neutron-build/sql/fts";': {
    preamble: `import { pgTable, serial, text, index, desc, sql } from "@neutron-build/sql";\nimport { db } from "./fixture.js";`,
  },
  'import { pgListener } from "@neutron-build/sql/listen-notify";': {
    preamble: `declare const DATABASE_URL: string;\ndeclare const controller: AbortController;`,
  },
  'import { timeBucket, tsBetween, timeSeries, hypertableSupport } from "@neutron-build/sql/timeseries";': {
    preamble: `import { pgTable, serial, text, timestamptz, double, cteTable, avg, over, lag, asc } from "@neutron-build/sql";\nimport { db } from "./fixture.js";`,
  },
  'import { inspectColumnarStorage } from "@neutron-build/sql/columnar";': { preamble: `import { db } from "./fixture.js";` },
  "// ignore unique violations: conflicted rows are simply not inserted": {
    preamble: `import { sql, excluded } from "@neutron-build/sql";\nimport { db, members, soft } from "./fixture.js";\ndeclare const rows: (typeof members.$inferInsert)[];\ndeclare const row: typeof members.$inferInsert & typeof soft.$inferInsert;`,
  },
  'import { keyset, ascNullsLast, descNullsFirst } from "@neutron-build/sql";': { preamble: `import { db, events } from "./fixture.js";` },
  'const stmt = db.driver.prepare!("select id from users where email = $1");': { preamble: `import { db } from "./fixture.js";` },
  'import { over, rowNumber, lag, desc, asc, lte } from "@neutron-build/sql";': {
    preamble: `import { cteTable } from "@neutron-build/sql";\nimport { db, posts } from "./fixture.js";`,
  },
  "const claimed = await db.transaction(async (tx) => {": { preamble: `import { eq } from "@neutron-build/sql";\nimport { db, jobs } from "./fixture.js";` },
  "for await (const row of db.select().from(events).stream({ batchSize: 100 })) {": {
    preamble: `import { db, events } from "./fixture.js";\ndeclare function done(row: unknown): boolean;\ndeclare function process(batch: unknown[]): Promise<void>;`,
  },
};

const FIXTURE = `import { createDatabase, pgTable, serial, integer, text, numeric, timestamptz } from "@neutron-build/sql";
import { users, posts, usersRelations, postsRelations } from "./schema.js";
export { users, posts };

export const members = pgTable("members", {
  id: serial("id").primaryKey(),
  email: text("email").notNull().unique(),
  hits: integer("hits").notNull().default(0),
  score: numeric("score"),
});
export const soft = pgTable("soft", {
  id: serial("id").primaryKey(),
  email: text("email").notNull(),
  deletedAt: timestamptz("deleted_at"),
});
export const events = pgTable("events", {
  id: serial("id").primaryKey(),
  occurredAt: timestamptz("occurred_at"),
  rank: integer("rank"),
});
export const jobs = pgTable("jobs", {
  id: serial("id").primaryKey(),
  state: text("state").notNull(),
});

export const db = await createDatabase({
  url: "postgres://localhost/unused",
  tables: { users, posts, members, soft, events, jobs },
  relations: { users: usersRelations, posts: postsRelations },
});
`;

// Every fenced block is classified by its info string's first word, case-
// insensitively: code languages are compiled, known prose/shell languages
// are skipped, and anything else is reported (a new spelling such as
// ```typescript must not slip past compilation).
const CODE_LANGS = new Set(["ts", "typescript", "tsx", "js", "javascript", "jsx", "mjs"]);
const OTHER_LANGS = new Set(["bash", "sh", "shell", "console", "sql", "json", "text", "diff"]);

// Blockquote structure: the number of leading ">" markers of a line, and
// the line with its first n markers (plus one optional space) removed.
const quoteDepth = (l) => /^(?:\s*>)*/.exec(l)[0].split(">").length - 1;
function unquote(l, n) {
  let s = l;
  for (let k = 0; k < n; k++) s = s.replace(/^\s*>/, "");
  return n > 0 ? s.replace(/^ /, "") : s;
}

function readmeExamples(readme) {
  const blocks = [];
  const unknown = [];
  const lines = readme.split("\n");
  const indentOf = (l) => /^\s*/.exec(l)[0].length;
  for (let i = 0; i < lines.length; i++) {
    // Outside a fence, blockquote markers are markdown structure: strip
    // them so a fence inside "> " is seen, and remember the depth.
    const depth = quoteDepth(lines[i]);
    const opener = unquote(lines[i], depth);
    // Backtick and tilde fences (CommonMark); the closing fence repeats
    // the opening character.
    const m = /^\s*(```|~~~)[`~]*\s*(\S*)/.exec(opener);
    if (!m) continue;
    const lang = m[2].toLowerCase();
    const close = new RegExp(`^\\s*${m[1][0] === "`" ? "```" : "~~~"}[${m[1][0]}]*\\s*$`);
    const indent = indentOf(opener);
    // Inside the fence only the opener's quote depth is structure; deeper
    // ">" belongs to the code and reaches compilation unchanged.
    const inner = (l) => unquote(l, depth);
    const start = i + 1;
    let end = start;
    // An unclosed fence ends with its container: a quoted fence when the
    // quote depth drops below the opener's, a list-item fence at a
    // non-blank line indented less than the fence.
    while (
      end < lines.length &&
      quoteDepth(lines[end]) >= depth &&
      !close.test(inner(lines[end])) &&
      !(indent > 0 && inner(lines[end]).trim() !== "" && indentOf(inner(lines[end])) < indent)
    )
      end++;
    const ended = end < lines.length && quoteDepth(lines[end]) >= depth && close.test(inner(lines[end]));
    if (CODE_LANGS.has(lang)) blocks.push({ lang, line: start + 1, body: lines.slice(start, end).map(inner).join("\n") });
    else if (!OTHER_LANGS.has(lang)) unknown.push(`line ${start}: ${m[1]}${m[2]}`);
    i = ended ? end : end - 1;
  }
  return { blocks, unknown };
}

const cleanups = [];
let admin = null;
try {
  // ---------------------------------------------------------------- pack
  const work = mkdtempSync(path.join(os.tmpdir(), "neutron-sql-installed-"));
  if (!KEEP_WORK) cleanups.push(() => rmSync(work, { recursive: true, force: true }));
  const rel = path.relative(REPO, work);
  verdict("setup: consumers live outside the monorepo", rel.startsWith("..") || path.isAbsolute(rel), work);
  // A clean build: tsc never deletes outputs of removed sources, and pnpm
  // pack ships whatever dist/ holds.
  rmSync(path.join(PKG, "dist"), { recursive: true, force: true });
  run(pnpm, ["run", "build"], PKG);
  const packDir = path.join(work, "pack");
  mkdirSync(packDir);
  run(pnpm, ["pack", "--pack-destination", packDir], PKG);
  const tgzName = readdirSync(packDir).find((f) => f.endsWith(".tgz"));
  const tarball = path.join(packDir, tgzName);
  if (KEEP) {
    mkdirSync(KEEP, { recursive: true });
    cpSync(tarball, path.join(KEEP, tgzName));
  }
  const unpacked = path.join(work, "unpacked");
  mkdirSync(unpacked);
  run("tar", ["-xzf", tarball, "-C", unpacked], work);
  const root = path.join(unpacked, "package");
  const walk = (dir, rel = "") =>
    readdirSync(dir, { withFileTypes: true }).flatMap((e) => (e.isDirectory() ? walk(path.join(dir, e.name), `${rel}${e.name}/`) : [`${rel}${e.name}`]));
  const entries = walk(root).sort();
  const outside = entries.filter((f) => !["package.json", "README.md", "LICENSE"].includes(f) && !f.startsWith("dist/"));
  const forbidden = entries.filter((f) => /\.test\.|live-harness|(^|\/)(bench|scripts|src|types\.consumer)\//.test(f));
  verdict("tarball: only package.json, README.md, LICENSE and dist/", outside.length === 0, outside.join(", ") || `${entries.length} files`);
  verdict("tarball: no tests, live harness, bench/, scripts/ or sources", forbidden.length === 0, forbidden.join(", "));
  const packed = JSON.parse(readFileSync(path.join(root, "package.json"), "utf8"));
  const exportTargets = Object.entries(packed.exports).flatMap(([sub, t]) => Object.values(t).map((f) => [sub, f.replace(/^\.\//, "")]));
  const missingTargets = exportTargets.filter(([, f]) => !entries.includes(f));
  verdict("tarball: every exports target is present", missingTargets.length === 0, missingTargets.map(([s, f]) => `${s} -> ${f}`).join(", ") || `${exportTargets.length} targets`);
  const unresolved = [];
  for (const f of entries.filter((e) => /\.(js|d\.ts)$/.test(e))) {
    const text = readFileSync(path.join(root, f), "utf8");
    for (const m of text.matchAll(/(?:^|[\s;])(?:import|export)\b[^'"]*?\bfrom\s*["'](\.[^"']+)["']|import\(\s*["'](\.[^"']+)["']\s*\)/gm)) {
      const spec = m[1] ?? m[2];
      const target = path.posix.normalize(path.posix.join(path.posix.dirname(f), spec));
      const candidates = f.endsWith(".d.ts") ? [target.replace(/\.js$/, ".d.ts"), target] : [target];
      if (!candidates.some((c) => entries.includes(c))) unresolved.push(`${f}: ${spec}`);
    }
  }
  verdict("tarball: every relative import in dist resolves inside the tarball", unresolved.length === 0, unresolved.slice(0, 10).join(", "));
  const manifestText = readFileSync(path.join(root, "package.json"), "utf8");
  verdict("package.json: no workspace: specifiers", !manifestText.includes("workspace:"));
  verdict("package.json: engines.node is >=22", packed.engines?.node === ">=22", String(packed.engines?.node));
  verdict(
    "package.json: pg and postgres are optional peers, nothing else is a dependency",
    packed.peerDependenciesMeta?.pg?.optional === true &&
      packed.peerDependenciesMeta?.postgres?.optional === true &&
      Object.keys(packed.peerDependencies ?? {}).sort().join(",") === "pg,postgres" &&
      Object.keys(packed.dependencies ?? {}).length === 0 &&
      Object.keys(packed.optionalDependencies ?? {}).length === 0,
    JSON.stringify({ peer: packed.peerDependencies, deps: packed.dependencies ?? {} }),
  );
  const readme = readFileSync(path.join(root, "README.md"), "utf8");
  const documented = [...new Set([...readme.matchAll(/["'`]@neutron-build\/sql(\/[a-z0-9-]+)?["'`]/g)].map((m) => `.${m[1] ?? ""}`))].sort();
  const notExported = documented.filter((s) => !(s in packed.exports));
  verdict("README: every documented import path is exported", notExported.length === 0, notExported.join(", ") || documented.join(" "));

  // ---------------------------------------------------------- consumers
  // Self-check of the classifier: every fence spelling reaches compilation
  // or is reported; nothing is skipped silently.
  {
    const probe = [
      "```typescript", "a", "```", "~~~ts", "b", "~~~", "```TS title=x.ts", "c", "```",
      "> ```ts", "> d", "> ```",
      "- item", "   ```bash", "   unclosed", "```ts", "e", "```",
      "```yaml", "f", "```", "```bash", "g", "```",
      // An unclosed fence in a quote ends with the quote (N9-R).
      "> ```bash", "> unclosed", "", "```ts", "h", "```",
      // ">" inside a fence is code, not structure (N9-S).
      "```ts", ">i", "```", "> ```ts", "> >j", "> ```",
    ].join("\n");
    const got = readmeExamples(probe);
    verdict(
      "README gate: the fence classifier sees backtick, tilde, titled, quoted and list-item fences",
      got.blocks.map((b) => b.body).join("") === "abcdeh>i>j" && got.unknown.length === 1,
      JSON.stringify(got),
    );
  }
  const { blocks: examples, unknown: unclassified } = readmeExamples(readme);
  verdict("README: every fenced block has a known language", unclassified.length === 0, unclassified.join(" | ") || "ok");
  const unregistered = examples.filter((b) => !(b.body.split("\n")[0] in EXAMPLE_CONTEXT));
  verdict(
    "README: every TypeScript/JavaScript example is registered for compilation",
    unregistered.length === 0,
    unregistered.map((b) => `line ${b.line}: ${b.body.split("\n")[0]}`).join(" | ") || `${examples.length} examples`,
  );
  const subpaths = Object.keys(packed.exports);
  if (TEST_URL) {
    admin = new (require_("pg").Client)({ connectionString: TEST_URL });
    await admin.connect();
  }

  for (const [driver, spec] of Object.entries(DRIVERS)) {
    const dir = path.join(work, `consumer-${driver}`);
    mkdirSync(dir);
    writeFileSync(path.join(dir, "package.json"), JSON.stringify({ name: `consumer-${driver}`, private: true, type: "module" }, null, 2));
    const current = TS_VERSIONS[TS_VERSIONS.length - 1];
    run(npm, ["install", "--no-audit", "--no-fund", tarball, ...spec.install, `typescript@${current}`, TYPES_NODE], dir);
    const nm = path.join(dir, "node_modules");
    const installedSql = JSON.parse(readFileSync(path.join(nm, "@neutron-build", "sql", "package.json"), "utf8"));
    verdict(`${driver}: installed package is the packed version`, installedSql.version === packed.version, installedSql.version);
    const leaked = [spec.other, "drizzle-orm", ...Object.keys(packed.dependencies ?? {})].filter((m) => existsSync(path.join(nm, m)));
    verdict(`${driver}: no optional-peer or dependency leakage`, leaked.length === 0, leaked.join(", ") || `${spec.other} and drizzle-orm absent`);

    // Types: examples, exports, the package's consumer fixture.
    for (const b of examples) {
      const ctx = EXAMPLE_CONTEXT[b.body.split("\n")[0]];
      if (!ctx) continue;
      const file = ctx.file ?? `readme-${b.line}.ts`;
      writeFileSync(path.join(dir, file), `${ctx.preamble ? `${ctx.preamble}\n` : ""}${b.body}\n${ctx.file ? "" : "export {};\n"}`);
    }
    writeFileSync(path.join(dir, "fixture.ts"), FIXTURE);
    writeFileSync(
      path.join(dir, "exports.ts"),
      subpaths.map((s, i) => `import * as m${i} from "@neutron-build/sql${s.slice(1)}";`).join("\n") +
        `\nexport const loaded: number[] = [${subpaths.map((_, i) => `Object.keys(m${i}).length`).join(", ")}];\n`,
    );
    cpSync(path.join(PKG, "types.consumer", "consumer-types.ts"), path.join(dir, "consumer-types.ts"));
    const base = { target: "ES2022", lib: ["ES2022"], strict: true, skipLibCheck: false, noEmit: true, types: ["node"], allowJs: true, checkJs: true };
    const configs = {
      nodenext: { ...base, module: "NodeNext", moduleResolution: "NodeNext" },
      bundler: { ...base, module: "ESNext", moduleResolution: "Bundler" },
    };
    for (const [name, compilerOptions] of Object.entries(configs)) {
      writeFileSync(path.join(dir, `tsconfig.${name}.json`), JSON.stringify({ compilerOptions, include: ["*.ts", "*.mjs"] }, null, 2));
    }
    for (const ts of TS_VERSIONS) {
      let tsc = path.join(nm, "typescript", "bin", "tsc");
      if (ts !== current) {
        const prefix = path.join(dir, `ts-${ts}`);
        run(npm, ["install", "--no-audit", "--no-fund", "--prefix", prefix, `typescript@${ts}`], dir);
        tsc = path.join(prefix, "node_modules", "typescript", "bin", "tsc");
      }
      for (const name of Object.keys(configs)) {
        const r = tryRun(process.execPath, [tsc, "-p", `tsconfig.${name}.json`, "--pretty", "false"], dir);
        verdict(`${driver}: README examples, every export and the consumer type fixture compile against the installed declarations (TypeScript ${ts}, ${name})`, r.ok, r.ok ? "" : r.out.trim().split("\n").slice(0, 15).join(" | "));
      }
    }

    // Runtime: every subpath, the absent driver, a live round trip.
    const probe = `
import { createDatabase, isMissingDriverError, pgTable, bigint, numeric, text, eq } from "@neutron-build/sql";
const out = { node: process.versions.node, subpaths: {} };
for (const s of ${JSON.stringify(subpaths)}) {
  try { out.subpaths[s] = Object.keys(await import("@neutron-build/sql" + s.slice(1))).length; } catch (e) { out.subpaths[s] = "error: " + (e.code ?? e.message); }
}
const t = pgTable("installed_probe", { id: bigint("id", { mode: "bigint" }).primaryKey(), amount: numeric("amount").notNull(), label: text("label") });
try {
  const other = await createDatabase({ url: process.env.PROBE_URL || "postgres://localhost/unused", tables: { t }, driverOptions: { driver: ${JSON.stringify(spec.other)} } });
  await other.select().from(t);
  out.absentDriver = "no error";
} catch (e) { out.absentDriver = isMissingDriverError(e) ? "MissingDriverError" : "other: " + e.message; }
if (process.env.PROBE_URL) {
  const db = await createDatabase({ url: process.env.PROBE_URL, tables: { t } });
  await db.driver.execute("create table installed_probe (id bigint primary key, amount numeric not null, label text)");
  const id = 9007199254740993n, amount = "123456789012345678901234567890.123456789";
  await db.insert(t).values({ id, amount, label: null });
  const [row] = await db.select().from(t).where(eq(t.id, id));
  out.row = { id: typeof row.id === "bigint" ? row.id.toString() + "n" : String(row.id), amount: row.amount, label: row.label };
  await db.close();
}
console.log(JSON.stringify(out));
`;
    writeFileSync(path.join(dir, "probe.mjs"), probe);
    let dbName = null;
    let probeUrl = "";
    if (admin) {
      dbName = `neutron_sql_installed_${driver}_${process.pid}_${Date.now()}`;
      await admin.query(`create database "${dbName}"`);
      const u = new URL(TEST_URL);
      u.pathname = `/${dbName}`;
      probeUrl = u.toString();
    }
    try {
      const r = tryRun(process.execPath, ["probe.mjs"], dir, { PROBE_URL: probeUrl });
      const got = r.ok ? JSON.parse(r.out.trim().split("\n").pop()) : null;
      const failedSubpaths = got ? Object.entries(got.subpaths).filter(([, v]) => typeof v !== "number" || v === 0) : [];
      verdict(`${driver}: every subpath imports at runtime (Node ${process.versions.node})`, r.ok && failedSubpaths.length === 0, r.ok ? failedSubpaths.map(([s, v]) => `${s}: ${v}`).join(", ") || JSON.stringify(got.subpaths) : r.out.slice(0, 800));
      verdict(`${driver}: requesting the absent ${spec.other} driver is a MissingDriverError`, got?.absentDriver === "MissingDriverError", String(got?.absentDriver));
      if (admin && got) {
        const direct = new (require_("pg").Client)({ connectionString: probeUrl });
        await direct.connect();
        const o = (await direct.query("select id::text as id, amount::text as amount, label is null as label_null from installed_probe")).rows;
        await direct.end();
        verdict(
          `${driver}: auto-detected driver connects; int8 and numeric round-trip exactly`,
          o.length === 1 && o[0].id === "9007199254740993" && o[0].amount === "123456789012345678901234567890.123456789" && o[0].label_null === true &&
            got.row?.id === "9007199254740993n" && got.row?.amount === o[0].amount && got.row?.label === null,
          `db ${JSON.stringify(o)} app ${JSON.stringify(got.row)}`,
        );
      }
    } finally {
      if (admin && dbName) await admin.query(`drop database if exists "${dbName}" with (force)`);
    }
  }
  if (!TEST_URL) console.error("installed-gate: NEUTRON_TEST_DATABASE_URL not set — live round trips skipped (set NEUTRON_LIVE_REQUIRED=1 to fail instead)");
} catch (e) {
  verdict("gate ran to completion", false, e.stack ?? String(e));
} finally {
  if (admin) await admin.end().catch(() => {});
  for (const c of cleanups.reverse()) {
    try {
      c();
    } catch {}
  }
}

const failed = results.filter((r) => !r.pass);
const summary = {
  gate: "neutron-sql installed artifact",
  package: `${pkgJson.name}@${pkgJson.version}`,
  node: process.versions.node,
  platform: `${process.platform}-${process.arch}`,
  typescript: TS_VERSIONS,
  drivers: Object.fromEntries(Object.entries(DRIVERS).map(([k, v]) => [k, v.install])),
  live: Boolean(TEST_URL),
  passed: results.length - failed.length,
  failed: failed.length,
  results,
};
if (OUT) writeFileSync(OUT, `${JSON.stringify(summary, null, 2)}\n`);
console.error(`installed-gate: ${summary.passed} passed, ${summary.failed} failed`);
process.exit(failed.length ? 1 : 0);
