#!/usr/bin/env node
// Fresh-machine onboarding and legacy upgrade from the documentation
// (orm-program R02, VERIFICATION V17).
//
//   node scripts/onboarding.mjs --cli <neutron binary> --tarball <@neutron-build/sql .tgz> [--out file.json]
//
// Everything runs in temporary projects outside the monorepo, from the
// installed package tarball and a CLI binary, with the documentation as the
// script: the code and commands come verbatim from the INSTALLED README
// (node_modules/@neutron-build/sql/README.md) and the CLI reference
// (typescript/apps/site/src/content/docs/cli/commands.mdx). Each command
// that runs is printed with its document and line. Database state is
// checked with psql, never through the tools under test.
//
//   fresh   npm install the tarball + postgres.js; write schema.ts, app.ts
//           and export-schema.mjs from the README; compile with tsc; run
//           the README's migration commands (export, migrate generate,
//           migrate, migrate status) and inspect the generated SQL and the
//           history row; run app.ts (queries, relational read, printed
//           toSQL); open the CLI's Studio in headless Chrome as a new user
//           — add the connection through the form, browse the table, edit a
//           cell, commit; a second migrate generate plans nothing.
//   legacy  a database migrated by an older CLI (pre-v2 _neutron_migrations,
//           no checksums) with an existing project: `neutron migrate`
//           refuses until `neutron migrate adopt` (from commands.mdx)
//           adopts the history as UNVERIFIED; the README workflow then adds
//           the new schema as migration 002 without touching existing data.
//
// Needs NEUTRON_TEST_DATABASE_URL (a disposable server; each scenario owns
// its database), psql, npm, network access to the npm registry, and Chrome
// (or CHROME_PATH).

import { execFileSync, spawnSync } from "node:child_process";
import { mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync, mkdirSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { launchChrome, sleep, startStudio } from "./cli-studio.mjs";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(HERE, "..", "..");
const COMMANDS_DOC = path.join(REPO, "typescript", "apps", "site", "src", "content", "docs", "cli", "commands.mdx");

const argv = process.argv.slice(2);
const opt = (name, fallback) => {
  const i = argv.indexOf(name);
  return i >= 0 ? argv[i + 1] : fallback;
};
const CLI = opt("--cli", null) && path.resolve(opt("--cli"));
const TARBALL = opt("--tarball", null) && path.resolve(opt("--tarball"));
const OUT = opt("--out", null);
const TEST_URL = process.env.NEUTRON_TEST_DATABASE_URL || "";
if (!CLI || !TARBALL || !TEST_URL) {
  console.error("onboarding: --cli <neutron binary>, --tarball <package .tgz> and NEUTRON_TEST_DATABASE_URL are required");
  process.exit(2);
}
const isWin = process.platform === "win32";
const npm = isWin ? "npm.cmd" : "npm";

const results = [];
function verdict(id, pass, detail = "") {
  results.push({ id, pass, detail });
  console.error(`${pass ? "PASS" : "FAIL"} ${id}${detail ? ` :: ${detail}` : ""}`);
}
const dbUrl = (db) => {
  const u = new URL(TEST_URL);
  u.pathname = `/${db}`;
  return u.toString();
};
const psql = (db, sqlText) => execFileSync("psql", [dbUrl(db), "-v", "ON_ERROR_STOP=1", "-qAtc", sqlText], { encoding: "utf8" }).trim();

// ---------------------------------------------------------------- docs
function fencedBlocks(file) {
  const lines = readFileSync(file, "utf8").split("\n");
  const blocks = [];
  for (let i = 0; i < lines.length; i++) {
    const m = /^\s*```(\S*)/.exec(lines[i]);
    if (!m) continue;
    let end = i + 1;
    while (end < lines.length && !/^\s*```\s*$/.test(lines[end])) end++;
    // The info string's first word, case-insensitive; every shell spelling
    // counts as bash so a ```sh or ```shell block is not silently skipped.
    const word = m[1].toLowerCase();
    const lang = ["sh", "shell", "console", "zsh"].includes(word) ? "bash" : word;
    blocks.push({ lang, file, lines: lines.slice(i + 1, end).map((text, k) => ({ text, line: i + 2 + k })) });
    i = end;
  }
  return blocks;
}
function codeBlock(file, firstLine) {
  const b = fencedBlocks(file).find((x) => x.lines[0]?.text === firstLine);
  if (!b) throw new Error(`${path.basename(file)}: no code block starting with ${JSON.stringify(firstLine)}`);
  return `${b.lines.map((l) => l.text).join("\n")}\n`;
}
// Shell lines of the bash block that contains `anchor`, comments removed.
function shellLines(file, anchor) {
  const b = fencedBlocks(file).find((x) => x.lang === "bash" && x.lines.some((l) => l.text.trim().startsWith(anchor)));
  if (!b) throw new Error(`${path.basename(file)}: no bash block with ${JSON.stringify(anchor)}`);
  return b.lines.map((l) => ({ ...l, argv: words(l.text) })).filter((l) => l.argv.length > 0);
}
// Minimal POSIX word splitting: whitespace, single and double quotes, and a
// trailing # comment.
function words(line) {
  const out = [];
  let cur = null;
  let quote = null;
  for (const ch of line) {
    if (quote) {
      if (ch === quote) quote = null;
      else cur += ch;
    } else if (ch === "'" || ch === '"') {
      quote = ch;
      cur ??= "";
    } else if (/\s/.test(ch)) {
      if (cur !== null) out.push(cur);
      cur = null;
    } else if (ch === "#" && cur === null) {
      break;
    } else {
      cur = (cur ?? "") + ch;
    }
  }
  if (cur !== null) out.push(cur);
  return out;
}
// The documented command line that is exactly `command`, or with `prefix`
// set, the first one that starts with it.
function findLine(lines, command, doc, { prefix = false } = {}) {
  const l = lines.find((x) => (prefix ? x.argv.join(" ").startsWith(command) : x.argv.join(" ") === command));
  if (!l) throw new Error(`${doc}: no documented command ${prefix ? "starting with " : ""}${JSON.stringify(command)}`);
  return l;
}

// Runs a documented command line in the project; `neutron` is the CLI under
// test and `node` this Node.
function runDoc(project, docLine, doc, env) {
  const [cmd, ...args] = docLine.argv;
  const bin = cmd === "neutron" ? CLI : cmd === "node" ? process.execPath : cmd;
  console.error(`  $ ${docLine.argv.join(" ")}    (${doc}:${docLine.line})`);
  const r = spawnSync(bin, args, { cwd: project, env: { ...process.env, ...env }, encoding: "utf8" });
  return { code: r.status, out: `${r.stdout ?? ""}${r.stderr ?? ""}` };
}

function newProject(label) {
  const dir = mkdtempSync(path.join(os.tmpdir(), `neutron-onboarding-${label}-`));
  const rel = path.relative(REPO, dir);
  if (!(rel.startsWith("..") || path.isAbsolute(rel))) throw new Error(`${dir} is inside the monorepo`);
  const peer = JSON.parse(execFileSync("tar", ["-xzOf", TARBALL, "package/package.json"], { encoding: "utf8" })).peerDependencies.postgres;
  writeFileSync(path.join(dir, "package.json"), JSON.stringify({ name: `onboarding-${label}`, private: true, type: "module" }, null, 2));
  execFileSync(npm, ["install", "--no-audit", "--no-fund", TARBALL, `postgres@${peer}`, "typescript@5.9.3", "@types/node@22"], { cwd: dir, stdio: "pipe", shell: isWin });
  writeFileSync(
    path.join(dir, "tsconfig.json"),
    JSON.stringify({ compilerOptions: { target: "ES2022", module: "NodeNext", moduleResolution: "NodeNext", strict: true, skipLibCheck: false, types: ["node"] }, include: ["*.ts"] }, null, 2),
  );
  return dir;
}
function writeReadmeCode(project) {
  const readme = path.join(project, "node_modules", "@neutron-build", "sql", "README.md");
  for (const [file, first] of [["schema.ts", "// schema.ts"], ["app.ts", "// app.ts"], ["export-schema.mjs", "// export-schema.mjs"]]) {
    writeFileSync(path.join(project, file), codeBlock(readme, first));
  }
  const tsc = spawnSync(process.execPath, [path.join(project, "node_modules", "typescript", "bin", "tsc"), "-p", "."], { cwd: project, encoding: "utf8" });
  return { readme, tsc };
}
function createDb(name) {
  psql("postgres", `create database "${name}"`);
  return () => psql("postgres", `drop database if exists "${name}" with (force)`);
}

const cleanups = [];
const stamp = `${process.pid}_${Math.floor(Date.now() / 1000)}`;
try {
  // ============================================================== fresh
  {
    const DB = `onboarding_fresh_${stamp}`;
    cleanups.push(createDb(DB));
    const project = newProject("fresh");
    cleanups.push(() => rmSync(project, { recursive: true, force: true }));
    const env = { DATABASE_URL: dbUrl(DB) };
    const { readme, tsc } = writeReadmeCode(project);
    const README = "README.md";
    verdict("fresh.compile: README schema.ts and app.ts compile against the installed package", tsc.status === 0, `${tsc.stdout}${tsc.stderr}`.trim().slice(0, 600));

    const doc = shellLines(readme, "node export-schema.mjs");
    const exp = runDoc(project, findLine(doc, "node export-schema.mjs", README), README, env);
    const schemaDoc = (() => {
      try {
        return JSON.parse(readFileSync(path.join(project, "neutron.schema.json"), "utf8"));
      } catch {
        return null;
      }
    })();
    verdict("fresh.export: export-schema.mjs writes a schema document v2", exp.code === 0 && schemaDoc?.version === 2, exp.out.trim().slice(0, 300) || `tables ${schemaDoc?.tables?.map((t) => t.identity?.name).join(",")}`);

    const gen = runDoc(project, findLine(doc, "neutron migrate generate --schema", README, { prefix: true }), README, env);
    const migDir = path.join(project, "migrations");
    const upFiles = (() => {
      try {
        return readdirSync(migDir).filter((f) => f.endsWith(".up.sql"));
      } catch {
        return [];
      }
    })();
    const up = upFiles.length === 1 ? readFileSync(path.join(migDir, upFiles[0]), "utf8") : "";
    verdict(
      "fresh.generate: one reviewable migration creating users and posts with the FK",
      gen.code === 0 && upFiles[0] === "001_add_users.up.sql" && /create table "public"\."users"/.test(up) && /create table "public"\."posts"/.test(up) && /references "public"\."users" \("id"\) on delete cascade/.test(up) && !/\bdrop\b/i.test(up),
      `${upFiles.join(",")} ${gen.code === 0 ? "" : gen.out.slice(0, 300)}`,
    );

    const apply = runDoc(project, findLine(doc, "neutron migrate", README), README, env);
    const status = runDoc(project, findLine(doc, "neutron migrate status", README), README, env);
    const hist = psql(DB, `select version || '|' || name || '|' || (checksum is not null) from _neutron_migrations order by version`);
    const cols = psql(DB, `select string_agg(table_name || '.' || column_name, ',' order by table_name, ordinal_position) from information_schema.columns where table_schema = 'public' and table_name in ('users','posts')`);
    verdict(
      "fresh.apply: migrate applies 001 once, with a checksum; status lists it",
      apply.code === 0 && status.code === 0 && hist === "001|add_users|true" && /001/.test(status.out) && /applied/.test(status.out) &&
        cols === "posts.id,posts.user_id,posts.title,users.id,users.email,users.created_at",
      `history ${hist}; columns ${cols}`,
    );

    console.error(`  $ node app.js    (README.md: // app.ts, compiled)`);
    const app = spawnSync(process.execPath, ["app.js"], { cwd: project, env: { ...process.env, ...env }, encoding: "utf8" });
    const users = psql(DB, `select string_agg(id || ':' || email, ',' order by id) from users`);
    const posts = psql(DB, `select string_agg(p.user_id || ':' || p.title, ',') from posts p join users u on u.id = p.user_id`);
    verdict("fresh.query: app.ts runs — insert, update, select, relational read, transaction", app.status === 0 && users === "1:b@x.com" && posts === "1:hello", `exit ${app.status}; users ${users}; posts ${posts}; ${app.status === 0 ? "" : app.stderr.slice(0, 400)}`);
    verdict(
      "fresh.inspect: toSQL prints the statement without executing it",
      /select "users"\."id", "users"\."email", .*"createdAt".* from "users"/.test(app.stdout),
      (app.stdout.match(/sql: '[^']*'/) ?? [""])[0].slice(0, 200),
    );

    // Studio as a new user: empty home, connection added through the form.
    const studio = await startStudio(CLI);
    cleanups.push(studio.stop);
    const browser = await launchChrome();
    cleanups.push(() => browser.close());
    const page = await (await browser.newContext({ viewport: { width: 1280, height: 800 } })).newPage();
    const pageErrors = [];
    page.on("pageerror", (e) => pageErrors.push(String(e)));
    await page.goto(`${studio.base}/`);
    await page.getByRole("button", { name: "+ Add" }).click();
    await page.getByLabel("Name").fill("onboarding");
    await page.getByLabel("Connection URL").fill(dbUrl(DB));
    await page.getByRole("button", { name: "Test Connection" }).click();
    const tested = await page.getByText(/^Connected — PostgreSQL/).waitFor({ timeout: 15000 }).then(() => true, () => false);
    await page.getByRole("button", { name: "Save" }).click();
    await page.getByRole("button", { name: "Connect" }).click();
    await page.getByTitle("Browse users", { exact: true }).click();
    await page.waitForFunction(() => document.querySelectorAll('table[role="grid"] tbody tr[data-row-index]').length === 1, null, { timeout: 30000 });
    const emailCell = await page.evaluateHandle(() => {
      const grid = document.querySelector('table[role="grid"]');
      const heads = [...grid.querySelectorAll("thead th")].map((th) => th.textContent.trim().split(/\s+/).pop());
      return grid.querySelector("tbody tr[data-row-index]").children[heads.indexOf("email")];
    });
    await emailCell.asElement().dblclick();
    const input = page.locator('input[aria-label$=" value"]').first();
    await input.fill("c@x.com");
    await input.press("Enter");
    await page.getByRole("button", { name: /^Commit 1 change$/ }).click();
    let email = "";
    for (let i = 0; i < 50 && email !== "c@x.com"; i++) {
      email = psql(DB, `select email from users where id = 1`);
      if (email !== "c@x.com") await sleep(100);
    }
    verdict("fresh.edit: a new user connects Studio through the form and commits a cell edit", tested && email === "c@x.com" && pageErrors.length === 0, `tested=${tested}; db email=${email}; pageErrors=${pageErrors.slice(0, 2).join(" | ") || "none"}`);

    const again = runDoc(project, { ...findLine(doc, "neutron migrate generate --schema", README, { prefix: true }), argv: ["neutron", "migrate", "generate", "--schema", "neutron.schema.json", "--name", "again"] }, `${README} (same command, --name again)`, env);
    const afterFiles = readdirSync(migDir).filter((f) => f.endsWith(".sql")).sort();
    verdict("fresh.inspect: after the edit, the schema is in sync — a second generate plans nothing", again.code === 0 && afterFiles.join(",") === "001_add_users.down.sql,001_add_users.up.sql" && /No schema changes detected/.test(again.out) && !/not in sync/.test(again.out), afterFiles.join(","));
  }

  // ============================================================= legacy
  {
    const DB = `onboarding_legacy_${stamp}`;
    cleanups.push(createDb(DB));
    // The database an older CLI left behind: its history shape (text
    // version, no checksum/owner/format) and the effects of 001.
    psql(
      DB,
      `create table _neutron_migrations (version text primary key, name text not null, applied_at timestamptz default now());
       create table users (id serial primary key, email text not null unique);
       insert into users (email) values ('kept@x.com');
       insert into _neutron_migrations (version, name) values ('001', 'init');`,
    );
    const project = newProject("legacy");
    cleanups.push(() => rmSync(project, { recursive: true, force: true }));
    mkdirSync(path.join(project, "migrations"));
    writeFileSync(path.join(project, "migrations", "001_init.up.sql"), "create table users (id serial primary key, email text not null unique);\n");
    writeFileSync(path.join(project, "migrations", "001_init.down.sql"), "drop table users;\n");
    const env = { DATABASE_URL: dbUrl(DB) };
    const CMDS = "commands.mdx";
    const cliDoc = fencedBlocks(COMMANDS_DOC)
      .filter((b) => b.lang === "bash")
      .flatMap((b) => b.lines.map((l) => ({ ...l, argv: words(l.text) })))
      .filter((l) => l.argv.length > 0);

    const refused = runDoc(project, findLine(cliDoc, "neutron migrate", CMDS), CMDS, env);
    verdict("legacy.refuse: neutron migrate refuses the legacy history and names adopt", refused.code !== 0 && /adopt/.test(refused.out), refused.out.trim().split("\n").slice(-2).join(" | ").slice(0, 300));
    const adopt = runDoc(project, findLine(cliDoc, "neutron migrate adopt", CMDS), CMDS, env);
    const adopted = psql(DB, `select version || '|' || name || '|' || (checksum is null) from _neutron_migrations order by version`);
    verdict("legacy.adopt: adopt reports 001 UNVERIFIED and keeps it without a fabricated checksum", adopt.code === 0 && /UNVERIFIED/.test(adopt.out) && adopted === "001|init|true", `history ${adopted}`);

    const { readme, tsc } = writeReadmeCode(project);
    const README = "README.md";
    verdict("legacy.compile: README schema.ts and app.ts compile", tsc.status === 0, `${tsc.stdout}${tsc.stderr}`.trim().slice(0, 400));
    const doc = shellLines(readme, "node export-schema.mjs");
    runDoc(project, findLine(doc, "node export-schema.mjs", README), README, env);
    const gen = runDoc(project, findLine(doc, "neutron migrate generate --schema", README, { prefix: true }), README, env);
    const newFiles = readdirSync(path.join(project, "migrations")).filter((f) => f.endsWith(".up.sql") && !f.startsWith("001_")).sort();
    const up = newFiles.length === 1 ? readFileSync(path.join(project, "migrations", newFiles[0]), "utf8") : "";
    verdict(
      "legacy.generate: the new schema becomes 002 — add created_at and posts, drop nothing",
      gen.code === 0 && newFiles[0] === "002_add_users.up.sql" && /alter table "public"\."users" add column "created_at"/.test(up) && /create table "public"\."posts"/.test(up) && !/create table "public"\."users"/.test(up) && !/\bdrop\b/i.test(up),
      `${newFiles.join(",")} ${gen.code === 0 ? "" : gen.out.slice(0, 300)}`,
    );
    const apply = runDoc(project, findLine(doc, "neutron migrate", README), README, env);
    const hist = psql(DB, `select string_agg(version || ':' || (checksum is not null), ',' order by version) from _neutron_migrations`);
    const kept = psql(DB, `select string_agg(email || ':' || (created_at is not null), ',') from users`);
    verdict("legacy.apply: only 002 runs; the adopted row and the existing data survive", apply.code === 0 && hist === "001:false,002:true" && kept === "kept@x.com:true", `history ${hist}; users ${kept}`);
    const app = spawnSync(process.execPath, ["app.js"], { cwd: project, env: { ...process.env, ...env }, encoding: "utf8" });
    const users = psql(DB, `select string_agg(email, ',' order by id) from users`);
    verdict("legacy.query: app.ts runs against the upgraded database", app.status === 0 && users === "kept@x.com,b@x.com", `exit ${app.status}; users ${users}`);
  }
} catch (err) {
  verdict("harness", false, err instanceof Error ? err.stack : String(err));
} finally {
  for (const c of cleanups.reverse()) {
    try {
      await c();
    } catch (err) {
      verdict("cleanup", false, String(err));
    }
  }
}
const failed = results.filter((r) => !r.pass).length;
if (OUT) writeFileSync(OUT, JSON.stringify({ tool: "onboarding", platform: `${process.platform}-${process.arch}`, cli: CLI, tarball: path.basename(TARBALL), results }, null, 2) + "\n");
console.error(`onboarding: ${results.length - failed}/${results.length} passed`);
process.exit(failed === 0 ? 0 : 1);
