// Renders every case in libraries.mjs under preact/compat and reports what
// happened, per library.
//
// A-007: `docs/react-compat.md` answered the highest-stakes adoption question
// — "will my React libraries work?" — with "usually works, verify per package
// in app context". That moves all the risk onto the evaluator, at evaluation
// time, on their own codebase. The existing `ci:runtime-compat` lane proves the
// FRAMEWORK runs in both modes; it says nothing about the ecosystem, which is
// what is actually being asked.
//
// This renders the real packages. `react` and `react-dom` resolve to
// preact/compat through the file: shims in ./shims, which is the same
// substitution the Vite build makes for `runtime: "react-compat"`.
//
// Usage:
//   node run.mjs           # run, print, exit non-zero if a PASS regressed
//   node run.mjs --write   # also rewrite ../docs/react-compat-matrix.md
import { readFileSync, writeFileSync, existsSync, realpathSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import process from "node:process";

import { renderToString } from "preact-render-to-string";

import { CASES } from "./libraries.mjs";
import { registerBundlerParityHooks } from "./node-hooks.mjs";

// Before anything loads: the hooks make raw Node resolve packages the way the
// Vite build does (see node-hooks.mjs for why each rule exists).
registerBundlerParityHooks();

const HERE = dirname(fileURLToPath(import.meta.url));

// Assert the substitution before measuring anything.
//
// The libraries reach preact through the file: shims; this harness renders with
// its own preact. If those are two different copies, every hooks-using library
// fails with "Cannot read properties of undefined (reading '__H')" and the
// matrix reports a wall of ecosystem incompatibility that is entirely an
// artifact of the harness. That is not hypothetical: the first published matrix
// said 12/13 because this machine happened to have a matching preact hoisted,
// while a clean install resolved the shims to a different version and scored
// 1/13. A version number in a report is worth less than the identity check, so
// do the identity check.
function assertOnePreact() {
  const require = createRequire(import.meta.url);
  const versionAt = (p) => JSON.parse(readFileSync(join(p, "package.json"), "utf-8")).version;

  // Resolve the way each side actually does at runtime: the shims resolve from
  // their real location in the virtual store, not from ./shims.
  const shimEntry = realpathSync(require.resolve("react"));
  const shimPreact = realpathSync(createRequire(shimEntry).resolve("preact/package.json"));
  const oursPreact = realpathSync(require.resolve("preact/package.json"));

  if (shimPreact !== oursPreact) {
    console.error("The compat shims and this harness are using different copies of preact.");
    console.error(`  harness : ${versionAt(dirname(oursPreact))}  ${dirname(oursPreact)}`);
    console.error(`  shims   : ${versionAt(dirname(shimPreact))}  ${dirname(shimPreact)}`);
    console.error("");
    console.error("Every hooks-based library will fail with `__H` of undefined, and none of");
    console.error("those failures says anything about preact/compat. Both package.json files");
    console.error("under compat-matrix/shims pin preact exactly for this reason -- check that");
    console.error("the pin still matches compat-matrix/package.json, then reinstall.");
    process.exit(2);
  }

  // Same package is necessary but not sufficient: Node loads a package's CJS
  // and ESM builds as two instances, and any dependency that reaches react
  // through CJS (use-sync-external-store shims, @tiptap/react and @xyflow/react
  // both do) would call hooks on a preact whose hooks state the renderer never
  // initializes. node-hooks.mjs forces every preact resolution onto the ESM
  // dist, so require() and import() must land on the same instance. If they
  // do not, every library with a CJS hop in its tree reports `__H` failures
  // that are harness artifacts.
  const byRequire = require("preact/compat");
  return import("preact/compat").then((byImport) => {
    if (byRequire.useState !== byImport.useState) {
      console.error("preact/compat loaded as two module instances (CJS dist and ESM dist).");
      console.error("The CJS copy's hooks never see a current component, so any library that");
      console.error("reaches react through CJS fails with `__H` of undefined. The resolve hook");
      console.error("in node-hooks.mjs forces preact onto the ESM dist for exactly this reason;");
      console.error("check that it is registered before the first library import.");
      process.exit(2);
    }
    return versionAt(dirname(oursPreact));
  });
}

const PREACT_VERSION = await assertOnePreact();
const DOC_PATH = join(HERE, "..", "docs", "react-compat-matrix.md");
const BASELINE_PATH = join(HERE, "baseline.json");
const WRITE = process.argv.includes("--write");

function versionOf(pkg) {
  // The version actually installed and rendered, not the declared range.
  // Resolving `<pkg>/package.json` only works for packages that export that
  // subpath — none of the Radix packages do — and the declared-range fallback
  // below prints the specifier with its prefix stripped. The first published
  // matrix combined the two into a quiet lie: every Radix row claimed
  // "1.1.4"-era numbers while the lockfile had 1.1.23+ installed and under
  // test. Resolve the package entry and walk up to the manifest that owns it
  // (skipping marker package.json files with no version, e.g. the ESM marker
  // inside qrcode.react).
  const declaredRange = () => {
    try {
      const own = JSON.parse(readFileSync(join(HERE, "package.json"), "utf-8"));
      return (own.dependencies?.[pkg] ?? "").replace(/^[\^~]/, "") || "unknown";
    } catch {
      return "unknown";
    }
  };
  try {
    let dir = dirname(fileURLToPath(import.meta.resolve(pkg)));
    for (;;) {
      const manifest = join(dir, "package.json");
      if (existsSync(manifest)) {
        const version = JSON.parse(readFileSync(manifest, "utf-8")).version;
        if (typeof version === "string") return version;
      }
      const parent = dirname(dir);
      if (parent === dir) break;
      dir = parent;
    }
  } catch {
    // Not every package resolves an entry from here (@tiptap/pm exports only
    // subpaths).
  }
  return declaredRange();
}

async function runCase(testCase) {
  const started = Date.now();
  try {
    const element = await testCase.load();
    const html = renderToString(element);
    const ms = Date.now() - started;

    if (typeof html !== "string" || html.length === 0) {
      // The most common way a compat problem hides: no throw, no output.
      return { status: "FAIL", detail: "rendered empty output", ms };
    }
    if (!testCase.expect(html)) {
      return {
        status: "FAIL",
        detail: `rendered, but the expected content was absent (${html.slice(0, 120).replace(/\s+/g, " ")}…)`,
        ms,
      };
    }
    return { status: "PASS", detail: `${html.length} bytes of markup`, ms };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return { status: "FAIL", detail: message.split("\n")[0].slice(0, 200), ms: Date.now() - started };
  }
}

const results = [];
for (const testCase of CASES) {
  const result = await runCase(testCase);
  results.push({ ...testCase, ...result, version: versionOf(testCase.label) });
  const mark = result.status === "PASS" ? "PASS" : "FAIL";
  console.log(`${mark.padEnd(5)} ${testCase.label.padEnd(32)} ${result.detail}`);
}

const passed = results.filter((r) => r.status === "PASS");
const failed = results.filter((r) => r.status !== "PASS");
console.log(`\n${passed.length}/${results.length} render under preact/compat.`);

// Regression gate: a library that passed before must not start failing. New
// failures are recorded, not fatal — the matrix is meant to state the truth,
// including where the truth is "no".
let exitCode = 0;
if (existsSync(BASELINE_PATH)) {
  const baseline = JSON.parse(readFileSync(BASELINE_PATH, "utf-8"));
  const regressed = results.filter((r) => r.status !== "PASS" && baseline[r.id] === "PASS");
  if (regressed.length > 0) {
    console.error(`\nREGRESSION: ${regressed.map((r) => r.label).join(", ")} passed before and now fail.`);
    exitCode = 1;
  }
}

if (WRITE) {
  const baseline = Object.fromEntries(results.map((r) => [r.id, r.status]));
  writeFileSync(BASELINE_PATH, `${JSON.stringify(baseline, null, 2)}\n`);

  const rows = results
    .map(
      (r) =>
        `| \`${r.label}\` | ${r.version} | ${r.status === "PASS" ? "yes" : "**no**"} | ${
          r.status === "PASS" ? r.why : `${r.why}<br>**Fails:** ${r.detail.replace(/\|/g, "\\|")}`
        } |`
    )
    .join("\n");

  writeFileSync(
    DOC_PATH,
    `# React library compatibility under Neutron

Neutron renders with Preact. \`runtime: "react-compat"\` aliases \`react\`,
\`react-dom\` and the JSX runtimes to \`preact/compat\`, so most React libraries
work unchanged — but "most" is not an answer anyone can plan against, and the
previous answer to this question was "usually works, verify per package in app
context", which hands the risk back to whoever is evaluating.

So this page is generated by actually rendering each library. It is produced by
\`typescript/compat-matrix\` (\`pnpm --filter @neutron/compat-matrix matrix\`),
which mounts the real package under \`preact/compat\` and server-renders it. A
row says yes only if the component produced markup containing what it should;
rendering empty output counts as a failure, because that is how a compat
problem usually hides.

**${passed.length} of ${results.length} render.** Generated from the versions in
the table; a different major version can behave differently, which is the reason
the version column exists.

| Library | Version | Renders | Notes |
|---|---|---|---|
${rows}

## What this does and does not tell you

It covers **server rendering**, which is where most compat failures surface
(hooks reading React internals, \`findDOMNode\`, portals, context identity
across the boundary, \`forwardRef\` shapes). It does not cover interaction:
a library can render and still misbehave on click, focus, or animation frames.
Treat a yes as "safe to adopt, still test your own flows", not as certification.

Loading follows the Vite build's semantics, not raw Node's: one preact instance
for CJS and ESM consumers, require-style path fallbacks, the \`module\` field
for dependency-internal imports (\`compat-matrix/node-hooks.mjs\` implements and
explains each). Those are bundler mechanics, not library compatibility — a
library that still fails here fails for its own reasons, and the row says so.

It also covers only the libraries listed. Adding one is a single entry in
\`compat-matrix/libraries.mjs\` — a mount and an assertion — and a
\`pnpm --filter @neutron/compat-matrix matrix:write\`.

## If a library you need says no

The listed failure is the real error, not a summary. Usual routes: check
whether the library ships a Preact-compatible build; pin to a version that
does; wrap the component as an island so it never server-renders; or use a
Preact-native equivalent. If you need React internals or RSC specifically,
Next.js is the honest recommendation — see the roadmap note in
\`docs/ADOPTION_FINDINGS.md\`.
`
  );
  console.log(`\nwrote ${DOC_PATH}`);
  console.log(`wrote ${BASELINE_PATH}`);
}

if (failed.length > 0 && !WRITE) {
  console.log(`\nNot rendering: ${failed.map((r) => r.label).join(", ")}`);
}

process.exit(exitCode);
