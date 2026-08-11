#!/usr/bin/env node
// Committed launcher for the `neutron-ts` bin.
//
// `bin` pointed straight at `dist/index.js`, which is a build artifact and so
// absent on a clean checkout. pnpm creates bin symlinks during `install`, and
// when the target does not exist it only WARNS:
//
//   WARN Failed to create bin at apps/site/node_modules/.bin/neutron-ts.
//   ENOENT: ... packages/neutron-cli/dist/index.js
//
// Install then "succeeds", the later `pnpm -r run build` produces dist/, but
// nothing goes back to create the symlink — so every app that builds with
// `neutron-ts build` died on `sh: 1: neutron-ts: not found`. It only worked on
// a developer machine because dist/ was left over from a previous build, which
// is why this looked like a CI-only fault.
//
// Pointing `bin` at this file instead means the symlink target always exists at
// install time. dist/ is required lazily, by which point the build has run.
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const entry = join(dirname(fileURLToPath(import.meta.url)), "..", "dist", "index.js");

if (!existsSync(entry)) {
  console.error(
    "neutron-ts: @neutron-build/cli has not been built (dist/index.js is missing).\n" +
      "Run `pnpm --filter @neutron-build/cli build` first, or `pnpm -r run build` from the workspace root."
  );
  process.exit(1);
}

await import(entry);
