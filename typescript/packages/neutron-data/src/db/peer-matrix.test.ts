import assert from "node:assert/strict";
import test from "node:test";
import { createRequire } from "node:module";
import { existsSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

// I03 consumers' matrix (V17 subset): assert the peer combinations this
// package is ACTUALLY tested against by resolving every optional peer from
// this package's own node_modules (npm-ls-style), checking each installed
// version against the declared peer range (caret semantics, including the
// 0.x rule where ^0.44.5 means >=0.44.5 <0.45.0), and dynamically importing
// each installed combination's connector modules. The supported-peer claims
// in the README cite exactly these resolutions.
//
// Live behavioral smoke per combination lives in the sibling live suites
// (drizzle.postgres.live.test.ts, drizzle.sqlite.live.test.ts); here we pin
// the install shape so a dependency drift that silently changed the tested
// matrix would fail.

const packageRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
const require = createRequire(path.join(packageRoot, "package.json"));

interface PeerSpec {
  name: string;
  range: string;
}

const peers: PeerSpec[] = (() => {
  const pkg = JSON.parse(readFileSync(path.join(packageRoot, "package.json"), "utf8")) as {
    peerDependencies?: Record<string, string>;
  };
  return Object.entries(pkg.peerDependencies ?? {}).map(([name, range]) => ({ name, range }));
})();

function resolveInstalled(name: string): { version: string } | null {
  // Peer packages are exports-encapsulated (`<pkg>/package.json` is not a
  // subpath they expose), so resolve the ENTRY and walk up to the nearest
  // package.json whose name matches — npm-ls style, against the versions
  // this package's own resolution actually finds. ESM-only peers (the
  // nucleus workspace package has no `require` condition) resolve through
  // import.meta.resolve.
  let entry: string | null = null;
  try {
    entry = require.resolve(name);
  } catch {
    try {
      entry = fileURLToPath(import.meta.resolve(name));
    } catch {
      return null;
    }
  }
  let dir = path.dirname(entry!);
  for (;;) {
    const manifest = path.join(dir, "package.json");
    if (existsSync(manifest)) {
      const pkg = JSON.parse(readFileSync(manifest, "utf8")) as { name?: string; version?: string };
      if (pkg.name === name) {
        return pkg.version ? { version: pkg.version } : null;
      }
    }
    const parent = path.dirname(dir);
    if (parent === dir) return null;
    dir = parent;
  }
}

/** Caret-range satisfaction (the only comparator style this package
 *  declares). ^x.y.z: >=x.y.z and <(x+1).0.0 normally; <0.(y+1).0 when
 *  x === 0 and y > 0; <0.0.(z+1) when x === 0 and y === 0. Segment
 *  comparison is lexicographic on the first differing segment (3.989.0 is
 *  below 4.0.0). */
function satisfiesCaret(version: string, range: string): boolean {
  const match = /^\^(\d+)\.(\d+)\.(\d+)/.exec(range);
  if (!match) throw new Error(`peer-matrix: unsupported range style "${range}" for caret checker`);
  const [, xs, ys, zs] = match;
  const [x, y, z] = [Number(xs), Number(ys), Number(zs)];
  const upper = x > 0 ? [x + 1, 0, 0] : y > 0 ? [0, y + 1, 0] : [0, 0, z + 1];
  const parts = version.split("-")[0]!.split(".").map(Number);
  while (parts.length < 3) parts.push(0);
  const cmp = (a: number[], b: number[]): number => {
    for (let i = 0; i < 3; i++) {
      if (a[i]! < b[i]!) return -1;
      if (a[i]! > b[i]!) return 1;
    }
    return 0;
  };
  return cmp(parts, [x, y, z]) >= 0 && cmp(parts, upper) < 0;
}

test("peer-matrix: every installed optional peer satisfies its declared range", () => {
  assert.ok(peers.length >= 6, `expected the optional peer set, found ${peers.length}`);
  for (const peer of peers) {
    const installed = resolveInstalled(peer.name);
    if (!installed) continue; // optional: absence is a valid matrix cell
    // workspace:^ is the pnpm protocol form; published artifacts carry the
    // caret range it resolves to (a bare workspace:^ pins "this workspace's
    // twin", whose version the matrix records instead of checking it).
    const range = peer.range.replace(/^workspace:/, "");
    if (range === "^" || range === "*" || range === "") continue;
    assert.ok(
      satisfiesCaret(installed.version, range),
      `${peer.name}@${installed.version} is installed but does not satisfy the declared peer range ${peer.range} — update the range or the pin before claiming support`,
    );
  }
});

test("peer-matrix: the Drizzle combinations are importable as installed", async () => {
  const drizzle = resolveInstalled("drizzle-orm");
  const postgres = resolveInstalled("postgres");
  const libsql = resolveInstalled("@libsql/client");

  // The two combinations the wrapper's overloads type and the live suites
  // exercise. If either is absent locally the corresponding live suite
  // documents the skip; the matrix records the fact rather than inventing it.
  if (drizzle && postgres) {
    const pgConnector = (await import("drizzle-orm/postgres-js")) as { drizzle: unknown };
    assert.equal(typeof pgConnector.drizzle, "function", "drizzle-orm/postgres-js must export drizzle()");
    const postgresMod = (await import("postgres")) as { default: unknown };
    assert.equal(typeof postgresMod.default, "function", "postgres must export its client factory as default");
  }
  if (drizzle && libsql) {
    const libsqlConnector = (await import("drizzle-orm/libsql")) as { drizzle: unknown };
    assert.equal(typeof libsqlConnector.drizzle, "function", "drizzle-orm/libsql must export drizzle()");
    const libsqlMod = (await import("@libsql/client")) as { createClient: unknown };
    assert.equal(typeof libsqlMod.createClient, "function", "@libsql/client must export createClient()");
  }

  // Record the matrix (printed in the run output; the evidence file cites it).
  console.log(
    JSON.stringify({
      peerMatrix: {
        "drizzle-orm": drizzle?.version ?? null,
        postgres: postgres?.version ?? null,
        "@libsql/client": libsql?.version ?? null,
        "@neutron-build/nucleus": resolveInstalled("@neutron-build/nucleus")?.version ?? null,
      },
    }),
  );
});

test("peer-matrix: version facts match the ranges this package documents", () => {
  // The README's supported-peer line cites the versions the live suites
  // actually ran against. Pin them here so a lockfile drift that changed the
  // tested combination fails loudly instead of silently widening the claim.
  const drizzle = resolveInstalled("drizzle-orm");
  assert.ok(drizzle, "drizzle-orm must be installed in the development workspace for the wrapper suites to run");
  assert.match(drizzle.version, /^0\.44\./, `expected the 0.44.x drizzle-orm line, found ${drizzle.version}`);
});
