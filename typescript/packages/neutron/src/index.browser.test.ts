import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import * as full from "./index.js";
import * as browser from "./index.browser.js";

const SRC = path.dirname(fileURLToPath(import.meta.url));

/** Bare and node: specifiers reachable from a source file through relative imports. */
function reachableSpecifiers(entry: string): Set<string> {
  const found = new Set<string>();
  const seen = new Set<string>();
  const visit = (file: string) => {
    if (seen.has(file)) return;
    seen.add(file);
    const code = fs.readFileSync(file, "utf-8");
    const re = /^\s*(?:import|export)\s(?!type\b)[^;]*?from\s*["']([^"']+)["']|^\s*import\s*["']([^"']+)["']|\bimport\(\s*["']([^"']+)["']\s*\)/gm;
    for (const match of code.matchAll(re)) {
      const specifier = match[1] ?? match[2] ?? match[3];
      if (!specifier.startsWith(".")) {
        found.add(specifier);
        continue;
      }
      const base = path.resolve(path.dirname(file), specifier.replace(/\.js$/, ""));
      const candidates = [".ts", ".tsx", "/index.ts"].map((ext) => base + ext);
      const next = candidates.find((candidate) => fs.existsSync(candidate));
      if (next) visit(next);
    }
  };
  visit(entry);
  return found;
}

describe("browser root entry", () => {
  it("exports exactly the names of the full root entry", () => {
    expect(Object.keys(browser).sort()).toEqual(Object.keys(full).sort());
  });

  it("re-exports the client modules themselves, not copies", () => {
    expect(browser.Link).toBe(full.Link);
    expect(browser.useLocation).toBe(full.useLocation);
    expect(browser.withRouterProviders).toBe(full.withRouterProviders);
    expect(browser.z).toBe(full.z);
  });

  it("reaches no node built-in and no server-only dependency", () => {
    const specifiers = [...reachableSpecifiers(path.join(SRC, "index.browser.ts"))];
    const allowed = /^(preact(\/.*)?|devalue|zod|@preact\/signals-core|@neutron-build\/core\/client)$/;
    expect(specifiers.filter((specifier) => !allowed.test(specifier))).toEqual([]);
  });

  it("stubs node-only functions with a server-only error", () => {
    expect(() => browser.createServer()).toThrow(/createServer from @neutron-build\/core is server-only/);
  });
});
