import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterAll, describe, expect, it } from "vitest";
import { isCommonJsModule, transformSsrCommonJs } from "./ssr-cjs.js";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "neutron-ssr-cjs-"));
afterAll(() => fs.rmSync(root, { recursive: true, force: true }));

function pkg(name: string, manifest: Record<string, unknown>, files: Record<string, string>): string {
  const dir = path.join(root, "node_modules", name);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, "package.json"), JSON.stringify({ name, ...manifest }));
  for (const [rel, source] of Object.entries(files)) {
    fs.mkdirSync(path.dirname(path.join(dir, rel)), { recursive: true });
    fs.writeFileSync(path.join(dir, rel), source);
  }
  return dir;
}

describe("isCommonJsModule", () => {
  it("tells CommonJS from ESM by syntax and the nearest package.json type", () => {
    const dir = pkg("mixed", {}, {});
    const at = (rel: string) => path.join(dir, rel);
    expect(isCommonJsModule(at("a.js"), "exports.useStore = 1;\n")).toBe(true);
    expect(isCommonJsModule(at("b.js"), "'use strict';\nmodule.exports = require('./a');\n")).toBe(true);
    expect(isCommonJsModule(at("c.cjs"), "whatever")).toBe(true);
    expect(isCommonJsModule(at("d.js"), "export const x = 1;\nexports.y = 2;\n")).toBe(false);
    expect(isCommonJsModule(at("e.js"), "import x from 'y';\n")).toBe(false);
    expect(isCommonJsModule(at("f.mjs"), "module.exports = 1;\n")).toBe(false);
    const esm = pkg("esm-only", { type: "module" }, {});
    expect(isCommonJsModule(path.join(esm, "g.js"), "module.exports = 1;\n")).toBe(false);
  });
});

describe("transformSsrCommonJs", () => {
  it("loads a package without React dependencies natively", () => {
    const dir = pkg("plain", {}, { "index.js": "module.exports = 1;\n" });
    const out = transformSsrCommonJs("module.exports = 1;\n", path.join(dir, "index.js"))!;
    expect(out).toContain("__neutron_createRequire__(");
    expect(out).not.toContain("module.exports = 1");
  });

  it("runs a React-ecosystem package in-graph, hoisting react and its own files but not optional deps", () => {
    const dir = pkg("shim", { peerDependencies: { react: "*" } }, { "impl.js": "exports.x = 1;\n" });
    const code = "var React = require('react');\nvar impl = require('./impl.js');\ntry { require('not-installed'); } catch (e) {}\nexports.y = impl.x;\n";
    const out = transformSsrCommonJs(code, path.join(dir, "index.js"))!;
    expect(out).toContain('import * as __neutron_req0__ from "react";');
    expect(out).toContain('import * as __neutron_req1__ from "./impl.js";');
    expect(out).not.toContain('from "not-installed"');
    expect(out).toContain(code);
  });
});
