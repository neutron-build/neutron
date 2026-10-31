import { describe, expect, it } from "vitest";
import {
  analyzeServerActionModule,
  generateServerActionStub,
  serverActionId,
  ServerActionRegistry,
} from "./server-actions.js";

const analyze = (code: string) => analyzeServerActionModule(code, "src/actions.ts");

describe("analyzeServerActionModule", () => {
  it("ignores modules without the directive", () => {
    expect(analyze(`export const x = 1;`)).toEqual({ isServerActionModule: false, exports: [], errors: [] });
    expect(analyze(`"use client"; export function f() {}`).isServerActionModule).toBe(false);
  });

  it("collects every async-function export form", () => {
    const result = analyze(`
"use server";
import { db } from "./db";
export type Input = { a: string };
export interface Shape { b: number }
export async function create(input: Input) { return db.insert(input); }
export const update = async (id: string) => id;
export const remove = async function (id: string) { return id; };
export const typed = (async () => 1) satisfies () => Promise<number>;
async function local() { return 1; }
const aliasTarget = async () => 2;
export { local, aliasTarget as renamed };
export type { Shape as Alias };
export default async function () { return "default"; }
`);
    expect(result.errors).toEqual([]);
    expect(result.isServerActionModule).toBe(true);
    expect(result.exports).toEqual(["create", "update", "remove", "typed", "local", "renamed", "default"]);
  });

  it("accepts the directive after comments and another directive", () => {
    const result = analyze(`// header\n/* block */\n"use strict";\n'use server';\nexport async function a() {}`);
    expect(result.isServerActionModule).toBe(true);
    expect(result.exports).toEqual(["a"]);
  });

  it("rejects non-async, non-function and unverifiable exports with the rule", () => {
    const result = analyze(`
"use server";
import { helper } from "./helper";
export function sync() {}
export const value = 1;
export let later = async () => 1;
export const wrapped = withAuth(async () => 1);
export async function* stream() {}
export class Service {}
export { helper };
export { other } from "./other";
export * from "./more";
`);
    const errors = result.errors.join("\n");
    expect(errors).toMatch(/export "sync" is not async/);
    expect(errors).toMatch(/export "value" is not a function/);
    expect(errors).toMatch(/export "later" is not declared with `const`/);
    expect(errors).toMatch(/export "wrapped" is the result of a call the build cannot verify/);
    expect(errors).toMatch(/export "stream" is a generator function/);
    expect(errors).toMatch(/export "Service" is a class/);
    expect(errors).toMatch(/export "helper" re-exports an import/);
    expect(errors).toMatch(/re-exporting from another module is not allowed/);
    expect(errors).toMatch(/`export \*` is not allowed/);
    expect(errors).toMatch(/src\/actions\.ts:4: export "sync"/);
    expect(result.errors.every((error) => error.startsWith("src/actions.ts"))).toBe(true);
  });

  it("rejects inline \"use server\" functions anywhere", () => {
    const inComponent = analyze(`
export function Page() {
  async function save() {
    "use server";
    return 1;
  }
  return save;
}
`);
    expect(inComponent.isServerActionModule).toBe(false);
    expect(inComponent.errors).toHaveLength(1);
    expect(inComponent.errors[0]).toMatch(/src\/actions\.ts:3: inline "use server" functions .* are not supported/);

    const arrow = analyze(`export const f = () => { const g = async () => { "use server"; }; return g; };`);
    expect(arrow.errors[0]).toMatch(/inline "use server"/);
  });

  it("rejects a \"use server\" string that is not the module's first statement", () => {
    const result = analyze(`import x from "./x";\n"use server";\nexport async function a() {}`);
    expect(result.isServerActionModule).toBe(false);
    expect(result.errors[0]).toMatch(/:2: "use server" must be the first statement/);
  });

  it("rejects a module that is both client and server", () => {
    expect(analyze(`"use client";\n"use server";\nexport async function a() {}`).errors[0]).toMatch(
      /both "use client" and "use server"/
    );
  });
});

describe("serverActionId", () => {
  it("is a stable function of module path and export name", () => {
    const id = serverActionId("src/actions.ts", "create");
    expect(id).toMatch(/^[0-9a-f]{40}$/);
    expect(serverActionId("src/actions.ts", "create")).toBe(id);
    expect(serverActionId("src/actions.ts", "update")).not.toBe(id);
    expect(serverActionId("src/other.ts", "create")).not.toBe(id);
  });
});

describe("generateServerActionStub", () => {
  it("exports one reference per action, and nothing of the module", () => {
    const stub = generateServerActionStub("src/actions.ts", ["create", "default"]);
    expect(stub).toContain('import { createServerActionReference } from "@neutron-build/core/client";');
    expect(stub).toContain(
      `export const create = /*#__PURE__*/ createServerActionReference(${JSON.stringify(serverActionId("src/actions.ts", "create"))}, "create");`
    );
    expect(stub).toContain(`export default /*#__PURE__*/ createServerActionReference(`);
    expect(stub).not.toContain("./db");
  });
});

describe("ServerActionRegistry", () => {
  it("maps ids to modules, replaces and forgets modules, and writes a sorted manifest", () => {
    const registry = new ServerActionRegistry();
    registry.set("src/b.ts", ["two", "one"]);
    registry.set("src/a.ts", ["x"]);
    expect(registry.size).toBe(3);
    expect(registry.lookup(serverActionId("src/b.ts", "one"))).toEqual({ file: "src/b.ts", export: "one" });
    registry.set("src/b.ts", ["one"]);
    expect(registry.lookup(serverActionId("src/b.ts", "two"))).toBeNull();
    const manifest = registry.toManifest();
    expect(manifest.format).toBe(1);
    expect(Object.values(manifest.actions)).toEqual([
      { file: "src/a.ts", export: "x" },
      { file: "src/b.ts", export: "one" },
    ]);
    registry.delete("src/a.ts");
    expect(registry.size).toBe(1);
    expect(registry.lookup("unknown")).toBeNull();
  });
});
