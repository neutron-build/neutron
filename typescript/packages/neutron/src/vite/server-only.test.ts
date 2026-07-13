import { describe, expect, it } from "vitest";
import {
  hasServerOnlyImport,
  isServerOnlySpecifier,
  stripServerOnlyRouteModule,
} from "./server-only.js";

describe("server-only module handling", () => {
  it("detects .server imports", () => {
    expect(isServerOnlySpecifier("./db.server")).toBe(true);
    expect(isServerOnlySpecifier("./db.server.ts")).toBe(true);
    expect(isServerOnlySpecifier("./db.server.ts?raw")).toBe(true);
    expect(isServerOnlySpecifier("./db.client.ts")).toBe(false);
  });

  it("finds .server imports in module code", () => {
    const code = `
      import { db } from "./db.server";
      import { h } from "preact";
      export default function Page() {
        return h("div", null, "ok");
      }
    `;

    expect(hasServerOnlyImport(code)).toBe(true);
  });

  it("strips server exports and .server imports from route modules", () => {
    const code = `
      import { db } from "./db.server";
      import { h } from "preact";

      export const config = { mode: "app" };
      export async function loader() {
        return db.query();
      }
      export const action = async () => ({ ok: true });
      export { loader as alsoLoader };

      export default function Page() {
        return h("div", null, "ok");
      }
    `;

    const transformed = stripServerOnlyRouteModule(code);

    expect(transformed.includes("./db.server")).toBe(false);
    expect(transformed.includes("export async function loader")).toBe(false);
    expect(transformed.includes("export const action")).toBe(false);
    expect(transformed.includes("alsoLoader")).toBe(false);
    expect(transformed.includes("export const config")).toBe(true);
    expect(transformed.includes("export default function Page")).toBe(true);
  });

  it("strips HTTP-method handler exports from API routes (e.g. rss.xml.ts)", () => {
    // Regression: a GET handler that reads content must not survive into the
    // client bundle, or its server-side import (getCollection -> node:fs) leaks
    // and breaks the build. After stripping GET, the now-unused import is
    // tree-shaken by the bundler (core is sideEffects:false).
    const code = `
      import { getCollection } from "@neutron-build/core";
      export async function GET() {
        const posts = await getCollection("blog");
        return new Response("<rss/>");
      }
      export const POST = async () => new Response("ok");
    `;

    const transformed = stripServerOnlyRouteModule(code);

    expect(transformed.includes("export async function GET")).toBe(false);
    expect(transformed.includes("export const POST")).toBe(false);
  });

  it("strips node: builtin imports from the client route module (R3 #5)", () => {
    // A loader's node builtin import survives the loader's removal and, left
    // in place, breaks the browser bundle. Both node: and bare-builtin forms.
    const code = `
      import { readFileSync } from "node:fs";
      import path from "path";
      import { getUser } from "./db.server.ts";
      import { h } from "preact";
      export async function loader() {
        return { data: readFileSync(path.join("/x")), user: getUser() };
      }
      export default function Page() { return h("div", null, "hi"); }
    `;

    const transformed = stripServerOnlyRouteModule(code);

    expect(transformed.includes("node:fs")).toBe(false);
    expect(/from ["']path["']/.test(transformed)).toBe(false);
    expect(transformed.includes("db.server")).toBe(false);
    // The isomorphic import and the component survive.
    expect(transformed.includes("preact")).toBe(true);
    expect(transformed.includes("export default function Page")).toBe(true);
  });

  it("keeps look-alike packages (fs-extra, path-browserify) that are NOT builtins", () => {
    const code = `
      import fse from "fs-extra";
      import pb from "path-browserify";
      import { h } from "preact";
      export default function Page() { return h("div", null, String(fse) + String(pb)); }
    `;
    const transformed = stripServerOnlyRouteModule(code);
    expect(transformed.includes("fs-extra")).toBe(true);
    expect(transformed.includes("path-browserify")).toBe(true);
  });
});
