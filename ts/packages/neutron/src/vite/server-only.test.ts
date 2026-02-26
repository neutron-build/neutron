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
});
