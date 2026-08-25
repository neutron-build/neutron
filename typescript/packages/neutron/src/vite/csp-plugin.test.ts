import { describe, it, expect } from "vitest";
import type { Plugin } from "vite";
import { cspPlugin } from "./csp-plugin.js";

const HTML = `<html><head><title>t</title><script>console.log(1)</script><style>.a{color:red}</style></head><body></body></html>`;

function transform(plugin: Plugin, html: string): string {
  const hook = plugin.transformIndexHtml as unknown as
    | ((html: string, ctx: unknown) => unknown)
    | { handler: (html: string, ctx: unknown) => unknown };
  const fn = typeof hook === "function" ? hook : hook.handler;
  return fn(html, undefined) as string;
}

function resolve(plugin: Plugin, command: "build" | "serve"): void {
  const hook = plugin.configResolved as unknown as
    | ((config: unknown) => void)
    | { handler: (config: unknown) => void };
  if (typeof hook === "function") {
    hook({ command });
  } else if (hook && typeof hook === "object" && typeof hook.handler === "function") {
    hook.handler({ command });
  }
}

describe("cspPlugin nonce mode", () => {
  it("dev (serve): nonce is per-request", () => {
    const plugin = cspPlugin({ enabled: true, useNonce: true, directives: {} });
    resolve(plugin, "serve");

    const first = transform(plugin as Plugin, HTML);
    const second = transform(plugin as Plugin, HTML);

    expect(first).toMatch(/'nonce-/);
    expect(second).toMatch(/'nonce-/);
    expect(first).not.toBe(second);
  });

  it("build: a build-time nonce is shared by every served copy, so the plugin must fall back to hashes", () => {
    const plugin = cspPlugin({ enabled: true, useNonce: true, directives: {} });
    resolve(plugin, "build");

    const out = transform(plugin as Plugin, HTML);

    // Hash-based policy covering the same inline content instead of a nonce
    // that every visitor shares (a shared nonce protects nothing).
    expect(out).toMatch(/'sha256-/);
    expect(out).not.toMatch(/'nonce-/);
    expect(out).not.toMatch(/<script nonce=/);
  });
});
