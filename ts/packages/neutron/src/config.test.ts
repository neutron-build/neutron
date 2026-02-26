import { describe, expect, it } from "vitest";
import {
  defineConfig,
  resolveRuntime,
  resolveRuntimeAliases,
  resolveRuntimeNoExternal,
} from "./config.js";

describe("runtime config", () => {
  it("defaults to preact runtime", () => {
    expect(resolveRuntime({})).toBe("preact");
    expect(resolveRuntime()).toBe("preact");
  });

  it("supports react-compat aliases", () => {
    const aliases = resolveRuntimeAliases("react-compat");
    expect(aliases?.react).toBe("preact/compat");
    expect(aliases?.["react-dom"]).toBe("preact/compat");
    expect(aliases?.["react-dom/server"]).toBe("preact-render-to-string");
    expect(aliases?.["react/jsx-runtime"]).toBe("preact/jsx-runtime");
  });

  it("does not emit aliases for default runtime", () => {
    expect(resolveRuntimeAliases("preact")).toBeUndefined();
  });

  it("returns noExternal dependencies for react-compat runtime", () => {
    expect(resolveRuntimeNoExternal("preact")).toEqual([]);
    expect(resolveRuntimeNoExternal("react-compat")).toContain("react-dom/server");
  });

  it("accepts global route rules in config", () => {
    const config = defineConfig({
      routes: {
        redirects: [{ source: "/old", destination: "/new", permanent: true }],
        rewrites: [{ source: "/app/:slug", destination: "/users/:slug" }],
        headers: [{ source: "/app/:slug", headers: { "X-Test": "on" } }],
      },
    });

    expect(config.routes?.redirects?.[0]?.statusCode).toBeUndefined();
    expect(config.routes?.redirects?.[0]?.permanent).toBe(true);
    expect(config.routes?.rewrites?.[0]?.destination).toBe("/users/:slug");
  });
});
