// @vitest-environment happy-dom
import { describe, expect, it } from "vitest";
import { render } from "preact";
import { createContext, createElement, Suspense } from "preact/compat";
import * as reactCompat from "./index.js";
import React, { use } from "./index.js";

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

describe("react-compat use()", () => {
  it("is on the module and on the default React object, beside compat's API", () => {
    expect(typeof use).toBe("function");
    expect(React.use).toBe(use);
    expect(typeof React.useState).toBe("function");
    expect(typeof (reactCompat as Record<string, unknown>).useState).toBe("function");
  });

  it("suspends on a pending promise and renders its value once it settles", async () => {
    let resolve!: (value: string) => void;
    const pending = new Promise<string>((r) => (resolve = r));
    function Value() {
      return createElement("b", null, "value:" + use(pending));
    }
    const root = document.createElement("div");
    render(createElement(Suspense, { fallback: createElement("i", null, "waiting") }, createElement(Value, null)), root);
    await flush();
    expect(root.textContent).toBe("waiting");
    resolve("ready");
    await flush();
    await flush();
    expect(root.textContent).toBe("value:ready");
  });

  it("returns a settled promise's value synchronously and throws a rejected one's reason", () => {
    const fulfilled = Object.assign(Promise.resolve(1), { status: "fulfilled" as const, value: 1 });
    expect(use(fulfilled)).toBe(1);
    const error = new Error("nope");
    const rejected = Object.assign(Promise.resolve(0), { status: "rejected" as const, reason: error });
    expect(() => use(rejected)).toThrow(error);
  });

  it("reads a context", () => {
    const Theme = createContext("light");
    function Reader() {
      return createElement("span", null, "theme:" + use(Theme));
    }
    const root = document.createElement("div");
    render(createElement(Theme.Provider, { value: "dark" }, createElement(Reader, null)), root);
    expect(root.textContent).toBe("theme:dark");
  });

  it("rejects values that are neither", () => {
    expect(() => use(42 as never)).toThrow(/unsupported type/);
  });
});
