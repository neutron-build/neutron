import { describe, it, expect } from "vitest";
import { generateFontHTML, validateFontConfig } from "./fonts.js";

describe("generateFontHTML preloads", () => {
  it("does not fabricate gstatic preload URLs for Google fonts", () => {
    const html = generateFontHTML({ google: ["Inter:400,700"], preload: ["Inter-400"] });
    // Real gstatic file URLs carry content hashes only knowable by fetching
    // the provider CSS; a fabricated URL is a guaranteed 404 preload.
    expect(html).not.toContain("fonts.gstatic.com");
  });

  it("does not fabricate bunny preload URLs for Bunny fonts", () => {
    const html = generateFontHTML({ bunny: ["Inter"], preload: ["Inter-400"] });
    expect(html).not.toMatch(/fonts\.bunny\.net\/[a-z-]*inter.*\.woff2/);
  });

  it("preloads local font files from their known src", () => {
    const html = generateFontHTML({
      local: [{ family: "Custom", src: "/fonts/custom.woff2", weight: 400 }],
      preload: ["Custom-400"],
    });
    expect(html).toContain('<link rel="preload" href="/fonts/custom.woff2"');
  });
});

describe("validateFontConfig preload specs", () => {
  it("accepts hyphenated family names in preload specs", () => {
    const { valid, errors } = validateFontConfig({
      local: [{ family: "My-Font", src: "/f.woff2" }],
      preload: ["My-Font-400"],
    });
    expect(errors).toEqual([]);
    expect(valid).toBe(true);
  });
});
