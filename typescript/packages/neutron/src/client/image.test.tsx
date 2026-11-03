import { afterEach, describe, expect, it } from "vitest";
import { h } from "preact";
import { renderToString } from "preact-render-to-string";
import { Image } from "./image.js";
import { Picture } from "./picture.js";
import { configureImageWidths } from "../core/image-widths.js";

afterEach(() => configureImageWidths(undefined));

function attr(html: string, name: string): string | undefined {
  return new RegExp(`\\s${name}="([^"]*)"`).exec(html)?.[1]?.replace(/&amp;/g, "&");
}

function srcsetWidths(srcset: string | undefined): number[] {
  return (srcset ?? "").split(", ").map((entry) => Number(/ (\d+)w$/.exec(entry)?.[1]));
}

describe("<Image>", () => {
  it("fixed px sizes emits 1x/2x candidates and the 1x src", () => {
    const html = renderToString(h(Image, { src: "/nav.png", alt: "", sizes: "370px" }) as never);
    expect(attr(html, "srcset")).toBe(
      "/_neutron/image?src=%2Fnav.png&w=384&q=75 384w, /_neutron/image?src=%2Fnav.png&w=750&q=75 750w"
    );
    expect(attr(html, "src")).toBe("/_neutron/image?src=%2Fnav.png&w=384&q=75");
    expect(attr(html, "sizes")).toBe("370px");
  });

  it("a width prop sizes the image to that width", () => {
    const html = renderToString(h(Image, { src: "/a.png", alt: "", width: 48, height: 48 }) as never);
    expect(srcsetWidths(attr(html, "srcset"))).toEqual([48, 96]);
    expect(attr(html, "sizes")).toBe("48px");
  });

  it("viewport sizes emits the full ladder without using the largest as src", () => {
    const html = renderToString(h(Image, { src: "/hero.png", alt: "", sizes: "100vw" }) as never);
    expect(srcsetWidths(attr(html, "srcset"))).toHaveLength(16);
    expect(attr(html, "src")).toContain("w=640");
  });

  it("uses the configured ladder", () => {
    configureImageWidths([320, 640, 960, 1200, 1600]);
    const html = renderToString(h(Image, { src: "/a.png", alt: "", sizes: "100vw" }) as never);
    expect(srcsetWidths(attr(html, "srcset"))).toEqual([320, 640, 960, 1200, 1600]);
  });

  it("clamps to the intrinsic width", () => {
    const html = renderToString(
      h(Image, { src: "/a.png", alt: "", sizes: "370px", intrinsicWidth: 400 }) as never
    );
    expect(srcsetWidths(attr(html, "srcset"))).toEqual([384]);
  });
});

describe("<Picture>", () => {
  it("shares the selection rule for every source and the fallback", () => {
    const html = renderToString(
      h(Picture, { src: "/p.png", alt: "", sizes: "370px", formats: ["avif", "webp"] }) as never
    );
    const sources = html.match(/<source[^>]*>/g) ?? [];
    expect(sources).toHaveLength(2);
    for (const source of sources) {
      expect(srcsetWidths(attr(source, "srcset"))).toEqual([384, 750]);
      expect(attr(source, "sizes")).toBe("370px");
    }
    const img = /<img[^>]*>/.exec(html)?.[0] ?? "";
    expect(attr(img, "src")).toBe("/_neutron/image?src=%2Fp.png&w=384");
  });

  it("defaults to 100vw over the full ladder", () => {
    const html = renderToString(h(Picture, { src: "/p.png", alt: "", formats: ["webp"] }) as never);
    const source = /<source[^>]*>/.exec(html)?.[0] ?? "";
    expect(attr(source, "sizes")).toBe("100vw");
    expect(srcsetWidths(attr(source, "srcset"))).toHaveLength(16);
  });
});
