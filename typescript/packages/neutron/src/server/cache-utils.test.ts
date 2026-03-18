import { describe, expect, it } from "vitest";
import { createEntityTag, requestHasMatchingEtag } from "./cache-utils.js";

describe("cache utils", () => {
  it("creates stable weak etags for same body", () => {
    const a = createEntityTag("<h1>Hello</h1>");
    const b = createEntityTag("<h1>Hello</h1>");
    expect(a).toBe(b);
    expect(a.startsWith('W/"')).toBe(true);
  });

  it("matches if-none-match with exact etag", () => {
    const etag = createEntityTag("payload");
    const request = new Request("https://example.test/", {
      headers: {
        "If-None-Match": etag,
      },
    });
    expect(requestHasMatchingEtag(request, etag)).toBe(true);
  });

  it("matches weak and strong etag variants", () => {
    const weak = 'W/"a-b"';
    const strong = '"a-b"';
    const request = new Request("https://example.test/", {
      headers: {
        "If-None-Match": strong,
      },
    });
    expect(requestHasMatchingEtag(request, weak)).toBe(true);
  });

  it("supports wildcard revalidation", () => {
    const request = new Request("https://example.test/", {
      headers: {
        "If-None-Match": "*",
      },
    });
    expect(requestHasMatchingEtag(request, '"x"')).toBe(true);
  });
});
