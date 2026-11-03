import { describe, expect, it } from "vitest";
import { negotiateContentEncoding } from "./content-encoding.js";

const both = ["br", "gzip"] as const;

describe("negotiateContentEncoding", () => {
  it.each([
    [undefined, null],
    ["", null],
    ["gzip, deflate, br", "br"],
    ["br;q=1.0, gzip;q=1.0", "br"],
    ["gzip, br;q=0.5", "gzip"],
    ["br;q=0, gzip", "gzip"],
    ["BR, GZIP", "br"],
    ["x-gzip", "gzip"],
    ["*", "br"],
    ["*;q=0.5, br;q=0", "gzip"],
    ["deflate", null],
    ["identity", null],
    ["br;q=0, gzip;q=0", null],
    ["gzip;q=0.5, identity", null],
    ["*;q=0", null],
  ] as const)("%s -> %s", (header, expected) => {
    expect(negotiateContentEncoding(header, both)).toBe(expected);
  });

  it("only offers what is available", () => {
    expect(negotiateContentEncoding("br, gzip", ["gzip"])).toBe("gzip");
    expect(negotiateContentEncoding("br", ["gzip"])).toBeNull();
    expect(negotiateContentEncoding("br, gzip", [])).toBeNull();
  });
});
