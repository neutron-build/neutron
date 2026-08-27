import { describe, expect, it } from "vitest";
import {
  renderSpeculationRules,
  renderStaticLinkSpeculationRules,
} from "./speculation-rules.js";

describe("speculation rules nonce handling", () => {
  it("admits a well-formed nonce", () => {
    const html = renderSpeculationRules({}, "abc123-_=");
    expect(html).toContain(' nonce="abc123-_="');
  });

  it("refuses a nonce that could inject attributes (charset-validated)", () => {
    // Pre-consolidation this copy interpolated the nonce verbatim, so a
    // hostile value could break out of the attribute.
    const hostile = 'x" onerror="alert(1)';
    for (const html of [
      renderSpeculationRules({}, hostile),
      renderStaticLinkSpeculationRules(undefined, hostile),
    ]) {
      expect(html).not.toContain("onerror");
      expect(html).not.toContain(`"${hostile}`);
    }
  });
});
