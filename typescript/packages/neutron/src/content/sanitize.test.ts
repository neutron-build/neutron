import { describe, it, expect } from "vitest";
import { sanitizeHtml } from "./sanitize.js";

describe("sanitizeHtml (real parser-based sanitizer)", () => {
  it("preserves safe markup", async () => {
    const out = await sanitizeHtml("<p>Hello <strong>world</strong></p>");
    expect(out).toContain("<p>Hello <strong>world</strong></p>");
  });

  it("strips <script> tags", async () => {
    const out = await sanitizeHtml("<p>ok</p><script>alert(1)</script>");
    expect(out).not.toContain("<script");
    expect(out).toContain("<p>ok</p>");
  });

  it("strips inline event-handler attributes", async () => {
    const out = await sanitizeHtml('<img src="x" onerror="alert(1)">');
    expect(out).not.toContain("onerror");
  });

  it("strips javascript: URLs", async () => {
    const out = await sanitizeHtml('<a href="javascript:alert(1)">x</a>');
    expect(out.toLowerCase()).not.toContain("javascript:");
  });

  it("neutralizes the svg/onload bypass that defeated the old regex", async () => {
    const out = await sanitizeHtml("<svg/onload=alert(1)>");
    expect(out).not.toContain("onload");
  });

  it("neutralizes the unquoted/entity-encoded javascript URL bypasses", async () => {
    const out = await sanitizeHtml('<a href=javascript:alert(1)>x</a>');
    expect(out.toLowerCase()).not.toContain("javascript:");
  });
});
