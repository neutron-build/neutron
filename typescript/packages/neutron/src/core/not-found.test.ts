import { describe, expect, it } from "vitest";
import { notFound } from "./response.js";

describe("notFound", () => {
  // It used to return `new Response("Not Found")` with no content type, so a
  // browser rendered two words of plain text on a white page — which is what
  // every Neutron app shipped as its 404 unless the author hand-rolled one.
  it("returns an HTML document, not bare text", async () => {
    const res = notFound();
    expect(res.status).toBe(404);
    expect(res.headers.get("Content-Type")).toContain("text/html");
    const body = await res.text();
    expect(body).toMatch(/^<!doctype html>/i);
    expect(body).toContain("404");
  });

  it("places a short message inside the shell", async () => {
    const body = await notFound("No such project").text();
    expect(body).toContain("No such project");
    expect(body).toMatch(/^<!doctype html>/i);
  });

  // A caller supplying a whole document means it, and wrapping it would produce
  // nested <html> elements.
  it("passes a full document through untouched", async () => {
    const custom = "<!doctype html><html><body>mine</body></html>";
    expect(await notFound(custom).text()).toBe(custom);
  });

  // The message reaches the page as text, so it must not be able to introduce
  // markup — a 404 often echoes part of the URL that produced it.
  it("escapes the message", async () => {
    const body = await notFound('<img src=x onerror="alert(1)">').text();
    expect(body).not.toContain("<img");
    expect(body).toContain("&lt;img");
  });
});
