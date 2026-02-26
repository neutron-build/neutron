import { describe, expect, it } from "vitest";
import { getCookie, parseCookieHeader, serializeCookie } from "./cookies.js";

describe("cookies", () => {
  it("parses cookie headers", () => {
    const parsed = parseCookieHeader("a=1; b=two%20words; c=3");
    expect(parsed).toEqual({
      a: "1",
      b: "two words",
      c: "3",
    });
  });

  it("reads single cookie from request", () => {
    const request = new Request("http://localhost", {
      headers: {
        Cookie: "session=abc123; theme=dark",
      },
    });

    expect(getCookie(request, "session")).toBe("abc123");
    expect(getCookie(request, "missing")).toBeUndefined();
  });

  it("serializes cookie with options", () => {
    const value = serializeCookie("session", "abc 123", {
      path: "/",
      httpOnly: true,
      secure: true,
      sameSite: "Lax",
      maxAge: 60,
    });

    expect(value).toContain("session=abc%20123");
    expect(value).toContain("Max-Age=60");
    expect(value).toContain("Path=/");
    expect(value).toContain("HttpOnly");
    expect(value).toContain("Secure");
    expect(value).toContain("SameSite=Lax");
  });

  it("throws on invalid cookie name", () => {
    expect(() => serializeCookie("bad name", "value")).toThrow("Invalid cookie name");
    expect(() => serializeCookie("bad\nname", "value")).toThrow("Invalid cookie name");
    expect(() => serializeCookie("", "value")).toThrow("Invalid cookie name");
  });

  it("throws on invalid cookie domain", () => {
    expect(() => serializeCookie("ok", "value", { domain: "evil\nhost" })).toThrow("Invalid cookie domain");
    expect(() => serializeCookie("ok", "value", { domain: "evil host" })).toThrow("Invalid cookie domain");
  });

  it("strips surrounding quotes from cookie values per RFC 6265", () => {
    const parsed = parseCookieHeader('token="abc123"; theme="dark"');
    expect(parsed.token).toBe("abc123");
    expect(parsed.theme).toBe("dark");
  });
});
