import { describe, expect, it } from "vitest";
import {
  applyCorsHeaders,
  applySecurityHeaders,
  createCorsPreflightResponse,
  resolveCorsOptions,
  resolveSecurityHeadersConfig,
} from "./http-headers.js";

describe("http headers helpers", () => {
  it("creates preflight response for allowed origins", () => {
    const options = resolveCorsOptions({
      origin: ["https://app.example.com"],
      methods: ["GET", "POST"],
      allowedHeaders: ["Content-Type"],
      credentials: true,
      maxAge: 300,
    });
    expect(options).toBeTruthy();

    const request = new Request("http://localhost/users", {
      method: "OPTIONS",
      headers: {
        Origin: "https://app.example.com",
        "Access-Control-Request-Method": "POST",
      },
    });

    const response = createCorsPreflightResponse(request, options!);
    expect(response).toBeTruthy();
    expect(response?.status).toBe(204);
    expect(response?.headers.get("Access-Control-Allow-Origin")).toBe(
      "https://app.example.com"
    );
    expect(response?.headers.get("Access-Control-Allow-Credentials")).toBe("true");
    expect(response?.headers.get("Access-Control-Max-Age")).toBe("300");
  });

  it("rejects preflight response for disallowed origins", () => {
    const options = resolveCorsOptions({ origin: ["https://good.example.com"] });
    expect(options).toBeTruthy();

    const request = new Request("http://localhost/users", {
      method: "OPTIONS",
      headers: {
        Origin: "https://evil.example.com",
        "Access-Control-Request-Method": "POST",
      },
    });

    const response = createCorsPreflightResponse(request, options!);
    expect(response).toBeTruthy();
    expect(response?.status).toBe(403);
  });

  it("throws when credentials: true with wildcard origin", () => {
    expect(() =>
      resolveCorsOptions({ origin: "*", credentials: true })
    ).toThrow("credentials cannot be used with origin");

    // Also ensure default origin "*" triggers the error
    expect(() =>
      resolveCorsOptions({ credentials: true })
    ).toThrow("credentials cannot be used with origin");
  });

  it("applies cors headers to normal responses", () => {
    const options = resolveCorsOptions({
      origin: ["https://app.example.com"],
      credentials: true,
      exposedHeaders: ["x-neutron-cache"],
    });
    expect(options).toBeTruthy();

    const request = new Request("http://localhost/users", {
      headers: {
        Origin: "https://app.example.com",
      },
    });
    const response = new Response("ok");

    applyCorsHeaders(request, response, options!);

    expect(response.headers.get("Access-Control-Allow-Origin")).toBe(
      "https://app.example.com"
    );
    expect(response.headers.get("Access-Control-Allow-Credentials")).toBe("true");
    expect(response.headers.get("Access-Control-Expose-Headers")).toContain(
      "x-neutron-cache"
    );
  });

  it("applies security headers without overriding existing header values", () => {
    const config = resolveSecurityHeadersConfig({
      headers: {
        "X-Frame-Options": "SAMEORIGIN",
      },
    });
    expect(config).toBeTruthy();

    const response = new Response("ok", {
      headers: {
        "Referrer-Policy": "no-referrer",
      },
    });

    applySecurityHeaders(response, config!);

    expect(response.headers.get("X-Content-Type-Options")).toBe("nosniff");
    expect(response.headers.get("X-Frame-Options")).toBe("SAMEORIGIN");
    expect(response.headers.get("Referrer-Policy")).toBe("no-referrer");
  });
});
