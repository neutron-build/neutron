import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  validateImageParams,
  resolveSourceFile,
  buildCacheKey,
  optimizeImage,
  handleImageRequest,
} from "./image-optimizer.js";

describe("validateImageParams", () => {
  it("parses valid params", () => {
    const params = new URLSearchParams("src=/photo.jpg&w=640&q=80&fmt=webp");
    const result = validateImageParams(params);
    expect(result).toEqual({
      src: "/photo.jpg",
      width: 640,
      quality: 80,
      format: "webp",
    });
  });

  it("applies defaults for quality and format", () => {
    const params = new URLSearchParams("src=/img.png&w=320");
    const result = validateImageParams(params);
    expect(result).toEqual({
      src: "/img.png",
      width: 320,
      quality: 75,
      format: "webp",
    });
  });

  it("rejects missing src", () => {
    const params = new URLSearchParams("w=640");
    const result = validateImageParams(params);
    expect(result).toEqual({ error: "Missing 'src' parameter", status: 400 });
  });

  it("rejects src not starting with /", () => {
    const params = new URLSearchParams("src=photo.jpg&w=640");
    const result = validateImageParams(params);
    expect(result).toEqual({
      error: "Image src must start with '/'",
      status: 400,
    });
  });

  it("rejects path traversal", () => {
    const params = new URLSearchParams("src=/../../etc/passwd&w=640");
    const result = validateImageParams(params);
    expect(result).toEqual({
      error: "Path traversal not allowed",
      status: 400,
    });
  });

  it("rejects absolute URLs with no allowlist configured, naming the config", () => {
    const params = new URLSearchParams(
      "src=" + encodeURIComponent("https://cdn.example.com/photo.jpg") + "&w=640"
    );
    const result = validateImageParams(params);
    expect("error" in result).toBe(true);
    expect((result as { error: string }).error).toContain("remotePatterns");
    expect((result as { status: number }).status).toBe(400);
  });

  it("rejects absolute URLs whose hostname is not in the allowlist", () => {
    const params = new URLSearchParams(
      "src=" + encodeURIComponent("https://evil.example.com/photo.jpg") + "&w=640"
    );
    const result = validateImageParams(params, [
      { hostname: "cdn.example.com" },
    ]);
    expect("error" in result).toBe(true);
    expect((result as { error: string }).error).toContain("remotePatterns");
  });

  it("accepts an allowed remote URL and marks it remote", () => {
    const params = new URLSearchParams(
      "src=" + encodeURIComponent("https://cdn.example.com/photo.jpg") + "&w=640"
    );
    const result = validateImageParams(params, [
      { hostname: "cdn.example.com" },
    ]);
    expect(result).toEqual({
      src: "https://cdn.example.com/photo.jpg",
      width: 640,
      quality: 75,
      format: "webp",
      remote: true,
    });
  });

  it("enforces the pattern protocol when specified", () => {
    const params = new URLSearchParams(
      "src=" + encodeURIComponent("http://cdn.example.com/photo.jpg") + "&w=640"
    );
    const result = validateImageParams(params, [
      { hostname: "cdn.example.com", protocol: "https" },
    ]);
    expect("error" in result).toBe(true);
    expect((result as { error: string }).error).toContain("remotePatterns");
  });

  it("rejects missing width", () => {
    const params = new URLSearchParams("src=/img.png");
    const result = validateImageParams(params);
    expect(result).toEqual({
      error: "Missing 'w' (width) parameter",
      status: 400,
    });
  });

  it("rejects width below minimum", () => {
    const params = new URLSearchParams("src=/img.png&w=8");
    const result = validateImageParams(params);
    expect("error" in result).toBe(true);
  });

  it("rejects width above maximum", () => {
    const params = new URLSearchParams("src=/img.png&w=5000");
    const result = validateImageParams(params);
    expect("error" in result).toBe(true);
  });

  it("rejects invalid format", () => {
    const params = new URLSearchParams("src=/img.png&w=640&fmt=gif");
    const result = validateImageParams(params);
    expect("error" in result).toBe(true);
  });

  it("accepts all valid formats", () => {
    for (const fmt of ["webp", "avif", "jpeg", "png"]) {
      const params = new URLSearchParams(`src=/img.png&w=640&fmt=${fmt}`);
      const result = validateImageParams(params);
      expect("error" in result).toBe(false);
    }
  });

  it("rejects quality below 1", () => {
    const params = new URLSearchParams("src=/img.png&w=640&q=0");
    const result = validateImageParams(params);
    expect("error" in result).toBe(true);
  });

  it("rejects quality above 100", () => {
    const params = new URLSearchParams("src=/img.png&w=640&q=101");
    const result = validateImageParams(params);
    expect("error" in result).toBe(true);
  });
});

// A real 1x1 PNG — tests that exercise the decode path need valid image
// bytes, since the optimizer fails closed on undecodable input (TS-13).
const REAL_PNG = Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==",
  "base64"
);

describe("resolveSourceFile", () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "neutron-img-test-"));
    fs.mkdirSync(path.join(tmpDir, "public"), { recursive: true });
    fs.mkdirSync(path.join(tmpDir, "src"), { recursive: true });
    fs.writeFileSync(path.join(tmpDir, "public", "photo.jpg"), "fakejpg");
    fs.writeFileSync(path.join(tmpDir, "src", "logo.png"), "fakepng");
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it("finds file in first publicDir", async () => {
    const result = await resolveSourceFile("/photo.jpg", [
      path.join(tmpDir, "public"),
      path.join(tmpDir, "src"),
    ]);
    // Containment resolves through the filesystem (symlinks, /var →
    // /private/var on macOS), so compare realpaths.
    expect(result).toBe(fs.realpathSync(path.join(tmpDir, "public", "photo.jpg")));
  });

  it("finds file in second publicDir", async () => {
    const result = await resolveSourceFile("/logo.png", [
      path.join(tmpDir, "public"),
      path.join(tmpDir, "src"),
    ]);
    expect(result).toBe(fs.realpathSync(path.join(tmpDir, "src", "logo.png")));
  });

  it("returns null for missing file", async () => {
    const result = await resolveSourceFile("/missing.jpg", [
      path.join(tmpDir, "public"),
    ]);
    expect(result).toBeNull();
  });

  it("rejects path traversal attempts", async () => {
    // Even if validateImageParams is bypassed, resolveSourceFile should catch this
    const result = await resolveSourceFile("/../../../etc/passwd", [
      path.join(tmpDir, "public"),
    ]);
    expect(result).toBeNull();
  });

  it("rejects backslash paths that would escape on Windows", async () => {
    const result = await resolveSourceFile("/..%5C..%5Cetc%5Cpasswd", [
      path.join(tmpDir, "public"),
    ]);
    expect(result).toBeNull();
  });

  it("refuses a symlink that escapes the serving root", async () => {
    const outside = path.join(tmpDir, "outside");
    fs.mkdirSync(outside, { recursive: true });
    const secret = path.join(outside, "secret.txt");
    fs.writeFileSync(secret, "secret");
    fs.symlinkSync(secret, path.join(tmpDir, "public", "linked.txt"));
    const result = await resolveSourceFile("/linked.txt", [
      path.join(tmpDir, "public"),
    ]);
    expect(result).toBeNull();
  });
});

describe("buildCacheKey", () => {
  it("returns deterministic hash", () => {
    const params = {
      src: "/photo.jpg",
      width: 640,
      quality: 75,
      format: "webp" as const,
    };
    const a = buildCacheKey(params);
    const b = buildCacheKey(params);
    expect(a).toBe(b);
    expect(a).toHaveLength(64); // sha256 hex
  });

  it("produces different keys for different params", () => {
    const a = buildCacheKey({
      src: "/photo.jpg",
      width: 640,
      quality: 75,
      format: "webp",
    });
    const b = buildCacheKey({
      src: "/photo.jpg",
      width: 320,
      quality: 75,
      format: "webp",
    });
    expect(a).not.toBe(b);
  });

  it("produces different keys for different formats", () => {
    const base = { src: "/photo.jpg", width: 640, quality: 75 };
    const a = buildCacheKey({ ...base, format: "webp" });
    const b = buildCacheKey({ ...base, format: "avif" });
    expect(a).not.toBe(b);
  });
});

describe("optimizeImage", () => {
  let tmpDir: string;
  let cacheDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "neutron-img-opt-"));
    cacheDir = path.join(tmpDir, "cache");
    fs.mkdirSync(path.join(tmpDir, "public"), { recursive: true });
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it("returns 404 for missing source", async () => {
    const result = await optimizeImage(
      { src: "/nope.jpg", width: 640, quality: 75, format: "webp" },
      { publicDirs: [path.join(tmpDir, "public")], cacheDir }
    );
    expect("error" in result).toBe(true);
    if ("error" in result) {
      expect(result.status).toBe(404);
    }
  });

  it("fails closed — a non-image source is never served raw (TS-13)", async () => {
    // The old fallback returned the original file through the image
    // endpoint, which made it an arbitrary-file reader for anything inside
    // the serving roots. Undecodable bytes must be a 415; a missing sharp
    // must be a 503. Neither may serve the source bytes.
    const content = Buffer.from("definitely-not-an-image");
    fs.writeFileSync(path.join(tmpDir, "public", "test.txt"), content);

    const result = await optimizeImage(
      { src: "/test.txt", width: 640, quality: 75, format: "webp" },
      { publicDirs: [path.join(tmpDir, "public")], cacheDir }
    );

    expect("error" in result).toBe(true);
    if ("error" in result) {
      expect([415, 503]).toContain(result.status);
    }
  });

  it("transforms a real image rather than echoing its bytes", async () => {
    fs.writeFileSync(path.join(tmpDir, "public", "one.png"), REAL_PNG);

    const result = await optimizeImage(
      { src: "/one.png", width: 16, quality: 75, format: "webp" },
      { publicDirs: [path.join(tmpDir, "public")], cacheDir }
    );

    if ("error" in result) {
      expect(result.status).toBe(503); // sharp unavailable in this checkout
    } else {
      // A real transform: webp bytes, never the raw PNG echoed back.
      expect(result.contentType).toBe("image/webp");
      expect(result.buffer.equals(REAL_PNG)).toBe(false);
      expect(result.buffer.length).toBeGreaterThan(0);
      // And the published cache entry exists (atomic publication path).
      expect(fs.existsSync(cacheDir)).toBe(true);
    }
  });
});

describe("handleImageRequest", () => {
  let tmpDir: string;
  let cacheDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "neutron-img-handle-"));
    cacheDir = path.join(tmpDir, "cache");
    fs.mkdirSync(path.join(tmpDir, "public"), { recursive: true });
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it("returns 400 for bad params", async () => {
    const request = new Request("http://localhost/_neutron/image?w=640");
    const response = await handleImageRequest(request, {
      publicDirs: [path.join(tmpDir, "public")],
      cacheDir,
    });
    expect(response.status).toBe(400);
  });

  it("returns 404 for missing source", async () => {
    const request = new Request(
      "http://localhost/_neutron/image?src=/nope.jpg&w=640"
    );
    const response = await handleImageRequest(request, {
      publicDirs: [path.join(tmpDir, "public")],
      cacheDir,
    });
    expect(response.status).toBe(404);
  });

  it("returns correct content-type and cache headers from a warmed cache entry", async () => {
    // Served from the cache, so the response-shape assertions do not depend
    // on sharp being installed.
    const params = { src: "/test.png", width: 640, quality: 75, format: "png" as const };
    const { buildCachePath } = await import("./image-optimizer.js");
    const cached = buildCachePath(cacheDir, params);
    fs.mkdirSync(path.dirname(cached), { recursive: true });
    fs.writeFileSync(cached, Buffer.from("cachedpng"));

    const request = new Request(
      "http://localhost/_neutron/image?src=/test.png&w=640&fmt=png"
    );
    const response = await handleImageRequest(request, {
      publicDirs: [path.join(tmpDir, "public")],
      cacheDir,
    });
    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("image/png");
    expect(response.headers.get("Cache-Control")).toBe(
      "public, max-age=31536000, immutable"
    );
    expect(Buffer.from(await response.arrayBuffer()).toString()).toBe("cachedpng");
  });

  it("returns 400 for path traversal", async () => {
    const request = new Request(
      "http://localhost/_neutron/image?src=/../../etc/passwd&w=640"
    );
    const response = await handleImageRequest(request, {
      publicDirs: [path.join(tmpDir, "public")],
      cacheDir,
    });
    expect(response.status).toBe(400);
  });
});

describe("remote image allowlist (fetch path)", () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "neutron-img-remote-"));

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  function imageFetchResponse(body: Uint8Array, contentType: string): Response {
    return new Response(body as unknown as BodyInit, {
      status: 200,
      headers: { "content-type": contentType },
    });
  }

  it("fetches an allowed remote image and serves it", async () => {
    const fetchMock = vi.fn(async (_url: string) => imageFetchResponse(REAL_PNG, "image/png"));
    vi.stubGlobal("fetch", fetchMock);

    const request = new Request(
      "http://localhost/_neutron/image?src=" +
        encodeURIComponent("https://cdn.example.com/photo.png") +
        "&w=640&fmt=png"
    );
    const response = await handleImageRequest(request, {
      publicDirs: [tmp],
      cacheDir: path.join(tmp, "cache"),
      remotePatterns: [{ hostname: "cdn.example.com" }],
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect((fetchMock.mock.calls[0][0] as string)).toBe(
      "https://cdn.example.com/photo.png"
    );
    // With sharp installed the transform runs and serves 200; without it the
    // endpoint fails closed (503). Either way the allowlist decided the fetch.
    expect([200, 503]).toContain(response.status);
    if (response.status === 200) {
      expect(response.headers.get("content-type")).toContain("image/");
    }
  });

  it("refuses a remote response that is not an image", async () => {
    const fetchMock = vi.fn(
      async () =>
        new Response("<html>login page</html>", {
          status: 200,
          headers: { "content-type": "text/html" },
        })
    );
    vi.stubGlobal("fetch", fetchMock);

    const request = new Request(
      "http://localhost/_neutron/image?src=" +
        encodeURIComponent("https://cdn.example.com/photo.png") +
        "&w=640"
    );
    const response = await handleImageRequest(request, {
      publicDirs: [tmp],
      cacheDir: path.join(tmp, "cache"),
      remotePatterns: [{ hostname: "cdn.example.com" }],
    });

    expect(response.status).toBe(415);
    const body = await response.text();
    expect(body).toContain("image");
  });

  it("never fetches when the hostname is unapproved", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);

    const request = new Request(
      "http://localhost/_neutron/image?src=" +
        encodeURIComponent("https://evil.example.com/photo.png") +
        "&w=640"
    );
    const response = await handleImageRequest(request, {
      publicDirs: [tmp],
      cacheDir: path.join(tmp, "cache"),
      remotePatterns: [{ hostname: "cdn.example.com" }],
    });

    expect(fetchMock).not.toHaveBeenCalled();
    expect(response.status).toBe(400);
    expect(await response.text()).toContain("remotePatterns");
  });
});
