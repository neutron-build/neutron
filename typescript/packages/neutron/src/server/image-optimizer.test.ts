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

  it("finds file in first publicDir", () => {
    const result = resolveSourceFile("/photo.jpg", [
      path.join(tmpDir, "public"),
      path.join(tmpDir, "src"),
    ]);
    expect(result).toBe(path.join(tmpDir, "public", "photo.jpg"));
  });

  it("finds file in second publicDir", () => {
    const result = resolveSourceFile("/logo.png", [
      path.join(tmpDir, "public"),
      path.join(tmpDir, "src"),
    ]);
    expect(result).toBe(path.join(tmpDir, "src", "logo.png"));
  });

  it("returns null for missing file", () => {
    const result = resolveSourceFile("/missing.jpg", [
      path.join(tmpDir, "public"),
    ]);
    expect(result).toBeNull();
  });

  it("rejects path traversal attempts", () => {
    // Even if validateImageParams is bypassed, resolveSourceFile should catch this
    const result = resolveSourceFile("/../../../etc/passwd", [
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

  it("passes through original file when sharp is unavailable", async () => {
    const content = Buffer.from("fake-image-content");
    fs.writeFileSync(path.join(tmpDir, "public", "test.png"), content);

    const result = await optimizeImage(
      { src: "/test.png", width: 640, quality: 75, format: "webp" },
      { publicDirs: [path.join(tmpDir, "public")], cacheDir }
    );

    expect("buffer" in result).toBe(true);
    if ("buffer" in result) {
      expect(result.buffer).toEqual(content);
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

  it("returns correct content-type and cache headers", async () => {
    fs.writeFileSync(
      path.join(tmpDir, "public", "test.png"),
      Buffer.from("fakeimage")
    );

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
    const png = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 1, 2, 3]);
    const fetchMock = vi.fn(async (_url: string) => imageFetchResponse(png, "image/png"));
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
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toContain("image/");
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
