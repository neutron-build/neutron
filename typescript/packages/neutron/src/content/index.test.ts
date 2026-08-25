import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { h } from "preact";
import { renderToString } from "preact-render-to-string";
import {
  getCollection,
  getEntry,
  prepareContentCollections,
  renderEntry,
  setActiveMarkdownConfig,
  __renderCacheStatsForTest,
  __resetRenderCacheForTest,
} from "./index.js";

const tempRoots: string[] = [];
const originalCwd = process.cwd();

afterEach(async () => {
  process.chdir(originalCwd);
  while (tempRoots.length > 0) {
    const root = tempRoots.pop();
    if (!root) continue;
    await fs.rm(root, { recursive: true, force: true });
  }
});

describe("content collections", () => {
  it("loads markdown and data collections with validation", async () => {
    const root = await makeFixtureProject();
    process.chdir(root);

    const posts = await getCollection("blog");
    expect(posts.length).toBe(2);
    const hello = posts.find((post) => post.slug === "hello-world");
    expect(hello).toBeTruthy();
    expect(hello?.data).toMatchObject({ title: "Hello World", draft: false });
    // Bodies render lazily via render(): the eager `html` field is empty until
    // first render, so the rendered HTML for an entire collection is not pinned
    // in memory for the whole build. The raw body is always available.
    expect(hello?.html).toBe("");
    expect(hello?.body).toContain("# Hello world");

    const rendered = await hello!.render();
    const html = renderToString(h(rendered.Content, {}));
    expect(html).toContain("Hello world");
    expect(html).toContain("2"); // MDX expression output from fixture

    const author = await getEntry("authors", "jane");
    expect(author?.data).toMatchObject({ name: "Jane Doe" });

    const nested = await getEntry("blog", "guides/intro");
    expect(nested?.data).toMatchObject({ title: "Guides Intro", draft: false });
    const nestedRendered = await nested!.render();
    const nestedHtml = renderToString(h(nestedRendered.Content, {}));
    expect(nestedHtml).toContain("Nested guide");
  });

  it("assigns slugified, de-duplicated ids to plain-markdown headings", async () => {
    // The TOC sidebar (extractToc + slugify) links to "#<slug>" — without a
    // matching id on the actual heading element those links silently no-op.
    const root = await makeFixtureProject();
    process.chdir(root);

    const nested = await getEntry("blog", "guides/intro");
    const rendered = await nested!.render();
    const html = renderToString(h(rendered.Content, {}));

    expect(html).toContain('id="nested-guide"');
    expect(html).toContain('id="nested-guide-1"');
  });

  it("assigns slugified, de-duplicated ids to MDX headings", async () => {
    // MDX compiles through a separate path (compileMdx / @mdx-js/mdx) than plain
    // markdown, so it needs its own rehype-based id assignment. Neutron's own
    // docs site is 100% MDX, so this is the path its TOC actually depends on.
    const root = await makeFixtureProject();
    process.chdir(root);

    const entry = await getEntry("blog", "hello-world");
    const rendered = await entry!.render();
    const html = renderToString(h(rendered.Content, {}));

    expect(html).toContain('id="hello-world"');
    expect(html).toContain('id="details"');
    expect(html).toContain('id="details-1"');
  });

  it("writes manifest and type declarations during prepare", async () => {
    const root = await makeFixtureProject();
    await prepareContentCollections({ rootDir: root });

    const manifest = JSON.parse(
      await fs.readFile(path.join(root, "dist", ".neutron-content.json"), "utf-8")
    ) as { collections: Record<string, unknown> };
    expect(manifest.collections.blog).toBeTruthy();

    const types = await fs.readFile(
      path.join(root, "src", "content", ".neutron-content.d.ts"),
      "utf-8"
    );
    expect(types).toContain('interface ContentCollectionMap');
    expect(types).toContain('"blog"');
    expect(types).toContain('"summary"?: string;');
  });

  it("surfaces contextual MDX compile errors on render", async () => {
    const root = await makeBrokenMdxFixtureProject();
    process.chdir(root);

    // Body rendering is lazy, so a broken MDX file loads successfully (its
    // frontmatter is valid) and the compile error surfaces when render() runs —
    // wrapped in the same collection context as the old eager path.
    const posts = await getCollection("blog");
    const broken = posts.find((post) => post.slug === "broken");
    expect(broken).toBeTruthy();

    await expect(broken!.render()).rejects.toThrow(
      '[content:blog] Failed to render content entry for "broken.mdx": MDX compilation failed in "broken.mdx"'
    );
  });

  it("surfaces unsupported data extension errors with context", async () => {
    const root = await makeUnsupportedDataFixtureProject();
    process.chdir(root);

    await expect(getCollection("settings")).rejects.toThrow(
      '[content:settings] Failed to parse or validate data entry for "flags.md": Unsupported data file extension ".md"'
    );
  });

  it("collection entries are serializable by devalue", async () => {
    const root = await makeFixtureProject();
    process.chdir(root);
    const posts = await getCollection("blog");
    const { stringify } = await import("devalue");
    // Should not throw "Cannot stringify a function"
    expect(() => stringify(posts)).not.toThrow();
    // render() should still be accessible
    expect(typeof posts[0].render).toBe("function");
  });

  it("loads collections config from TypeScript file", async () => {
    const root = await makeTypeScriptConfigFixtureProject();
    process.chdir(root);

    const posts = await getCollection("blog");
    expect(posts.length).toBe(1);
    expect(posts[0]?.data).toMatchObject({ title: "Typed Config" });
  });

  it("renders identical content once and serves repeats from the render cache", async () => {
    const root = await makeDuplicateContentFixtureProject();
    process.chdir(root);
    // Fresh config reference resets both the store and render caches.
    setActiveMarkdownConfig({});
    __resetRenderCacheForTest();

    const posts = await getCollection("blog");
    expect(posts.length).toBe(2);

    // First render compiles the body exactly once.
    await posts[0]!.render();
    expect(__renderCacheStatsForTest()).toMatchObject({ misses: 1, size: 1 });

    // The second entry has byte-identical content → content-addressed cache
    // hit, no recompile (the dedup that also serves SSR hot paths).
    await posts[1]!.render();
    expect(__renderCacheStatsForTest()).toMatchObject({ misses: 1, size: 1 });

    // Re-rendering the same entry (the per-request SSR path) reuses the cache
    // rather than recompiling — the regression guard for the lazy-render change.
    await posts[0]!.render();
    expect(__renderCacheStatsForTest().misses).toBe(1);
  });

  it("drops the render cache when the markdown config changes", async () => {
    const root = await makeFixtureProject();
    process.chdir(root);
    setActiveMarkdownConfig({});
    __resetRenderCacheForTest();

    const posts = await getCollection("blog");
    await posts[0]!.render();
    expect(__renderCacheStatsForTest().size).toBe(1);

    // A new config reference can change rendered output (themes/plugins), so the
    // content-addressed cache — whose key omits the config — must be dropped.
    setActiveMarkdownConfig({});
    expect(__renderCacheStatsForTest().size).toBe(0);
  });

  it("renderCacheSize: 0 disables the render cache", async () => {
    const root = await makeDuplicateContentFixtureProject();
    process.chdir(root);
    setActiveMarkdownConfig({ renderCacheSize: 0 });
    __resetRenderCacheForTest();

    const posts = await getCollection("blog");
    await posts[0]!.render();
    await posts[0]!.render();

    // No caching: every render recompiles, nothing retained.
    expect(__renderCacheStatsForTest()).toMatchObject({ misses: 2, size: 0 });
  });

  it("setActiveMarkdownConfig is idempotent for reference-identical config", async () => {
    // Regression: layouts and SSR-bootstrap files call this at module-load
    // time. Every HMR pass was wiping the content cache, forcing a full
    // re-parse of every collection file (multi-GB churn on large projects).
    const root = await makeFixtureProject();
    process.chdir(root);
    setActiveMarkdownConfig(undefined);

    const config = {};
    setActiveMarkdownConfig(config as never);
    const first = await getCollection("blog");

    setActiveMarkdownConfig(config as never);
    const second = await getCollection("blog");

    expect(second).toBe(first);

    setActiveMarkdownConfig({} as never);
    const third = await getCollection("blog");
    expect(third).not.toBe(first);
    expect(third.length).toBe(first.length);

    setActiveMarkdownConfig(undefined);
  });

  it("renderEntry returns rendered HTML for a markdown entry without memoizing onto the entry", async () => {
    const root = await makeFixtureProject();
    process.chdir(root);

    const nested = await getEntry("blog", "guides/intro");
    expect(nested).toBeTruthy();

    // Sanity: the entry starts with an empty html field (lazy rendering).
    expect(nested!.html).toBe("");

    const { html } = await renderEntry(nested!);
    // Real markup — the body's heading and its slugified id are present.
    expect(html).toContain("Nested guide");
    expect(html).toContain('id="nested-guide"');

    // The contract from N-006: renderEntry does NOT memoize onto the entry.
    // A static build relies on this so the whole collection's HTML does not
    // re-accumulate in memory. The eager `html` field stays empty.
    expect(nested!.html).toBe("");

    // Re-rendering still works and returns identical markup — the bounded
    // content-addressed render cache serves it, not the entry.
    const again = await renderEntry(nested!);
    expect(again.html).toBe(html);
  });

  it("renderEntry returns rendered HTML for an MDX entry", async () => {
    const root = await makeFixtureProject();
    process.chdir(root);

    const hello = await getEntry("blog", "hello-world");
    expect(hello).toBeTruthy();
    expect(hello!.html).toBe("");

    const { html } = await renderEntry(hello!);
    // MDX expression output ({2}) is evaluated, and slugified heading ids
    // match the plain-markdown path.
    expect(html).toContain("Hello world");
    expect(html).toContain("2");
    expect(html).toContain('id="hello-world"');

    // Same no-memoization contract.
    expect(hello!.html).toBe("");
  });

  it("sanitizes untrusted HTML through the config path when sanitize: true", async () => {
    // The config loader used to rebuild collection definitions with only
    // `type` + `schema`, silently dropping `sanitize` — so a collection
    // declared untrusted still rendered raw CMS HTML (the markup that lands
    // in dangerouslySetInnerHTML) with no sanitization.
    const root = await makeUntrustedHtmlFixtureProject();
    process.chdir(root);

    const entry = await getEntry("cms", "malicious");
    expect(entry).toBeTruthy();
    expect(entry?.sanitize).toBe(true);

    const { html } = await renderEntry(entry!);
    expect(html).toContain("<p>legit markup</p>");
    expect(html).not.toContain("<script>");
    expect(html).not.toContain("onerror");
  });

  it("renderEntry output matches the docs template's loader data shape", async () => {
    // Type-level check that the data shape produced by switching the docs
    // template's loader to renderEntry() — `const { html } = await renderEntry(entry)`
    // — is assignable to the `html: string` field the template's component
    // declares on its `data` prop. This is the regression guard for N-003:
    // the template's data shape must still type-check after the switch.
    const root = await makeFixtureProject();
    process.chdir(root);
    const entry = await getEntry("blog", "guides/intro");
    const { html } = await renderEntry(entry!);

    // Mirrors the relevant slice of DocPage's `data` prop type.
    type DocPageData = { title: string; html: string; slug: string };
    const data: DocPageData = {
      title: (entry!.data as { title: string }).title,
      html,
      slug: entry!.slug,
    };
    expect(data.html).toBe(html);
    expect(typeof data.html).toBe("string");
  });
});

async function makeFixtureProject(): Promise<string> {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "neutron-content-test-"));
  tempRoots.push(root);

  await fs.mkdir(path.join(root, "src", "content", "blog"), { recursive: true });
  await fs.mkdir(path.join(root, "src", "content", "authors"), { recursive: true });

  await fs.writeFile(
    path.join(root, "src", "content", "config.js"),
    `
import { z } from "zod";

export const collections = {
  blog: {
    schema: z.object({
      title: z.string(),
      summary: z.string().optional(),
      draft: z.boolean().default(false),
    }),
  },
  authors: {
    type: "data",
    schema: z.object({
      name: z.string(),
      bio: z.string(),
    }),
  },
};
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(root, "src", "content", "blog", "hello-world.mdx"),
    `---
title: Hello World
---

# Hello world

2 + 0 = {2}

This is a test post.

## Details

## Details
`,
    "utf-8"
  );
  await fs.mkdir(path.join(root, "src", "content", "blog", "guides"), { recursive: true });
  await fs.writeFile(
    path.join(root, "src", "content", "blog", "guides", "intro.md"),
    `---
title: Guides Intro
---

# Nested guide

## Nested guide
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(root, "src", "content", "authors", "jane.yaml"),
    `
name: Jane Doe
bio: Writes docs
`,
    "utf-8"
  );

  return root;
}

async function makeDuplicateContentFixtureProject(): Promise<string> {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "neutron-content-dup-"));
  tempRoots.push(root);

  await fs.mkdir(path.join(root, "src", "content", "blog"), { recursive: true });
  await fs.writeFile(
    path.join(root, "src", "content", "config.js"),
    `
import { z } from "zod";
export const collections = {
  blog: {
    schema: z.object({ title: z.string() }),
  },
};
`,
    "utf-8"
  );

  // Two entries whose rendered BODY is byte-identical (only frontmatter
  // differs) — the content-addressed key must treat them as one render.
  const body = `
# Shared body

The same prose in both posts.
`;
  await fs.writeFile(
    path.join(root, "src", "content", "blog", "first.md"),
    `---\ntitle: First\n---\n${body}`,
    "utf-8"
  );
  await fs.writeFile(
    path.join(root, "src", "content", "blog", "second.md"),
    `---\ntitle: Second\n---\n${body}`,
    "utf-8"
  );

  return root;
}

async function makeBrokenMdxFixtureProject(): Promise<string> {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "neutron-content-mdx-error-"));
  tempRoots.push(root);

  await fs.mkdir(path.join(root, "src", "content", "blog"), { recursive: true });
  await fs.writeFile(
    path.join(root, "src", "content", "config.js"),
    `
import { z } from "zod";
export const collections = {
  blog: {
    schema: z.object({
      title: z.string(),
    }),
  },
};
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(root, "src", "content", "blog", "broken.mdx"),
    `---
title: Broken
---

# broken

{1 + }
`,
    "utf-8"
  );

  return root;
}

async function makeUnsupportedDataFixtureProject(): Promise<string> {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "neutron-content-data-error-"));
  tempRoots.push(root);

  await fs.mkdir(path.join(root, "src", "content", "settings"), { recursive: true });
  await fs.writeFile(
    path.join(root, "src", "content", "config.js"),
    `
import { z } from "zod";
export const collections = {
  settings: {
    type: "data",
    schema: z.object({
      enabled: z.boolean().optional(),
    }),
  },
};
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(root, "src", "content", "settings", "flags.md"),
    `enabled: true`,
    "utf-8"
  );

  return root;
}

async function makeUntrustedHtmlFixtureProject(): Promise<string> {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "neutron-content-untrusted-"));
  tempRoots.push(root);

  await fs.mkdir(path.join(root, "src", "content", "cms"), { recursive: true });
  await fs.writeFile(
    path.join(root, "src", "content", "config.js"),
    `
import { z } from "zod";

export const collections = {
  cms: {
    schema: z.object({
      title: z.string(),
    }),
    sanitize: true,
  },
};
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(root, "src", "content", "cms", "malicious.html"),
    `---
title: Malicious
---

<p>legit markup</p>
<script>alert(1)</script>
<img src="x" onerror="alert(2)">
`,
    "utf-8"
  );

  return root;
}

async function makeTypeScriptConfigFixtureProject(): Promise<string> {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "neutron-content-ts-config-"));
  tempRoots.push(root);

  await fs.mkdir(path.join(root, "src", "content", "blog"), { recursive: true });
  await fs.writeFile(
    path.join(root, "src", "content", "config.ts"),
    `
import { defineCollection, z } from "@neutron-build/core/content";

export const collections = {
  blog: defineCollection({
    schema: z.object({
      title: z.string(),
    }),
  }),
};
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(root, "src", "content", "blog", "typed.md"),
    `---
title: Typed Config
---

# Typed Config
`,
    "utf-8"
  );

  return root;
}
