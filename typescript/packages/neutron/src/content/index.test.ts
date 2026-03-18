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
    expect(hello?.html).toContain("<h1>Hello world");

    const rendered = await hello!.render();
    const html = renderToString(h(rendered.Content, {}));
    expect(html).toContain("Hello world");
    expect(html).toContain("2"); // MDX expression output from fixture

    const author = await getEntry("authors", "jane");
    expect(author?.data).toMatchObject({ name: "Jane Doe" });

    const nested = await getEntry("blog", "guides/intro");
    expect(nested?.data).toMatchObject({ title: "Guides Intro", draft: false });
    expect(nested?.html).toContain("Nested guide");
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

  it("surfaces contextual MDX compile errors", async () => {
    const root = await makeBrokenMdxFixtureProject();
    process.chdir(root);

    await expect(getCollection("blog")).rejects.toThrow(
      '[content:blog] Failed to parse, validate, or render content entry for "broken.mdx": MDX compilation failed in "broken.mdx"'
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

async function makeTypeScriptConfigFixtureProject(): Promise<string> {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "neutron-content-ts-config-"));
  tempRoots.push(root);

  await fs.mkdir(path.join(root, "src", "content", "blog"), { recursive: true });
  await fs.writeFile(
    path.join(root, "src", "content", "config.ts"),
    `
import { defineCollection, z } from "neutron/content";

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
