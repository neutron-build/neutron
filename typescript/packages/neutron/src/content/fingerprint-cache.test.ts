import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

const statCalls = vi.hoisted(() => ({ count: 0 }));

vi.mock("node:fs/promises", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:fs/promises")>();
  return {
    ...actual,
    stat: async (...args: Parameters<typeof actual.stat>) => {
      statCalls.count += 1;
      return actual.stat(...args);
    },
  };
});

import { getCollection } from "./index.js";

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

describe("collection store fingerprinting", () => {
  it("does not re-stat every content file on every getCollection() call", { timeout: 30_000 }, async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "neutron-fingerprint-test-"));
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
    await fs.writeFile(
      path.join(root, "src", "content", "blog", "one.md"),
      `---\ntitle: One\n---\n\n# One\n`,
      "utf-8"
    );
    process.chdir(root);

    const first = await getCollection("blog");
    expect(first.length).toBe(1);

    // The fingerprint walk stats the config + every content file. A second
    // getCollection() in the same instant must reuse that walk: O(N) stat()
    // syscalls per SSR request on large collections is the bug.
    const statCallsAfterFirst = statCalls.count;
    const second = await getCollection("blog");
    expect(second.length).toBe(1);
    expect(statCalls.count).toBe(statCallsAfterFirst);
  });
});
