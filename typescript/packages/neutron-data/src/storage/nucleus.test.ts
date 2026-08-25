import assert from "node:assert/strict";
import test from "node:test";
import { NucleusStorageDriver, type NucleusBlobLike } from "./nucleus.js";

test("NucleusStorageDriver.get preserves the stored contentType", async () => {
  const stored: Array<{ key: string; contentType?: string }> = [];
  const blob: NucleusBlobLike = {
    put: async (_bucket, key, _data, opts) => {
      stored.push({ key, contentType: opts?.contentType });
    },
    get: async (_bucket, key) => {
      const entry = stored.find((item) => item.key === key);
      if (!entry) return null;
      return {
        data: new TextEncoder().encode("payload"),
        meta: { contentType: entry.contentType },
      };
    },
    delete: async () => true,
  };
  const driver = new NucleusStorageDriver({ blob });

  await driver.put({
    key: "img/logo.png",
    body: new TextEncoder().encode("payload"),
    contentType: "image/png",
  });

  const object = await driver.get("img/logo.png");
  assert.ok(object);
  // The S3 driver round-trips contentType; the Nucleus driver discarded the
  // blob metadata, so a put-then-get lost the content type.
  assert.equal(object.contentType, "image/png");
});
