import { expect, it } from "vitest";
import { beginCacheMutation } from "./cache-mutation.js";

it("TS-F05 invalidates every store in both phases and completion is idempotent", async () => {
  const calls: string[] = [];
  const stores = [0, 1].map(index => ({ async deleteByPath(path: string) { calls.push(`${index}:${path}`); } }));
  const finish = await beginCacheMutation(true, "/literal%value", stores);
  await Promise.all([finish(), finish()]);
  expect(calls).toEqual(["0:/literal%25value", "1:/literal%25value", "0:/literal%25value", "1:/literal%25value"]);
});
it("TS-F05 failing cache invalidation still attempts all backing stores", async () => {
  let attempted = false;
  await expect(beginCacheMutation(true, "/", [
    { async deleteByPath() { throw new Error("backing unavailable"); } },
    { async deleteByPath() { attempted = true; } },
  ])).rejects.toThrow("backing unavailable");
  expect(attempted).toBe(true);
});
