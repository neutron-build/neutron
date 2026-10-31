import assert from "node:assert/strict";
import test from "node:test";
import { cleanupAfterFailure, ownedClose } from "./resources.js";

for (const companionFails of [false, true]) for (const sqlFails of [false, true]) {
  test(`all owners drain: companion failure=${companionFails}, SQL failure=${sqlFails}`, async () => {
    const companionError = new Error("companion close");
    const sqlError = new Error("SQL end");
    const primary = new Error("startup");
    let companion = 0; let sql = 0;
    const resources = [() => { companion++; if (companionFails) throw companionError; },
      async () => { sql++; if (sqlFails) throw sqlError; }];
    const failures = [...(companionFails ? [companionError] : []), ...(sqlFails ? [sqlError] : [])];
    await assert.rejects(() => cleanupAfterFailure(primary, resources), (error: any) => {
      if (!failures.length) assert.equal(error, primary);
      else { assert.equal(error.cause, primary); assert.deepEqual(error.errors, [primary, ...failures]); }
      return true;
    });
    assert.equal(companion, 1); assert.equal(sql, 1);
    const close = ownedClose(resources);
    const first = close(); assert.equal(close(), first);
    if (failures.length) await assert.rejects(() => first, (error: any) => { assert.deepEqual(error.errors, failures); return true; });
    else await first;
    assert.equal(companion, 2); assert.equal(sql, 2);
  });
}
