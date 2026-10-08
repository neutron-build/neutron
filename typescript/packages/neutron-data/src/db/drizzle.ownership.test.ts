import assert from "node:assert/strict";
import test from "node:test";
import { createDrizzleDatabase } from "./drizzle.js";

for (const ownership of ["borrowed", "owned"] as const) {
  test(`configured Nucleus companion ${ownership}: close drains SQL and preserves ownership`, async () => {
    const failure = new Error("companion close");
    let companionCloses = 0;
    const companion = { close: async () => { companionCloses++; throw failure; } };
    const database = await createDrizzleDatabase({ profile: { provider: "nucleus", connectionString: "postgres://unused.invalid/db" },
      nucleusCompanion: { client: companion, ownership } });
    assert.equal(database.nucleus, companion);
    let ends = 0;
    database.client.end = async () => { ends++; };
    if (ownership === "owned") await assert.rejects(() => database.close(), (error: any) => { assert.deepEqual(error.errors, [failure]); return true; });
    else await database.close();
    if (ownership === "owned") await assert.rejects(() => database.close()); else await database.close();
    assert.equal(ends, 1);
    assert.equal(companionCloses, ownership === "owned" ? 1 : 0);
  });
}
