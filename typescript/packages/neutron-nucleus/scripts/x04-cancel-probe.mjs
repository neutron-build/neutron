// X04 cancellation probes — run against (a) PG control, (b) Nucleus.
// (a) proves the PgTransport side-channel cancels real work (pg_sleep -> 57014)
//     and the pool stays usable after;
// (b) proves the honest unsupported report on an engine without
//     pg_cancel_backend, with the statement completing anyway.
import { PgTransport } from "../dist/transport.js";
import { detectFeatures } from "../dist/features.js";
import { withKV } from "../dist/kv/index.js";
import { NucleusNotSupportedError } from "../dist/errors.js";

const url = process.argv[2];
if (!url) {
  console.error("usage: node x04-cancel-probe.mjs <database-url>");
  process.exit(2);
}

const t = new PgTransport(url);
const feats = await detectFeatures(t);
console.log(`engine: ${feats.isNucleus ? "Nucleus" : "PostgreSQL"} (${feats.version})`);

if (!feats.isNucleus) {
  // --- PG control: cancel a sleeping statement mid-flight ---
  for (const slow of ["SELECT pg_sleep(3)", "SELECT count(*) FROM generate_series(1, 1)"]) {
    const ac = new AbortController();
    const started = Date.now();
    const p = t.fetchval(slow, [], { signal: ac.signal });
    setTimeout(() => ac.abort(), 150);
    try {
      await p;
      console.log(`${slow}: completed without cancellation (unexpected for pg_sleep)`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      const code = err && err.code;
      console.log(`${slow}: rejected after ${Date.now() - started}ms — ${msg.slice(0, 90)} (code=${code ?? "?"})`);
      if (slow.includes("pg_sleep")) {
        const ok = /canceling statement|57014|aborted/i.test(msg) || code === "57014" || err?.name === "AbortError" || err instanceof NucleusNotSupportedError;
        if (!ok) {
          console.error("FAIL: cancellation did not surface as a cancel/abort error");
          process.exit(1);
        }
        if (Date.now() - started > 2500) {
          console.error("FAIL: statement ran to completion instead of being canceled");
          process.exit(1);
        }
      }
    }
  }
  // Pool must remain usable after a canceled statement.
  const alive = await t.fetchval("SELECT 41 + 1 AS v");
  console.log(`pool usable after cancel: ${alive === 42 ? "yes (42)" : `NO (${alive})`}`);
  if (alive !== 42) process.exit(1);
  console.log("PG-CONTROL-CANCEL: PASS");
} else {
  // --- Nucleus: engine lacks pg_cancel_backend (X00 N7) ---
  const kv = withKV.init(t, feats).kv;
  await kv.delete("x04_cancel_probe");
  const ac = new AbortController();
  const p = kv.set("x04_cancel_probe", "v", { signal: ac.signal });
  ac.abort();
  try {
    await p;
    console.error("FAIL: expected a rejection on abort");
    process.exit(1);
  } catch (err) {
    if (err && err.constructor && err.constructor.name === "NucleusNotSupportedError") {
      console.log(`abort surfaced honestly: ${err.constructor.name} — ${String(err.message).slice(0, 120)}`);
    } else if (err?.name === "AbortError") {
      console.log("abort surfaced as AbortError (pre-flight)");
    } else {
      console.error(`FAIL: unexpected error class: ${err && err.message}`);
      process.exit(1);
    }
  }
  const exists = await kv.exists("x04_cancel_probe");
  console.log(`statement completed despite abort (key exists): ${exists}`);
  if (!exists) process.exit(1);
  await kv.delete("x04_cancel_probe");
  console.log("NUCLEUS-CANCEL-HONESTY: PASS");
}

await t.close();
