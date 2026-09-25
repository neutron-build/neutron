// X05 Nucleus leg — streams/CDC/PubSub/Datalog/LISTEN semantics against the
// REAL engine build (never mocked): round-trips, ordering, resume cursors,
// duplicate delivery, acknowledgement, restart/disconnect recovery and the
// RLS policy boundary for CDC exploration. Every verdict is recorded; the
// engine is hard-killed (SIGKILL) for restart cases so WAL replay — not a
// graceful checkpoint — is what is measured. No processes or data dirs are
// left behind.
//
// Usage: node conformance/live/orm/x05-nucleus-leg.mjs <path-to-nucleus-binary> [out.json]
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { createRequire } from "node:module";

const BIN = process.argv[2];
if (!BIN) {
  console.error("usage: node x05-nucleus-leg.mjs <nucleus-binary> [out.json]");
  process.exit(2);
}
const OUT = process.argv[3] || "x05-nucleus.json";
const PORT = 55932;
const TAG = `x05_${Date.now().toString(36)}`;

const require_ = createRequire(
  new URL("../../../typescript/packages/neutron-sql/dist/index.js", import.meta.url),
);
const pg = require_("pg");
// The listen-notify module under test (built dist — this leg is its only
// Nucleus coverage; the sql live battery is PostgreSQL-only).
const { pgListener } = require_("./listen-notify.js");

const results = [];
function record(id, pass, detail) {
  results.push({ id, pass, detail });
  console.log(`${pass ? "PASS" : "FAIL"} ${id} :: ${detail}`);
}
async function check(id, fn) {
  try {
    const detail = await fn();
    record(id, true, detail ?? "");
  } catch (err) {
    record(id, false, err instanceof Error ? err.message : String(err));
  }
}

let child = null;
let dataDir = null;
const log = { text: "" };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function startEngine() {
  child = spawn(BIN, ["start", "--port", String(PORT), "--data", dataDir], {
    env: { ...process.env },
    stdio: ["ignore", "pipe", "pipe"],
  });
  child.stdout.on("data", (d) => (log.text += d));
  child.stderr.on("data", (d) => (log.text += d));
  for (let i = 0; i < 90; i++) {
    await sleep(400);
    try {
      const c = new pg.Client({ connectionString: url() });
      await c.connect();
      await c.end();
      return;
    } catch {
      /* not up yet */
    }
  }
  throw new Error(`engine did not come up; log tail: ${log.text.slice(-400)}`);
}

const url = () => `postgres://localhost:${PORT}/postgres`;
const clients = new Set();
function client() {
  const c = new pg.Client({ connectionString: url() });
  c.on("error", () => { /* restarts kill sockets; not a crash */ });
  clients.add(c);
  return c;
}
async function q(c, sql, params) {
  const r = await c.query(sql, params);
  return r.rows;
}
async function restartEngine() {
  for (const c of clients) {
    try { await c.end(); } catch { /* dying */ }
  }
  clients.clear();
  child.kill("SIGKILL");
  await sleep(600);
  await startEngine();
}

const out = { started: false };
try {
  dataDir = mkdtempSync(path.join(tmpdir(), "x05-nucleus-"));
  await startEngine();
  out.started = true;

  let c1 = client();
  await c1.connect();
  out.engine = (await q(c1, "SELECT VERSION()"))[0].version;
  const reopen = async () => {
    c1 = client();
    await c1.connect();
  };

  // ======================================================================
  // STREAMS
  // ======================================================================
  await check("streams.roundtrip_ordering", async () => {
    const ids = [];
    for (let i = 0; i < 6; i++) {
      ids.push((await q(c1, "SELECT STREAM_XADD($1,$2,$3) AS id", [`${TAG}_s`, "n", String(i)]))[0].id);
    }
    for (let i = 1; i < ids.length; i++) {
      const [a, b] = [ids[i - 1], ids[i]].map((x) => x.split("-").map(Number));
      if (b[0] < a[0] || (b[0] === a[0] && b[1] <= a[1])) throw new Error(`ids not strictly increasing: ${ids[i - 1]} -> ${ids[i]}`);
    }
    const len = Number((await q(c1, "SELECT STREAM_XLEN($1) AS n", [`${TAG}_s`]))[0].n);
    const all = JSON.parse((await q(c1, "SELECT STREAM_XRANGE($1,$2,$3,$4) AS r", [`${TAG}_s`, 0, 9999999999999, 100]))[0].r);
    const fieldsOk = all.every((e, i) => e.fields.n === String(i));
    return `xadd->6 strictly-increasing ids, xlen=${len}, xrange field order exact=${fieldsOk}`;
  });

  await check("streams.resume_cursor_full_id_vs_bare_ms", async () => {
    // TRUE engine semantics (engine scalar_fns.rs stream_cursor_arg + its
    // probe_streams_oracle, forced live here): a bare millisecond cursor —
    // number OR string — means STRICTLY AFTER that whole millisecond.
    // Same-millisecond entries appended after the last one read are silently
    // SKIPPED (a loss hazard, NOT re-served duplicates). Sequential xadds
    // never share a millisecond, so the same-ms pair is FORCED with
    // concurrent connections; this check fails loudly if the engine changes
    // either cursor form.
    const stream = `${TAG}_cur`;
    const conns = [];
    for (let i = 0; i < 6; i++) {
      const c = client();
      await c.connect();
      conns.push(c);
    }
    const ids = [];
    try {
      for (let round = 0; round < 60; round++) {
        const batch = await Promise.all(
          conns.map((c, i) => q(c, "SELECT STREAM_XADD($1,$2,$3) AS id", [stream, "k", `f${round}_${i}`])),
        );
        ids.push(...batch.map((r) => r[0].id));
        if (batch.some((r) => Number(r[0].id.split("-")[1]) > 0)) break;
      }
    } finally {
      for (const c of conns) await c.end().catch(() => {});
    }
    const byMs = new Map();
    for (const id of ids) {
      const ms = id.split("-")[0];
      if (!byMs.has(ms)) byMs.set(ms, []);
      byMs.get(ms).push(id);
    }
    let pairMs = null;
    for (const [ms, list] of byMs) {
      if (list.length >= 2) {
        pairMs = Number(ms);
        break;
      }
    }
    if (pairMs === null) throw new Error(`could not force a same-ms pair (${ids.length} concurrent adds, 60 rounds)`);
    const same = [...byMs.get(String(pairMs))].sort();
    const A = same[0];
    const B = same[1];
    // An entry strictly after the pair's whole millisecond.
    let C = null;
    for (let i = 0; i < 50; i++) {
      C = (await q(c1, "SELECT STREAM_XADD($1,$2,$3) AS id", [stream, "k", "tail" + i]))[0].id;
      if (Number(C.split("-")[0]) > pairMs) break;
    }
    if (Number(C.split("-")[0]) <= pairMs) throw new Error("could not append a later-ms entry");
    const xread = async (cursor) =>
      JSON.parse((await q(c1, "SELECT STREAM_XREAD($1,$2,$3) AS r", [stream, cursor, 1000]))[0].r);
    const full = await xread(A);
    const bareNum = await xread(pairMs);
    const bareStr = await xread(String(pairMs));
    if (!full.some((e) => e.id === B) || !full.some((e) => e.id === C)) {
      throw new Error(`full-id cursor missed entries: ${JSON.stringify(full.map((e) => e.id))}`);
    }
    for (const [label, bare] of [["number", bareNum], ["string", bareStr]]) {
      const sameMsLeak = bare.filter((e) => Number(e.id.split("-")[0]) === pairMs);
      if (sameMsLeak.length > 0) throw new Error(`bare ${label} cursor served same-ms entries (duplicate semantics): ${JSON.stringify(sameMsLeak.map((e) => e.id))}`);
      if (!bare.some((e) => e.id === C)) throw new Error(`bare ${label} cursor missed the later-ms entry`);
    }
    return `forcedPair=${A}->${B} siblings=${same.length} bareNum/bareStr skip same-ms (loss hazard), full id serves all — bare ms = strictly after the whole millisecond`;
  });

  await check("streams.nogroup_and_busygroup_errors", async () => {
    let nogroup = "";
    try {
      await q(c1, "SELECT STREAM_XREADGROUP($1,$2,$3,$4)", [`${TAG}_s`, "ghost", "c", 10]);
      throw new Error("xreadgroup on missing group did not error");
    } catch (e) {
      nogroup = `${e.code}:${String(e.message).slice(0, 40)}`;
    }
    await q(c1, "SELECT STREAM_XGROUP_CREATE($1,$2,$3)", [`${TAG}_s`, `${TAG}_g`, 0]);
    let busy = "";
    try {
      await q(c1, "SELECT STREAM_XGROUP_CREATE($1,$2,$3)", [`${TAG}_s`, `${TAG}_g`, 0]);
      throw new Error("duplicate xgroup_create did not error");
    } catch (e) {
      busy = `${e.code}:${String(e.message).slice(0, 30)}`;
    }
    if (!nogroup.includes("22000")) throw new Error(`NOGROUP not 22000: ${nogroup}`);
    if (!busy.includes("23000")) throw new Error(`BUSYGROUP not 23000: ${busy}`);
    return `NOGROUP=${nogroup} BUSYGROUP=${busy}`;
  });

  let firstDelivery = [];
  await check("streams.group_at_most_once_ack", async () => {
    firstDelivery = JSON.parse((await q(c1, "SELECT STREAM_XREADGROUP($1,$2,$3,$4) AS r", [`${TAG}_s`, `${TAG}_g`, "consumer-1", 4]))[0].r);
    if (firstDelivery.length !== 4) throw new Error(`expected 4 delivered, got ${firstDelivery.length}`);
    const acked = Number((await q(c1, "SELECT STREAM_XACK($1,$2,$3) AS n", [`${TAG}_s`, `${TAG}_g`, firstDelivery[0].id]))[0].n);
    if (acked !== 1) throw new Error(`xack of delivered entry returned ${acked}`);
    const again = JSON.parse((await q(c1, "SELECT STREAM_XREADGROUP($1,$2,$3,$4) AS r", [`${TAG}_s`, `${TAG}_g`, "consumer-1", 10]))[0].r);
    if (again.some((e) => e.id === firstDelivery[0].id)) throw new Error("acked entry redelivered (exactly-once violation both ways)");
    if (again.length !== 2) throw new Error(`expected remaining 2 entries, got ${again.length}`);
    const reack = Number((await q(c1, "SELECT STREAM_XACK($1,$2,$3) AS n", [`${TAG}_s`, `${TAG}_g`, firstDelivery[0].id]))[0].n);
    if (reack !== 0) throw new Error(`re-ack of already-acked entry returned ${reack}`);
    return `delivered=4 ack=1 nextRead=${again.length} reack=0 (at-most-once; ack idempotent-reporting 0)`;
  });

  await check("streams.rollback_removes_pending_append", async () => {
    const before = Number((await q(c1, "SELECT STREAM_XLEN($1) AS n", [`${TAG}_rb`]))[0].n);
    await c1.query("BEGIN");
    await q(c1, "SELECT STREAM_XADD($1,$2,$3)", [`${TAG}_rb`, "k", "in-tx"]);
    const inTx = Number((await c1.query("SELECT STREAM_XLEN($1) AS n", [`${TAG}_rb`])).rows[0].n);
    await c1.query("ROLLBACK");
    const after = Number((await q(c1, "SELECT STREAM_XLEN($1) AS n", [`${TAG}_rb`]))[0].n);
    if (after !== before) throw new Error(`rollback left the append: before=${before} after=${after}`);
    return `before=${before} inTx=${inTx} after=${after} (session-scoped rollback holds)`;
  });

  await check("streams.restart_entries_group_cursor_ack", async () => {
    // Fresh group so the delivery state at restart is exactly: 6 entries,
    // group cursor after 4, entry 1 acked, entries 2-4 pending, 5-6 never
    // delivered. (The at-most-once check above consumed its group's tail.)
    await q(c1, "SELECT STREAM_XGROUP_CREATE($1,$2,$3)", [`${TAG}_s`, `${TAG}_g2`, 0]);
    const first = JSON.parse((await q(c1, "SELECT STREAM_XREADGROUP($1,$2,$3,$4) AS r", [`${TAG}_s`, `${TAG}_g2`, "consumer-1", 4]))[0].r);
    if (first.length !== 4) throw new Error(`expected 4 delivered pre-restart, got ${first.length}`);
    const acked = Number((await q(c1, "SELECT STREAM_XACK($1,$2,$3) AS n", [`${TAG}_s`, `${TAG}_g2`, first[0].id]))[0].n);
    if (acked !== 1) throw new Error(`pre-restart ack returned ${acked}`);
    await restartEngine();
    const c = client();
    await c.connect();
    const len = Number((await q(c, "SELECT STREAM_XLEN($1) AS n", [`${TAG}_s`]))[0].n);
    if (len !== 6) throw new Error(`entries lost across restart: xlen=${len}`);
    const delivered = JSON.parse((await q(c, "SELECT STREAM_XREADGROUP($1,$2,$3,$4) AS r", [`${TAG}_s`, `${TAG}_g2`, "consumer-1", 10]))[0].r);
    if (delivered.length !== 2) throw new Error(`expected the 2 undelivered entries after restart, got ${delivered.length}`);
    const beforeIds = new Set(first.map((e) => e.id));
    const reServed = delivered.filter((e) => beforeIds.has(e.id));
    if (reServed.length > 0) {
      throw new Error(`post-restart delivery re-served consumed entries: ${JSON.stringify(reServed.map((e) => e.id))}`);
    }
    const reack = Number((await q(c, "SELECT STREAM_XACK($1,$2,$3) AS n", [`${TAG}_s`, `${TAG}_g2`, first[0].id]))[0].n);
    if (reack !== 0) throw new Error(`ack not durable across restart: re-ack returned ${reack}`);
    await c.end();
    return `xlen=6 groupSurvived=1 undeliveredBacklogDelivered=${delivered.length} ackDurable=yes`;
  });
  await reopen();

  await check("streams.disconnect_recovery_cursor_resume", async () => {
    // A consumer that drops its connection and reconnects resumes from its
    // OWN cursor (xread with the last seen full id) — streams are global,
    // not session-scoped.
    const c = client();
    await c.connect();
    const lastId = (await q(c, "SELECT STREAM_XADD($1,$2,$3) AS id", [`${TAG}_disc`, "k", "one"]))[0].id;
    await q(c, "SELECT STREAM_XADD($1,$2,$3)", [`${TAG}_disc`, "k", "two"]);
    await c.end(); // disconnect
    const c2 = client();
    await c2.connect(); // reconnect
    const resumed = JSON.parse((await q(c2, "SELECT STREAM_XREAD($1,$2,$3) AS r", [`${TAG}_disc`, lastId, 10]))[0].r);
    await c2.end();
    if (resumed.length !== 1 || resumed[0].fields.k !== "two") {
      throw new Error(`resume after disconnect wrong: ${JSON.stringify(resumed)}`);
    }
    return "reconnect + full-id cursor sees exactly the post-disconnect entry";
  });

  // ======================================================================
  // CDC
  // ======================================================================
  await q(c1, `CREATE TABLE ${TAG}_cdc (id INT PRIMARY KEY, v TEXT)`);
  let cdcBase = 0;
  await check("cdc.delivery_shape", async () => {
    cdcBase = Number((await q(c1, "SELECT CDC_COUNT() AS n"))[0].n);
    await q(c1, `INSERT INTO ${TAG}_cdc VALUES (1, 'a')`);
    await q(c1, `UPDATE ${TAG}_cdc SET v = 'b' WHERE id = 1`);
    await q(c1, `DELETE FROM ${TAG}_cdc WHERE id = 1`);
    await q(c1, `INSERT INTO ${TAG}_cdc VALUES (10, 'x'), (11, 'y'), (12, 'z')`);
    const evs = JSON.parse((await q(c1, "SELECT CDC_READ($1,$2) AS r", [cdcBase, 100]))[0].r);
    const shape = evs.map((e) => `${e.change}@${e.table}`);
    const keys = [...new Set(evs.flatMap((e) => Object.keys(e)))].sort().join("/");
    // UPSTREAM DEFECT (recorded, pinned): on this build only INSERT statements
    // append CDC events — UPDATE and DELETE statements emit nothing (their
    // emit sites exist in the source but the executed path never reaches
    // them). If this check fails with UPDATE/DELETE events present, the
    // engine changed — update this pin and the SDK docs together.
    if (shape.join(",") !== `INSERT@${TAG}_cdc,INSERT@${TAG}_cdc`) {
      throw new Error(`event sequence changed vs recorded defect: ${shape}`);
    }
    if (keys !== "change/seq/table/ts") throw new Error(`metadata-only contract broken: keys=${keys}`);
    return `I/U/D attempted -> ${evs.length} events, all INSERT (3-row insert = 1 per-statement event); UPDATE/DELETE never emitted — upstream defect; keys=${keys}`;
  });

  await check("cdc.pre_commit_emission_and_gaps", async () => {
    const base = Number((await q(c1, "SELECT CDC_COUNT() AS n"))[0].n);
    await c1.query("BEGIN");
    await c1.query(`INSERT INTO ${TAG}_cdc VALUES (99, 'rolled-back')`);
    await c1.query("ROLLBACK");
    await q(c1, `INSERT INTO ${TAG}_cdc VALUES (100, 'committed')`);
    const evs = JSON.parse((await q(c1, "SELECT CDC_READ($1,$2) AS r", [base, 100]))[0].r);
    const inserts = evs.filter((e) => e.change === "INSERT");
    if (inserts.length !== 2) throw new Error(`rolled-back transaction's event missing (${inserts.length} inserts; engine emits pre-commit)`);
    const gap = evs[1].seq - evs[0].seq !== 1;
    return `rolled-back INSERT visible=${inserts.length === 2} seqGapPresent=${gap} (consumers must reconcile, not assume commit)`;
  });

  await check("cdc.table_read_and_count", async () => {
    const n = Number((await q(c1, "SELECT CDC_COUNT() AS n"))[0].n);
    const evs = JSON.parse((await q(c1, "SELECT CDC_TABLE_READ($1,$2,$3) AS r", [`${TAG}_cdc`, 0, 3]))[0].r);
    if (!evs.every((e) => e.table === `${TAG}_cdc`)) throw new Error("table filter leaked other tables");
    return `count=${n} tableRead(limit 3)=${evs.length} all matching table`;
  });

  await check("cdc.restart_replay_seq_continues", async () => {
    const before = Number((await q(c1, "SELECT CDC_COUNT() AS n"))[0].n);
    const allBefore = JSON.parse((await q(c1, "SELECT CDC_READ($1,$2) AS r", [0, 100000]))[0].r);
    const maxSeq = Math.max(...allBefore.map((e) => e.seq));
    await restartEngine();
    const c = client();
    await c.connect();
    const after = Number((await q(c, "SELECT CDC_COUNT() AS n"))[0].n);
    if (after < before) throw new Error(`events lost across restart: ${before} -> ${after}`);
    await q(c, `INSERT INTO ${TAG}_cdc VALUES (200, 'post')`);
    const next = JSON.parse((await q(c, "SELECT CDC_READ($1,$2) AS r", [maxSeq, 10]))[0].r);
    await c.end();
    if (next.length !== 1 || next[0].seq <= maxSeq) throw new Error(`seq numbering reset or event missing after restart: ${JSON.stringify(next)}`);
    return `eventsReplayed=${after}/${before} seqContinuesFrom=${next[0].seq} (no reset)`;
  });
  await reopen();

  // ======================================================================
  // PUBSUB (SQL surface)
  // ======================================================================
  await check("pubsub.sql_surface_publish_only", async () => {
    const n = Number((await q(c1, "SELECT PUBSUB_PUBLISH($1,$2) AS n", [`${TAG}_ch`, "msg"]))[0].n);
    const chans = (await q(c1, "SELECT PUBSUB_CHANNELS() AS c"))[0].c;
    const subs = Number((await q(c1, "SELECT PUBSUB_SUBSCRIBERS($1) AS n", [`${TAG}_ch`]))[0].n);
    if (n !== 0 || subs !== 0 || chans !== "") throw new Error(`unexpected live hub over SQL: publish=${n} subs=${subs} channels=${JSON.stringify(chans)}`);
    return `publish=0 subscribers=0 channels="" — no SQL subscribe surface; delivery unobservable over the wire`;
  });

  await check("pubsub.channels_pattern_argument_ignored", async () => {
    // Engine defect pin (client workaround: the parameter was removed from
    // the TS client). PUBSUB_CHANNELS($1) is ACCEPTED and the pattern is
    // ignored — a silent-wrong-result shape, not an arity error.
    const r = await q(c1, "SELECT PUBSUB_CHANNELS($1) AS c", ["zzz*"]);
    return `accepted with pattern, result=${JSON.stringify(r[0].c)} (pattern silently ignored — upstream candidate)`;
  });

  // ======================================================================
  // DATALOG
  // ======================================================================
  await check("datalog.roundtrip_recursion_oracle", async () => {
    await q(c1, "SELECT DATALOG_CLEAR($1)", [`${TAG}_p`]);
    await q(c1, "SELECT DATALOG_CLEAR($1)", [`${TAG}_anc`]);
    for (const f of [`${TAG}_p(a, b)`, `${TAG}_p(b, c)`, `${TAG}_p(c, d)`]) {
      await q(c1, "SELECT DATALOG_ASSERT($1)", [f]);
    }
    await q(c1, "SELECT DATALOG_RULE($1)", [`${TAG}_anc(X, Y) :- ${TAG}_p(X, Y)`]);
    await q(c1, "SELECT DATALOG_RULE($1)", [`${TAG}_anc(X, Z) :- ${TAG}_p(X, Y), ${TAG}_anc(Y, Z)`]);
    const res = JSON.parse((await q(c1, "SELECT DATALOG_QUERY($1) AS r", [`${TAG}_anc(a, Who)`]))[0].r);
    const got = res.map((t) => t[1]).sort();
    const expected = ["b", "c", "d"]; // hand oracle: transitive closure of a
    if (JSON.stringify(got) !== JSON.stringify(expected)) throw new Error(`closure wrong: ${JSON.stringify(got)} vs ${JSON.stringify(expected)}`);
    return `anc(a,Who) = [${got.join(",")}] matches hand oracle (recursion depth 3)`;
  });

  await check("datalog.mutators_return_status_strings", async () => {
    const a = (await q(c1, "SELECT DATALOG_ASSERT($1) AS v", [`${TAG}_q(x, y)`]))[0].v;
    const r = (await q(c1, "SELECT DATALOG_RULE($1) AS v", [`${TAG}_qr(X, Y) :- ${TAG}_q(X, Y)`]))[0].v;
    const rt = (await q(c1, "SELECT DATALOG_RETRACT($1) AS v", [`${TAG}_q(x, y)`]))[0].v;
    const cl = (await q(c1, "SELECT DATALOG_CLEAR($1) AS v", [`${TAG}_q`]))[0].v;
    return `assert=${JSON.stringify(a)} rule=${JSON.stringify(r)} retract=${JSON.stringify(rt)} clear=${JSON.stringify(cl)}`;
  });

  await check("datalog.facts_and_rules_survive_restart", async () => {
    await q(c1, "SELECT DATALOG_CLEAR($1)", [`${TAG}_d`]);
    await q(c1, "SELECT DATALOG_ASSERT($1)", [`${TAG}_d(a, b)`]);
    await q(c1, "SELECT DATALOG_RULE($1)", [`${TAG}_dr(X, Y) :- ${TAG}_d(X, Y)`]);
    const before = (await q(c1, "SELECT DATALOG_QUERY($1) AS r", [`${TAG}_dr(X, Y)`]))[0].r;
    await restartEngine();
    const c = client();
    await c.connect();
    const after = (await q(c, "SELECT DATALOG_QUERY($1) AS r", [`${TAG}_dr(X, Y)`]))[0].r;
    await c.end();
    if (after !== before) throw new Error(`restart changed the answer: ${before} -> ${after}`);
    return `facts+rules durable across SIGKILL restart (${after})`;
  });
  await reopen();

  await check("datalog.two_arg_rule_form_rejected", async () => {
    // Pin the old TS-client shape: DATALOG_RULE($1, $2) — the engine takes
    // exactly one argument; the two-arg form fails (it did in every SDK).
    try {
      await q(c1, "SELECT DATALOG_RULE($1, $2)", ["h(X)", "b(X)"]);
      throw new Error("two-arg form accepted");
    } catch (e) {
      return `rejected as expected: ${String(e.message).slice(0, 70)}`;
    }
  });

  // ======================================================================
  // LISTEN/NOTIFY (statement surface, wire-level delivery)
  // ======================================================================
  await check("listen.cross_connection_delivery_flush_divergence", async () => {
    const c2 = client();
    await c2.connect();
    const got = [];
    c2.on("notification", (n) => got.push({ channel: n.channel, payload: n.payload }));
    await c2.query(`LISTEN "${TAG}_chan"`);
    await c1.query(`NOTIFY "${TAG}_chan", 'hello'`);
    await sleep(400);
    const idle = got.length;
    await c2.query("SELECT 1"); // flush trigger
    await sleep(300);
    await c2.end();
    if (got.length < 1) throw new Error("notification never delivered");
    const payloadOk = got[0].payload === "hello";
    return `idleDelivered=${idle} afterStatementDelivered=${got.length} payloadRoundTrip=${payloadOk} (delivery piggybacks on the listener's own statement traffic — a real PG divergence)`;
  });

  await check("listen.rollback_tx_pre_commit_divergence", async () => {
    const c2 = client();
    await c2.connect();
    const got = [];
    c2.on("notification", (n) => got.push(n.payload));
    await c2.query(`LISTEN "${TAG}_rb"`);
    await c1.query("BEGIN");
    await c1.query(`NOTIFY "${TAG}_rb", 'in-tx'`);
    await c1.query("ROLLBACK");
    await c1.query(`NOTIFY "${TAG}_rb", 'committed'`);
    await c2.query("SELECT 1");
    await sleep(300);
    await c2.end();
    const rolledBackVisible = got.includes("in-tx");
    if (!got.includes("committed")) throw new Error("committed notification missing");
    return `rolled-back-tx notification delivered=${rolledBackVisible} (PostgreSQL delivers only at COMMIT — divergence recorded)`;
  });

  // ======================================================================
  // LISTEN/NOTIFY — the @neutron-build/sql/listen-notify module itself on
  // this engine (its live battery is PostgreSQL-only, so the leg is the
  // Nucleus coverage): pollIntervalMs idle flush, channel-key normalization
  // (owned AND borrowed), and graceful-close event ordering.
  // ======================================================================
  await check("listen.module_owned_poll_flush_and_normalized_channel", async () => {
    const chan = `${TAG}_mod`;
    const got = [];
    const listener = await pgListener({ url: url(), pollIntervalMs: 150 });
    try {
      await listener.listen([chan], (n) => got.push(n.channel));
      await q(c1, `NOTIFY "${chan}", 'module-owned'`);
      const t0 = Date.now();
      while (got.length < 1 && Date.now() - t0 < 6000) await sleep(100);
      if (got.length !== 1) throw new Error("idle owned listener did not deliver within pollIntervalMs (flush failed)");
      if (got[0] !== chan) throw new Error(`owned channel not normalized: ${JSON.stringify(got[0])}`);
    } finally {
      await listener.close();
    }
    return `idle delivery via pollIntervalMs=150; channel reported normalized (${JSON.stringify(got[0])})`;
  });

  await check("listen.module_borrowed_channel_normalized", async () => {
    const chan = `${TAG}_modb`;
    const dedicated = new pg.Client({ connectionString: url() });
    dedicated.on("error", () => {});
    await dedicated.connect();
    const raw = [];
    dedicated.on("notification", (n) => raw.push(n.channel));
    const got = [];
    const listener = await pgListener({ client: dedicated, pollIntervalMs: 150 });
    try {
      await listener.listen([chan], (n) => got.push(n.channel));
      await q(c1, `NOTIFY "${chan}", 'module-borrowed'`);
      const t0 = Date.now();
      while (got.length < 1 && Date.now() - t0 < 6000) await sleep(100);
      if (got.length !== 1) throw new Error("borrowed listener did not deliver");
      if (got[0] !== chan) throw new Error(`borrowed channel not normalized: ${JSON.stringify(got[0])} (raw wire ${JSON.stringify(raw[0])})`);
    } finally {
      await listener.close();
    }
    const alive = await dedicated.query("SELECT 1 AS one");
    if (alive.rows[0].one !== 1) throw new Error("borrowed client unusable after listener close");
    await dedicated.end();
    return `borrowed channel normalized (${JSON.stringify(got[0])}; raw wire ${JSON.stringify(raw[0])}); owner's client survived close`;
  });

  await check("listen.module_owned_graceful_close_events", async () => {
    const events = [];
    let onDisconnect = 0;
    const listener = await pgListener({ url: url(), logger: (e) => events.push(e.kind) });
    await listener.listen(`${TAG}_modc`, () => {}, () => {
      onDisconnect++;
    });
    await listener.notify(`${TAG}_modc`, "last");
    await sleep(200);
    await listener.close();
    if (events.includes("listener-disconnected")) {
      throw new Error(`graceful close emitted listener-disconnected (teardown ordering): ${events.join(",")}`);
    }
    if (onDisconnect !== 0) throw new Error("onDisconnect fired on deliberate close");
    if (events[events.length - 1] !== "listener-closed") throw new Error(`last event not listener-closed: ${events.join(",")}`);
    return `close events=[${events.join(" -> ")}] onDisconnect=0 (disconnect signal only on real connection death)`;
  });

  // ======================================================================
  // RLS — the CDC exploration policy boundary (card requirement)
  // ======================================================================
  const role = `${TAG}_app`;
  // The engine's RLS predicate allow-list accepts boolean constants (not the
  // current_setting('app.tenant')::int idiom — that is a documented X00
  // limitation), so the policy here is a plain constant the role never
  // satisfies. What is under test is the CDC policy BOUNDARY, not predicate
  // expressiveness (X00 owns that).
  await q(c1, `CREATE ROLE ${role} NOLOGIN`);
  await q(c1, `CREATE TABLE ${TAG}_sec (id INT PRIMARY KEY, tenant INT NOT NULL)`);
  await q(c1, `INSERT INTO ${TAG}_sec VALUES (1, 1)`);
  await q(c1, `GRANT SELECT ON ${TAG}_sec TO ${role}`);
  await q(c1, `ALTER TABLE ${TAG}_sec ENABLE ROW LEVEL SECURITY`);
  await q(c1, `CREATE POLICY ${TAG}_pol ON ${TAG}_sec USING (false)`);

  // One statement per transaction: a denied statement aborts the whole
  // transaction on this engine (25P02 for everything after), so sharing a
  // transaction would test abort semantics, not the policy boundary.
  const asRole = async (sqlText) => {
    await c1.query("BEGIN");
    await c1.query(`SET LOCAL ROLE ${role}`);
    try {
      const r = await c1.query(sqlText);
      await c1.query("ROLLBACK");
      return { allowed: true, rows: r.rows };
    } catch (e) {
      await c1.query("ROLLBACK").catch(() => {});
      return { allowed: false, code: e.code, message: String(e.message).slice(0, 60) };
    }
  };

  await check("rls.restricted_principal_setup", async () => {
    const rows = await asRole(`SELECT id FROM ${TAG}_sec`);
    if (rows.allowed && rows.rows.length > 0) {
      throw new Error(`restricted role read ${rows.rows.length} RLS-protected rows`);
    }
    return rows.allowed
      ? "USING(false) policy filters the role to 0 rows — the role is genuinely restricted"
      : `table read denied (${rows.code}) — the role is genuinely restricted`;
  });

  await check("rls.cdc_denied_for_restricted_principal", async () => {
    const read = await asRole("SELECT CDC_READ(0, 10)");
    const tableRead = await asRole(`SELECT CDC_TABLE_READ('${TAG}_sec', 0, 10)`);
    const count = await asRole("SELECT CDC_COUNT()");
    if (read.allowed || tableRead.allowed || count.allowed) {
      throw new Error(`CDC exploration allowed for restricted principal: read=${read.allowed} table=${tableRead.allowed} count=${count.allowed}`);
    }
    return `CDC_READ=${read.code} CDC_TABLE_READ=${tableRead.code} CDC_COUNT=${count.code} (fail-closed)`;
  });

  await check("rls.cdc_pg_catalog_bypass_verdict", async () => {
    // Upstream defect candidate (engine's prefix guard misses
    // pg_catalog-qualified lower-case spellings — recorded in the X00-era
    // engine semantics doc). The TS client uses ONLY the guarded spellings;
    // this check records the engine's actual answer for the engine owners.
    const byp = await asRole(`SELECT pg_catalog.cdc_table_read('${TAG}_sec', 0, 10) AS r`);
    const bypCount = await asRole("SELECT pg_catalog.cdc_count()");
    const leak = byp.allowed
      ? `${JSON.parse(byp.rows[0].r).length} events leaked (metadata only: seq/table/change/ts)`
      : `denied (${byp.code}) — guard canonicalizes qualified calls on this build`;
    return `pg_catalog.cdc_table_read: ${leak}; pg_catalog.cdc_count: ${bypCount.allowed ? "allowed" : `denied (${bypCount.code})`}`;
  });

  await check("rls.streams_datalog_pubsub_denied", async () => {
    const attempts = [
      ["STREAM_XLEN", `SELECT STREAM_XLEN('${TAG}_s')`],
      ["DATALOG_QUERY", "SELECT DATALOG_QUERY('p(a, X)')"],
      ["PUBSUB_CHANNELS", "SELECT PUBSUB_CHANNELS()"],
    ];
    const verdicts = [];
    for (const [name, sqlText] of attempts) {
      const r = await asRole(sqlText);
      verdicts.push(`${name}=${r.allowed ? "ALLOWED" : r.code}`);
    }
    if (verdicts.some((v) => v.endsWith("ALLOWED"))) throw new Error(`policy boundary broken: ${verdicts.join(" ")}`);
    // The statement-level LISTEN surface is NOT gated on this build (the
    // RLS prefix guard covers scalar functions only) — recorded as an
    // upstream note, not a failure: channels are not policy-scoped, so a
    // restricted principal may listen for (and receive) whatever any session
    // NOTIFYs on that channel.
    const listen = await asRole(`LISTEN "${TAG}_l"`);
    verdicts.push(`LISTEN(statement)=${listen.allowed ? "ALLOWED (ungated statement surface — upstream note)" : listen.code}`);
    return verdicts.join(" ");
  });

  await c1.end();
} catch (err) {
  out.harnessError = err instanceof Error ? err.message : String(err);
  process.exitCode = 1;
} finally {
  if (child) child.kill("SIGKILL");
  for (const c of clients) {
    try { c.end(); } catch { /* dying */ }
  }
  await sleep(300);
  if (dataDir) {
    try { rmSync(dataDir, { recursive: true, force: true }); } catch { /* best effort */ }
  }
}

out.results = results;
out.summary = { total: results.length, passed: results.filter((r) => r.pass).length, failed: results.filter((r) => !r.pass).length };
writeFileSync(OUT, JSON.stringify(out, null, 2) + "\n");
console.log(`\nx05 nucleus leg: ${out.summary.passed}/${out.summary.total} passed (${out.summary.failed} failed) -> ${OUT}`);
if (out.summary.failed > 0) process.exitCode = 1;
