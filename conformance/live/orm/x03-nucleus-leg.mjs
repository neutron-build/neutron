// X03 Nucleus leg — time-series and columnar workflow evidence against the
// REAL engine build (never mocked). Records:
//   1. TS_* model semantics with negative controls (a global-aggregate-
//      as-range fake, a constant counter, an input-echo bucket fn all fail),
//      raw TS_RANGE exactness and TIME_BUCKET grid behavior, sub-ms
//      timestamp-key precision.
//   2. COLUMNAR_* store aggregate semantics per column, transaction
//      behavior (rollback persistence is documented), pre-restart baseline.
//   3. engine='columnar' storage-engine DDL + CRUD + rollback round-trip.
//   4. The neutron-nucleus CLIENT's probeTimeSeriesModel end-to-end through
//      a real PgTransport, plus client query() if the probe passes.
//   5. The SQL-side /timeseries capability verdicts (ts-bucketing, windows).
//   6. RESTART durability: clean SIGTERM restart, then kill -9 mid-window —
//      the documented durability split (TS fsync-at-commit vs columnar store
//      checkpoint-only vs columnar engine table fsync-at-commit).
//   7. RETENTION boundary last (destructive, global): old dropped / new
//      kept at the tick, backfill destruction reproducer.
//
// Usage: node conformance/live/orm/x03-nucleus-leg.mjs <path-to-nucleus-binary>
// Leaves no processes behind; data dir deleted at exit.
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

const BIN = process.argv[2];
if (!BIN) {
  console.error("usage: node x03-nucleus-leg.mjs <nucleus-binary>");
  process.exit(2);
}
const PORT = 55932;
// Short checkpoint interval (default 300s) so retention ticks and columnar
// checkpoints fire within seconds — documented env knob (CONFIG_REFERENCE:
// NUCLEUS_WAL_CHECKPOINT_INTERVAL_SECS).
const CHECKPOINT_SECS = 2;
const SQLDIST = path.resolve(new URL("../../../typescript/packages/neutron-sql/dist", import.meta.url).pathname);
const NUCDIST = path.resolve(new URL("../../../typescript/packages/neutron-nucleus/dist", import.meta.url).pathname);

const dataDir = mkdtempSync(path.join(tmpdir(), "x03-nucleus-"));
writeFileSync(
  path.join(dataDir, "nucleus.toml"),
  ["[storage]", "disk_warn_free_pct = 0.5", "disk_readonly_free_pct = 0.1", "disk_min_free_mb = 128", ""].join("\n"),
);

let child = null;
let log = "";
function startEngine() {
  child = spawn(BIN, ["start", "--port", String(PORT), "--data", dataDir], {
    env: { ...process.env, NUCLEUS_WAL_CHECKPOINT_INTERVAL_SECS: String(CHECKPOINT_SECS) },
    stdio: ["ignore", "pipe", "pipe"],
  });
  log = "";
  child.stdout.on("data", (d) => (log += d));
  child.stderr.on("data", (d) => (log += d));
}
function stopEngine(signal = "SIGTERM", timeoutMs = 15000) {
  return new Promise((resolve) => {
    if (child === null || child.exitCode !== null) return resolve({ already: true, code: child?.exitCode ?? null });
    const t = setTimeout(() => {
      try { child?.kill("SIGKILL"); } catch {}
      resolve({ timeout: true });
    }, timeoutMs);
    child.once("exit", (code, sig) => {
      clearTimeout(t);
      resolve({ code, sig });
    });
    try { child.kill(signal); } catch {}
  });
}
async function waitReady() {
  for (let i = 0; i < 80; i++) {
    await sleep(500);
    if (/listening|started|ready/i.test(log) || log.includes(`${PORT}`)) return true;
  }
  return false;
}

const url = `postgres://localhost:${PORT}/postgres`;
const out = { started: false, engine: null, matrix: {}, restart: {}, retention: {}, client: {}, sqlModule: {} };
const tsSeries = `x03_leg_ts_${Date.now()}`;
const colTable = `x03_leg_col_${Date.now()}`;

try {
  startEngine();
  out.started = await waitReady();
  if (!out.started) throw new Error("engine did not become ready:\n" + log.slice(-2000));

  const sql = await import(path.join(SQLDIST, "index.js"));
  const { createDatabase, capabilityGate } = sql;
  const db = await createDatabase({ url, driverOptions: { driver: "pg" } });
  const gate = capabilityGate(db.driver);
  out.engine = await gate.engine();

  const val = async (text, params = []) => {
    const rows = await db.driver.query(text, params);
    return rows.length > 0 ? rows[0][Object.keys(rows[0])[0]] : null;
  };

  // ---------------------------------------------------------------- 1. TS
  {
    const m = { series: tsSeries };
    const points = [
      { t: 1000, v: 1 },
      { t: 3000, v: 2 },
      { t: 6000, v: 6 },
      { t: 9000, v: 4 },
    ];
    try {
      for (const p of points) await db.driver.query("SELECT TS_INSERT($1, $2, $3)", [tsSeries, p.t, p.v]);
      m.insertAcked = true;
    } catch (err) {
      m.insertAcked = false;
      m.insertError = String(err.message ?? err).slice(0, 200);
    }
    if (m.insertAcked) {
      m.countTotal = Number(await val("SELECT TS_COUNT($1)", [tsSeries]));
      m.countExact = m.countTotal === 4;
      m.last = Number(await val("SELECT TS_LAST($1)", [tsSeries]));
      m.rangeCountScoped = Number(await val("SELECT TS_RANGE_COUNT($1, $2, $3)", [tsSeries, 3000, 7000]));
      m.rangeCountScopingExact = m.rangeCountScoped === 2; // a global answer (4) is a fake range
      m.rangeAvgScoped = Number(await val("SELECT TS_RANGE_AVG($1, $2, $3)", [tsSeries, 3000, 7000]));
      m.rangeAvgScopingExact = m.rangeAvgScoped === 4; // the global mean 3.25 is the fake answer
      try {
        const raw = await val("SELECT TS_RANGE($1, $2, $3)", [tsSeries, 2000, 7000]);
        m.tsRangeRaw = typeof raw === "string" ? raw.slice(0, 300) : raw;
        let parsed = null;
        try { parsed = JSON.parse(raw); } catch { parsed = null; }
        m.tsRangeExact =
          Array.isArray(parsed) &&
          JSON.stringify(parsed.map((p) => ({ t: p.t, v: p.v })).sort((a, b) => a.t - b.t)) ===
            JSON.stringify([{ t: 3000, v: 2 }, { t: 6000, v: 6 }]);
        m.tsRangeOrder = Array.isArray(parsed) ? parsed.map((p) => p.t).join(",") : null;
      } catch (err) {
        m.tsRangeError = String(err.message ?? err).slice(0, 200);
      }
      try {
        const b0 = Number(await val("SELECT TIME_BUCKET($1, $2)", [5000, 1]));
        const b1 = Number(await val("SELECT TIME_BUCKET($1, $2)", [5000, 12345]));
        const b2 = Number(await val("SELECT TIME_BUCKET($1, $2)", [5000, 17345]));
        m.timeBucket = { b0, b1, b2 };
        // grid-invariant controls: 1ms after epoch buckets to the epoch
        // (an echo answers 1); each input sits inside its own bucket; two
        // inputs a full interval apart land in different buckets.
        m.timeBucketReal = b0 === 0 && b1 <= 12345 && 12345 < b1 + 5000 && b2 <= 17345 && 17345 < b2 + 5000 && b1 !== b2;
      } catch (err) {
        m.timeBucketError = String(err.message ?? err).slice(0, 200);
      }
      try {
        await db.driver.query("SELECT TS_INSERT($1, $2, $3)", [`${tsSeries}_us`, 1730000000123456, 9]);
        m.microTimestampCountInRange = Number(
          await val("SELECT TS_RANGE_COUNT($1, $2, $3)", [`${tsSeries}_us`, 1730000000000000, 1730000000999999]),
        );
        m.microTimestampKeyPreserved = m.microTimestampCountInRange === 1; // numeric key compared at full precision
      } catch (err) {
        m.microTimestampError = String(err.message ?? err).slice(0, 200);
      }
    }
    out.matrix.timeSeries = m;
  }

  // ------------------------------------------------- 2. columnar STORE
  {
    const m = { table: colTable };
    try {
      m.insertReturns = await val("SELECT COLUMNAR_INSERT($1, $2, $3, $4, $5, $6, $7, $8, $9)", [
        colTable, "a", 1, "b", 10, "c", 100, "d", 1000,
      ]);
      await val("SELECT COLUMNAR_INSERT($1, $2, $3, $4, $5, $6, $7, $8, $9)", [colTable, "a", 2, "b", 20, "c", 200, "d", 2000]);
      await val("SELECT COLUMNAR_INSERT($1, $2, $3, $4, $5, $6, $7, $8, $9)", [colTable, "a", 3, "b", 30, "c", 300, "d", 3000]);
      await val("SELECT COLUMNAR_INSERT($1, $2, $3, $4, $5, $6, $7, $8, $9)", [colTable, "a", 4, "b", 40, "c", 400, "d", 4000]);
      m.countExact = Number(await val("SELECT COLUMNAR_COUNT($1)", [colTable])) === 4;
      // TYPING characterization (X03 finding): untyped params store as text
      // and the numeric aggregates then answer a SILENT 0/NULL; explicitly
      // cast values aggregate correctly. Both recorded.
      m.untypedSumA = Number(await val("SELECT COLUMNAR_SUM($1, $2)", [colTable, "a"]));
      m.untypedMinA = await val("SELECT COLUMNAR_MIN($1, $2)", [colTable, "a"]);
      m.untypedAggregateSilentlyZero = m.untypedSumA === 0 && m.untypedMinA === null;
      const castTable = `${colTable}_cast`;
      for (const a of [1, 2, 3, 4]) {
        await db.driver.query("SELECT COLUMNAR_INSERT($1, $2, $3::double precision, $4, $5::double precision)", [castTable, "a", a, "b", a * 10]);
      }
      m.castSumA = Number(await val("SELECT COLUMNAR_SUM($1, $2)", [castTable, "a"]));
      m.castSumB = Number(await val("SELECT COLUMNAR_SUM($1, $2)", [castTable, "b"]));
      m.castAggregatesExact = m.castSumA === 10 && m.castSumB === 100;
      m.castAvgA = Number(await val("SELECT COLUMNAR_AVG($1, $2)", [castTable, "a"]));
      m.castAvgExact = m.castAvgA === 2.5;
      m.castMinA = Number(await val("SELECT COLUMNAR_MIN($1, $2)", [castTable, "a"]));
      m.castMaxA = Number(await val("SELECT COLUMNAR_MAX($1, $2)", [castTable, "a"]));
      m.castMinMaxExact = m.castMinA === 1 && m.castMaxA === 4;
      // transactions: the engine REFUSES columnar inserts inside an explicit
      // transaction (observed live; the older MODEL_SEMANTICS claim that
      // rolled-back rows persist is stale — the refusal is safer).
      try {
        await db.driver.execute("BEGIN");
        await val("SELECT COLUMNAR_INSERT($1, $2, $3, $4, $5)", [colTable, "a", 99, "b", 990]);
        await db.driver.execute("ROLLBACK");
        m.inTxInsert = { rejected: false, countAfter: Number(await val("SELECT COLUMNAR_COUNT($1)", [colTable])) };
      } catch (err) {
        m.inTxInsert = { rejected: true, error: String(err.message ?? err).slice(0, 200) };
      }
      m.baselineCount = Number(await val("SELECT COLUMNAR_COUNT($1)", [colTable]));
      m.baselineCastSumA = Number(await val("SELECT COLUMNAR_SUM($1, $2)", [castTable, "a"]));
    } catch (err) {
      m.error = String(err.message ?? err).slice(0, 300);
    }
    out.matrix.columnarStore = m;
  }

  // ------------------------------------- 3. engine='columnar' TABLE
  {
    const m = {};
    try {
      await db.driver.execute("CREATE TABLE x03_leg_eng (id int primary key, v double precision) WITH (engine='columnar')");
      m.createOk = true;
      await db.driver.execute("INSERT INTO x03_leg_eng VALUES (1, 1.5), (2, 2.5), (3, 3.5)");
      const rows = await db.driver.query("SELECT id, v FROM x03_leg_eng ORDER BY id");
      m.selectRoundTrip = JSON.stringify(rows.map((r) => ({ id: Number(r.id), v: Number(r.v) }))) ===
        JSON.stringify([{ id: 1, v: 1.5 }, { id: 2, v: 2.5 }, { id: 3, v: 3.5 }]);
      await db.driver.execute("UPDATE x03_leg_eng SET v = 9.5 WHERE id = 2");
      m.updateWorks = Number(await val("SELECT v FROM x03_leg_eng WHERE id = 2")) === 9.5;
      await db.driver.execute("DELETE FROM x03_leg_eng WHERE id = 3");
      m.deleteWorks = Number(await val("SELECT count(*) FROM x03_leg_eng")) === 2;
      try {
        await db.driver.execute("BEGIN");
        await db.driver.execute("INSERT INTO x03_leg_eng VALUES (4, 4.5)");
        await db.driver.execute("ROLLBACK");
        m.rollbackOnEngineTable = Number(await val("SELECT count(*) FROM x03_leg_eng")) === 2;
      } catch (err) {
        m.rollbackOnEngineTable = `error: ${String(err.message ?? err).slice(0, 120)}`;
      }
    } catch (err) {
      m.createOk = false;
      m.error = String(err.message ?? err).slice(0, 300);
    }
    out.matrix.columnarEngineTable = m;
  }

  // ------------------------------------------------ 4. real CLIENT
  {
    try {
      const nuc = await import(path.join(NUCDIST, "index.js"));
      const client = await nuc
        .createClient({ url })
        .use(nuc.withTimeSeries)
        .use(nuc.withColumnar)
        .connect();
      out.client.probeEvidence = await nuc.probeTimeSeriesModel(client.transport);
      if (out.client.probeEvidence.rangeFetch) {
        await client.timeseries.write("x03_leg_client", [
          { timestamp: new Date(1000), value: 1 },
          { timestamp: new Date(3000), value: 2 },
          { timestamp: new Date(6000), value: 6 },
        ]);
        const points = await client.timeseries.query("x03_leg_client", new Date(2000), new Date(7000));
        out.client.queryRoundTrip = points.map((p) => `${p.timestamp.getTime()}=${p.value}`).join(",");
        out.client.queryRoundTripExact = out.client.queryRoundTrip === "3000=2,6000=6";
      }
      // columnar client path: insert() now binds numbers with an explicit
      // cast (X03 fix) — the aggregates must be REAL through the client.
      const ct = `x03_leg_client_col_${Date.now()}`;
      await client.columnar.insert(ct, { a: 1, b: 10 });
      await client.columnar.insert(ct, { a: 2, b: 20 });
      await client.columnar.insert(ct, { a: 3, b: 30 });
      out.client.columnar = {
        count: await client.columnar.count(ct),
        sumA: await client.columnar.sum(ct, "a"),
        sumB: await client.columnar.sum(ct, "b"),
        avgA: await client.columnar.avg(ct, "a"),
        minA: await client.columnar.min(ct, "a"),
        maxA: await client.columnar.max(ct, "a"),
      };
      out.client.columnarExact =
        out.client.columnar.count === 3 && out.client.columnar.sumA === 6 && out.client.columnar.sumB === 60 &&
        out.client.columnar.avgA === 2 && out.client.columnar.minA === 1 && out.client.columnar.maxA === 3;
      await client.close();
    } catch (err) {
      out.client.error = String(err.message ?? err).slice(0, 300);
    }
  }

  // --------------------------------------- 5. SQL module capabilities
  {
    const ev = await gate.status("ts-bucketing");
    out.sqlModule.tsBucketing = { status: ev.status, evidence: ev.evidence.slice(0, 200) };
    const w = await gate.status("window-functions");
    out.sqlModule.windowFunctions = { status: w.status };
  }

  await db.driver.close();

  // ------------------------------------------------ 6. restart evidence
  out.restart.cleanStop = await stopEngine("SIGTERM");
  startEngine();
  out.restart.restartedClean = await waitReady();
  if (!out.restart.restartedClean) throw new Error("engine did not restart cleanly:\n" + log.slice(-2000));
  {
    const db2 = await createDatabase({ url, driverOptions: { driver: "pg" } });
    const v2 = async (text, params = []) => {
      const rows = await db2.driver.query(text, params);
      return rows.length > 0 ? rows[0][Object.keys(rows[0])[0]] : null;
    };
    out.restart.afterCleanRestart = {
      tsCountSeries1: Number(await v2("SELECT TS_COUNT($1)", [tsSeries])),
      columnarStoreCount: Number(await v2("SELECT COLUMNAR_COUNT($1)", [colTable])),
      columnarStoreCastSumA: Number(await v2("SELECT COLUMNAR_SUM($1, $2)", [`${colTable}_cast`, "a"])),
      engineTableCount: Number(await v2("SELECT count(*) FROM x03_leg_eng")),
      engineTableV2: Number(await v2("SELECT v FROM x03_leg_eng WHERE id = 2")),
    };
    // kill -9 INSIDE the checkpoint window: write, then kill immediately.
    // fsync-at-commit stores keep the rows; checkpoint-only stores lose
    // whatever no checkpoint captured.
    await v2("SELECT TS_INSERT($1, $2, $3)", ["x03_leg_kill9", Date.now(), 42]);
    await v2("SELECT COLUMNAR_INSERT($1, $2, $3, $4, $5)", [`${colTable}_k9`, "a", 7, "b", 70]);
    await db2.driver.execute("INSERT INTO x03_leg_eng VALUES (10, 10.5)");
    await db2.driver.close();
  }
  out.restart.kill9 = await stopEngine("SIGKILL");
  startEngine();
  out.restart.restartedAfterKill9 = await waitReady();
  if (out.restart.restartedAfterKill9) {
    const db3 = await createDatabase({ url, driverOptions: { driver: "pg" } });
    const v3 = async (text, params = []) => {
      const rows = await db3.driver.query(text, params);
      return rows.length > 0 ? rows[0][Object.keys(rows[0])[0]] : null;
    };
    out.restart.afterKill9 = {
      tsKill9Count: Number(await v3("SELECT TS_COUNT($1)", ["x03_leg_kill9"])),
      columnarKill9Count: Number(await v3("SELECT COLUMNAR_COUNT($1)", [`${colTable}_k9`])),
      engineTableKill9HasId10: Number(await v3("SELECT count(*) FROM x03_leg_eng WHERE id = 10")),
    };
    await db3.driver.close();
  }

  // ------------------------------------------- 7. retention (LAST)
  {
    const db4 = await createDatabase({ url, driverOptions: { driver: "pg" } });
    const v4 = async (text, params = []) => {
      const rows = await db4.driver.query(text, params);
      return rows.length > 0 ? rows[0][Object.keys(rows[0])[0]] : null;
    };
    const now = Date.now();
    const oldSeries = "x03_leg_ret_old";
    const newSeries = "x03_leg_ret_new";
    const backfillSeries = "x03_leg_ret_backfill";
    await v4("SELECT TS_INSERT($1, $2, $3)", [oldSeries, now - 30_000, 1]);
    await v4("SELECT TS_INSERT($1, $2, $3)", [oldSeries, now - 25_000, 2]);
    await v4("SELECT TS_INSERT($1, $2, $3)", [newSeries, now - 1_000, 3]);
    const before = {
      oldCount: Number(await v4("SELECT TS_COUNT($1)", [oldSeries])),
      newCount: Number(await v4("SELECT TS_COUNT($1)", [newSeries])),
    };
    const ack = await v4("SELECT TS_RETENTION($1)", [8000]);
    let after = { ...before };
    for (let i = 0; i < 8; i++) {
      await sleep(CHECKPOINT_SECS * 1000);
      after = {
        oldCount: Number(await v4("SELECT TS_COUNT($1)", [oldSeries])),
        newCount: Number(await v4("SELECT TS_COUNT($1)", [newSeries])),
      };
      if (after.oldCount === 0) break;
    }
    await v4("SELECT TS_INSERT($1, $2, $3)", [backfillSeries, Date.now() - 30_000, 7]);
    const backfillImmediately = Number(await v4("SELECT TS_COUNT($1)", [backfillSeries]));
    let backfillAfterTicks = backfillImmediately;
    for (let i = 0; i < 8 && backfillAfterTicks !== 0; i++) {
      await sleep(CHECKPOINT_SECS * 1000);
      backfillAfterTicks = Number(await v4("SELECT TS_COUNT($1)", [backfillSeries]));
    }
    out.retention = {
      policyMs: 8000,
      ack,
      before,
      after,
      oldDroppedAtTick: after.oldCount === 0,
      newKeptAtTick: after.newCount === 1,
      globalAcrossSeries: before.oldCount === 2 && after.oldCount === 0,
      backfillImmediately,
      backfillDestroyedWithinTicks: backfillAfterTicks === 0,
      backfillCountAfterTicks: backfillAfterTicks,
      boundaryNote:
        "cutoff is computed from engine wall-clock at apply time, so an exactly-at-boundary row is not externally controllable; margins are the evidence (25-30s-old dropped, 1s-old kept, policy 8s)",
    };
    await db4.driver.close();
  }
} catch (err) {
  out.fatal = err?.stack ?? String(err);
} finally {
  await stopEngine("SIGKILL", 5000).catch(() => undefined);
  try { rmSync(dataDir, { recursive: true, force: true }); } catch {}
}

console.log(JSON.stringify(out, null, 2));
