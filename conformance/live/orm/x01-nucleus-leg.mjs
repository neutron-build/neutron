// X01 Nucleus leg — fail-closed evidence against the REAL engine build
// (never mocked). Runs the extension gate, the I01 capability probes and a
// statement-counted vector/FTS query attempt against a locally started
// nucleus binary, records every verdict, and leaves no processes behind.
//
// Usage: node conformance/live/orm/x01-nucleus-leg.mjs <path-to-nucleus-binary>
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

const BIN = process.argv[2];
if (!BIN) {
  console.error("usage: node x01-nucleus-leg.mjs <nucleus-binary>");
  process.exit(2);
}
const PORT = 55931;
const DIST = path.resolve(new URL("../../../typescript/packages/neutron-sql/dist", import.meta.url).pathname);

const dataDir = mkdtempSync(path.join(tmpdir(), "x01-nucleus-"));
const child = spawn(BIN, ["start", "--port", String(PORT), "--data", dataDir], {
  env: { ...process.env },
  stdio: ["ignore", "pipe", "pipe"],
});
let log = "";
child.stdout.on("data", (d) => (log += d));
child.stderr.on("data", (d) => (log += d));

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let up = false;
for (let i = 0; i < 60 && !up; i++) {
  await sleep(500);
  up = /listening|started|ready/i.test(log) || log.includes(`${PORT}`);
}
if (!up) await sleep(3000);

const url = `postgres://localhost:${PORT}/postgres`;
const sql = await import(path.join(DIST, "index.js"));
const { createDatabase, capabilityGate, CapabilityRequirementError, resolveCapabilityStatus, pgTable, serial } = sql;
const pgvector = await import(path.join(DIST, "pgvector.js"));
const sqlFts = await import(path.join(DIST, "fts.js"));
const { pgvectorExtension, l2Distance, pgVector } = pgvector;

const out = { started: up };

try {
  const statements = { count: 0 };
  const db = await createDatabase({
    url,
    driverOptions: { driver: "pg" },
    logger: (e) => {
      if (e.kind === "query-end") statements.count += 1;
    },
  });

  // Engine identity.
  const gate = capabilityGate(db.driver);
  out.engine = await gate.engine();

  // Extension gate (separate concern from capabilities, X01).
  out.extensionGate = await pgvectorExtension(db.driver);

  // Vector capability evidence via the I01 registry (probe-resolved).
  out.vectorCapabilityEvidence = {};
  for (const c of ["vector-type", "vector-operator-l2", "vector-operator-cosine"]) {
    const ev = await gate.status(c);
    out.vectorCapabilityEvidence[c] = { status: ev.status, evidence: ev.evidence.slice(0, 140) };
  }

  // A vector query must fail closed BEFORE any SQL runs.
  const before = statements.count;
  const docs = pgTable("x01_nucleus_docs", { id: serial("id").primaryKey(), embedding: pgVector("embedding", 3).notNull() });
  try {
    await db
      .select({ id: docs.id, d: l2Distance(docs.embedding, [1, 2, 3]) })
      .from(docs)
      .orderBy(sql.asc(l2Distance(docs.embedding, [1, 2, 3])))
      .limit(3);
    out.vectorFailClosed = { rejected: false };
  } catch (err) {
    out.vectorFailClosed = {
      rejected: true,
      capabilityError: err instanceof CapabilityRequirementError,
      failing:
        err instanceof CapabilityRequirementError
          ? err.results.filter((r) => r.status !== "supported").map((r) => `${r.capability}: ${r.status}`)
          : String(err).slice(0, 200),
      loggedStatementsDuringAttempt: statements.count - before,
    };
  }

  // FTS capability: probe-resolved on this engine.
  const ftsEv = await resolveCapabilityStatus(out.engine, "fts-functions", async (sqlText) => {
    await db.driver.query(sqlText);
  });
  out.ftsCapability = { status: ftsEv.status, evidence: ftsEv.evidence.slice(0, 200) };

  // An FTS query through the ORM must equally fail closed BEFORE any SQL.
  const ftsBefore = statements.count;
  try {
    await db
      .select({ id: docs.id, rank: sqlFts.tsRank(sqlFts.toTsvector("english", docs.id), sqlFts.websearchToTsquery("english", "x")) })
      .from(docs)
      .limit(1);
    out.ftsFailClosed = { rejected: false };
  } catch (err) {
    out.ftsFailClosed = {
      rejected: true,
      capabilityError: err instanceof CapabilityRequirementError,
      failing:
        err instanceof CapabilityRequirementError
          ? err.results.filter((r) => r.status !== "supported").map((r) => `${r.capability}: ${r.status}`)
          : String(err).slice(0, 200),
      loggedStatementsDuringAttempt: statements.count - ftsBefore,
    };
  }

  // Raw engine probes (recorded, whatever the answer is).
  out.rawProbes = {};
  const raw = async (name, sqlText) => {
    try {
      const rows = await db.driver.query(sqlText);
      out.rawProbes[name] = { ok: true, sample: JSON.stringify(rows[0] ?? null).slice(0, 120) };
    } catch (err) {
      out.rawProbes[name] = { ok: false, error: String(err?.message ?? err).slice(0, 160) };
    }
  };
  await raw("vector-cast", "select '[1]'::vector as v");
  await raw("vector-l2-op", "select ('[1]'::vector <-> '[2]'::vector) as d");
  await raw("to_tsvector", "select to_tsvector('english', 'neutron database') as v");
  await raw("ts_rank", "select ts_rank(to_tsvector('english', 'a b'), plainto_tsquery('english', 'a')) as r");
  await raw("pg_extension", "select count(*) as n from pg_catalog.pg_extension");
  await raw("create-extension", "create extension vector");

  await db.driver.close();
} catch (err) {
  out.harnessError = String(err?.message ?? err).slice(0, 300);
} finally {
  child.kill("SIGKILL");
  await sleep(300);
  try {
    rmSync(dataDir, { recursive: true, force: true });
  } catch {}
}

console.log(JSON.stringify(out, null, 2));
