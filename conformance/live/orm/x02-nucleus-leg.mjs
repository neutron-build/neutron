// X02 Nucleus leg — documents and graph relationships against the REAL
// engine build (never mocked). Exercises the card's V18 exit: two-tenant
// isolation, relationship traversal and cross-model failure cases, plus the
// capability gate, transaction scope, restart durability and permission
// surface — and a plain-PostgreSQL control proving the new surfaces fail
// closed there with zero statements.
//
// Every observation is a verdict. A verdict about a capability the client
// does NOT advertise passes when the engine still behaves as recorded
// (e.g. dirty reads observed while `specialty-session-isolation` is recorded
// unsupported); if the engine changes, the verdict fails and the capability
// record must be re-reviewed — same rule as `run.mjs --check`.
//
// Usage:
//   node conformance/live/orm/x02-nucleus-leg.mjs <nucleus-binary> [--port N] [--out file.json]
//   NEUTRON_TEST_DATABASE_URL=postgres://... enables the PostgreSQL control;
//   without it the control is recorded as skipped (and reported on stderr).
// Requires a built @neutron-build/nucleus (pnpm --filter @neutron-build/nucleus build).
// Exit status: 0 only when the engine started, nothing threw, and EVERY
// verdict passed; 1 otherwise. The JSON goes to --out (or stdout).
// Leaves nothing behind: engines killed, data dir removed, the PostgreSQL
// control only reads (VERSION()).
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { createRequire } from "node:module";
import path from "node:path";

const args = process.argv.slice(2);
const BIN = args.find((a) => !a.startsWith("--") && !/^\d+$/.test(a) && !a.endsWith(".json"));
const flag = (name) => {
  const i = args.indexOf(name);
  return i >= 0 ? args[i + 1] : undefined;
};
if (!BIN) {
  console.error("usage: node x02-nucleus-leg.mjs <nucleus-binary> [--port N] [--out file.json]");
  process.exit(2);
}
const PORT = Number(flag("--port") ?? 55942);
const OUT = flag("--out");
const DIST = path.resolve(new URL("../../../typescript/packages/neutron-nucleus/dist", import.meta.url).pathname);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const dataDir = mkdtempSync(path.join(tmpdir(), "x02-nucleus-"));
// Same watermark override as conformance/live/scripts/start-engine.sh: a
// near-full developer disk must not turn into a conformance failure.
writeFileSync(
  path.join(dataDir, "neutron.toml"),
  "[storage]\ndisk_warn_free_pct = 0.5\ndisk_readonly_free_pct = 0.1\ndisk_min_free_mb = 128\n",
);

let child = null;
async function startEngine() {
  const proc = spawn(BIN, ["start", "--port", String(PORT), "--data", dataDir], {
    env: { ...process.env },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let log = "";
  proc.stdout.on("data", (d) => (log += d));
  proc.stderr.on("data", (d) => (log += d));
  for (let i = 0; i < 80; i++) {
    await sleep(250);
    if (log.includes(`Listening on 127.0.0.1:${PORT}`)) return { proc, up: true, log };
    if (proc.exitCode !== null) break;
  }
  return { proc, up: false, log };
}
async function killEngine() {
  if (child && child.exitCode === null) {
    child.kill("SIGKILL");
    for (let i = 0; i < 40 && child.exitCode === null; i++) await sleep(50);
  }
}

const url = `postgres://127.0.0.1:${PORT}/postgres`;
const out = { leg: "x02-nucleus", tz: process.env.TZ ?? "system", port: PORT };
const verdicts = [];
function verdict(pass, detail) {
  const v = { pass: Boolean(pass), detail };
  verdicts.push(v);
  return v;
}
async function rejects(fn, test) {
  try {
    await fn();
    return { rejected: false };
  } catch (e) {
    return { rejected: true, ok: test(e), error: `${e?.name}: ${String(e?.message ?? e).slice(0, 200)}` };
  }
}

const pgLib = createRequire(path.join(DIST, "index.js"))("pg");

/** A Transport over ONE pg.Client (session state such as SET ROLE stays put). */
function singleConnectionTransport(client) {
  const run = (sql, params = []) => client.query(sql, params);
  return {
    query: async (sql, params) => {
      const r = await run(sql, params);
      return { rows: r.rows, rowCount: r.rowCount ?? 0 };
    },
    execute: async (sql, params) => (await run(sql, params)).rowCount ?? 0,
    fetchval: async (sql, params) => {
      const row = (await run(sql, params)).rows[0];
      return row === undefined ? null : (Object.values(row)[0] ?? null);
    },
    beginTransaction: async () => {
      throw new Error("not used");
    },
    close: async () => {},
    ping: async () => {},
  };
}

/** Wrap a transport so every statement is logged (live statement-counter proofs). */
function counting(inner) {
  const log = [];
  const wrap = (fn) => async (sql, params) => {
    log.push(sql);
    return inner[fn](sql, params);
  };
  return {
    log,
    transport: {
      query: wrap("query"),
      execute: wrap("execute"),
      fetchval: wrap("fetchval"),
      beginTransaction: (lvl) => inner.beginTransaction(lvl),
      close: () => inner.close(),
      ping: () => inner.ping(),
    },
  };
}

try {
  const started = await startEngine();
  child = started.proc;
  out.started = started.up;
  if (!started.up) throw new Error(`engine did not start: ${started.log.slice(-400)}`);

  const { PgTransport } = await import(path.join(DIST, "transport.js"));
  const { detectFeatures } = await import(path.join(DIST, "features.js"));
  const { withGraph } = await import(path.join(DIST, "graph/index.js"));
  const { withDocument, DocumentValidationError, NucleusCapabilityError } = await import(path.join(DIST, "document/index.js"));
  const { NucleusFeatureError, NucleusNotFoundError, NucleusConflictError } = await import(path.join(DIST, "errors.js"));

  let base = new PgTransport(url);
  const { log: stmts, transport: t } = counting(base);
  const features = await detectFeatures(t);
  let featuresFn = "present";
  try {
    await base.fetchval("SELECT NUCLEUS_FEATURES()");
  } catch (e) {
    featuresFn = `absent (${String(e.message).slice(0, 60)}) — per-model flags come from the all-enabled fallback`;
  }
  out.engine = { version: features.version, isNucleus: features.isNucleus, nucleusFeatures: featuresFn };

  const graph = withGraph.init(t, features).graph;
  const doc = withDocument.init(t, features).document;

  // -----------------------------------------------------------------------
  // Capability gate: probe-resolved on this build, measured-absent recorded.
  // -----------------------------------------------------------------------
  {
    const before = stmts.length;
    const g = await graph.capabilities();
    const d = await doc.capabilities();
    const probeSql = stmts.slice(before);
    const status = Object.fromEntries([...g, ...d].map((e) => [e.capability, e.status]));
    out.capabilities = {
      report: [...g, ...d.filter((e) => e.capability === "document-collections")],
      probed: verdict(
        status["document-collections"] === "supported" &&
          status["graph-adjacency"] === "supported" &&
          status["graph-property-match"] === "supported",
        "document-collections, graph-adjacency, graph-property-match resolve supported by read-only probes",
      ),
      measuredAbsent: verdict(
        ["graph-tenant-isolation", "graph-query-parameters", "graph-multi-label", "specialty-session-isolation", "atomic-sql-specialty-writes"].every(
          (c) => status[c] === "unsupported",
        ),
        "the five unadvertised capabilities report unsupported",
      ),
      probesReadOnly: verdict(
        probeSql.length === 6 && probeSql.every((s) => /^SELECT (DOC_COUNT|DOC_GET|DOC_PATH_IN|GRAPH_NODE|GRAPH_NEIGHBORS|GRAPH_QUERY)\(/.test(s)),
        `probe statements: ${JSON.stringify(probeSql)}`,
      ),
    };
  }

  // -----------------------------------------------------------------------
  // Client defects D1-D4 (fail-before recorded in the private evidence).
  // -----------------------------------------------------------------------
  out.defectsAfter = {};
  {
    const a = await graph.addNode(["N"]);
    const b = await graph.addNode(["N"]);
    const c = await graph.addNode(["N"]);
    await graph.addEdge(a, b, "NEXT");
    await graph.addEdge(b, c, "NEXT");
    const rawIgnoresBound = await base.fetchval("SELECT GRAPH_SHORTEST_PATH($1, $2, $3)", [a, c, 1]);
    const bounded = await graph.shortestPath(a, c, 1);
    const unbounded = await graph.shortestPath(a, c);
    out.defectsAfter.D1_boundedShortestPath = verdict(
      bounded.length === 0 && unbounded.length === 3 && JSON.parse(rawIgnoresBound).length === 3,
      `engine GRAPH_SHORTEST_PATH(a,c,1)=${rawIgnoresBound} (bound ignored); client maxDepth=1 -> ${JSON.stringify(bounded)}; unbounded -> ${JSON.stringify(unbounded)}`,
    );
    const d2 = await rejects(
      () => graph.addEdge(a, 999999, "TO_MISSING"),
      (e) => e instanceof NucleusNotFoundError && /to node 999999 does not exist/.test(e.message),
    );
    out.defectsAfter.D2_missingEndpointNamed = verdict(d2.rejected && d2.ok, d2.error);
    const nodesBefore = await graph.nodeCount();
    const d3 = await rejects(
      () => graph.addNode(["A", "B"]),
      (e) => e instanceof NucleusCapabilityError && e.capability === "graph-multi-label",
    );
    out.defectsAfter.D3_multiLabelRejected = verdict(d3.rejected && d3.ok && (await graph.nodeCount()) === nodesBefore, d3.error);
    const d4 = await rejects(
      () => graph.query("MATCH (n) WHERE n.x = $x RETURN n", { x: 1 }),
      (e) => e instanceof NucleusCapabilityError && e.capability === "graph-query-parameters",
    );
    out.defectsAfter.D4_paramsFailClosed = verdict(d4.rejected && d4.ok, d4.error);
  }

  // -----------------------------------------------------------------------
  // Documents — round trip, validation, boundaries, two-tenant isolation.
  // -----------------------------------------------------------------------
  out.documents = {};
  {
    const schema = {
      fields: {
        id: { type: "integer" },
        login: { type: "string" },
        score: { type: "number", optional: true },
        tags: { type: "array", optional: true, items: { type: "string" } },
        meta: { type: "object", optional: true, fields: { origin: { type: "string", optional: true } } },
      },
    };
    const tenantA = doc.collection("x02_tenant_a", { schema, boundTo: { table: "x02_users" } });
    const tenantB = doc.collection("x02_tenant_b", { schema });

    const aId = await tenantA.insert({
      id: 9007199254740991, // MAX_SAFE_INTEGER: the exact f64 boundary
      login: "ada",
      score: 0.5,
      tags: ["founder", "x02"],
      meta: { origin: "live-leg" },
    });
    const readBack = await tenantA.get(aId);
    out.documents.roundTrip = verdict(
      readBack?.id === 9007199254740991 && readBack.login === "ada" && readBack.score === 0.5 && readBack.tags?.[1] === "x02" && readBack.meta?.origin === "live-leg",
      `id=${aId} -> ${JSON.stringify(readBack)}`,
    );

    const before = stmts.length;
    const invalid = await rejects(
      () => tenantA.insert({ id: "one", login: 5, tags: ["ok", 7] }),
      (e) => e instanceof DocumentValidationError && e.issues.map((i) => i.path).join(",") === "$.id,$.login,$.tags[1]",
    );
    const beyond = await rejects(
      () => tenantA.insert({ id: 2 ** 53 + 2, login: "x" }),
      (e) => e instanceof DocumentValidationError && /would round it/.test(e.issues[0].expected),
    );
    out.documents.validationFailsClosed = verdict(
      invalid.ok && beyond.ok && stmts.length === before,
      `${invalid.error} | ${beyond.error} | statements sent: ${stmts.length - before}`,
    );

    const rawId = await base.fetchval("SELECT DOC_INSERT($1, $2)", ["x02_raw", JSON.stringify({ n: 2 ** 53 + 1 })]);
    const rawDoc = JSON.parse(await base.fetchval("SELECT DOC_GET($1, $2)", ["x02_raw", String(rawId)]));
    out.documents.engineF64Coercion = verdict(rawDoc.n === 2 ** 53, `raw 2^53+1 stored as ${rawDoc.n} (the reason integer fields stop at MAX_SAFE_INTEGER)`);

    const updated = await tenantA.update(aId, { score: 0.75 });
    const afterUpdate = await tenantA.get(aId);
    const badUpdate = await rejects(() => tenantA.update(aId, { login: 42 }), (e) => e instanceof DocumentValidationError);
    const afterBad = await tenantA.get(aId);
    out.documents.updateMergesAndValidates = verdict(
      updated === true && afterUpdate.score === 0.75 && afterUpdate.login === "ada" && badUpdate.ok && afterBad.login === "ada",
      `merged=${JSON.stringify(afterUpdate)}; invalid patch rejected, stored doc untouched`,
    );

    const bId = await tenantB.insert({ id: 2, login: "bob" });
    const defaultId = await doc.insert("", { login: "default-collection" });
    out.documents.twoTenantIsolation = {
      crossGet: verdict((await tenantB.get(aId)) === null && (await tenantA.get(bId)) === null, "neither tenant reads the other's id"),
      crossUpdate: verdict((await tenantB.update(aId, { login: "mallory" })) === false && (await tenantA.get(aId)).login === "ada", "cross update reports absent and changes nothing"),
      crossDelete: verdict((await tenantB.delete(aId)) === false && (await tenantA.get(aId)) !== null, "cross delete reports absent and deletes nothing"),
      crossPath: verdict((await tenantB.path(aId, "login")) === null, "cross path read is null"),
      counts: verdict((await tenantA.count()) === 1 && (await tenantB.count()) === 1, "per-collection counts"),
      queryScoped: verdict(
        (await tenantB.query({ login: "ada" })).length === 0 && (await tenantA.query({ login: "ada" })).length === 1,
        "containment queries never cross collections",
      ),
      defaultCollectionSeparate: verdict(
        (await tenantA.get(defaultId)) === null && (await doc.get(aId)) === null,
        "the unnamed default collection is a separate scope in both directions",
      ),
    };

    out.documents.missingAndDeleted = {
      neverExisted: verdict(
        (await tenantA.get(424242)) === null &&
          (await tenantA.update(424242, { score: 1 })) === false &&
          (await tenantA.delete(424242)) === false &&
          (await tenantA.path(424242, "login")) === null,
        "get null / update false / delete false / path null",
      ),
      deleted: verdict(
        (await tenantB.delete(bId)) === true &&
          (await tenantB.get(bId)) === null &&
          (await tenantB.update(bId, { login: "x" })) === false &&
          (await tenantB.delete(bId)) === false,
        "after delete: get null, update false, second delete false",
      ),
      pathAbsent: verdict((await tenantA.path(aId, "no_such_key")) === null, "absent path on a present document is null"),
    };
  }

  // -----------------------------------------------------------------------
  // Graph — traversal oracle, cycles and bounds.
  // -----------------------------------------------------------------------
  out.graph = {};
  {
    // Fixture (hand oracle):
    //   n0 -> n1 -> n2 -> n3   NEXT chain (n3 at depth 3)
    //   n0 -> n4               SIDE (depth 1)
    //   n3 -> n0               BACK (cycle)
    //   n4 -> n4               LOOP (self)
    //   n1 -> n5               OUT_ONLY (depth 2)
    //   n5 -> n0               FROM5 (cycle back to the start)
    const n = [];
    for (let i = 0; i < 6; i++) n.push(await graph.addNode(["X02"]));
    const [n0, n1, n2, n3, n4, n5] = n;
    for (const [f, to, ty] of [
      [n0, n1, "NEXT"], [n1, n2, "NEXT"], [n2, n3, "NEXT"], [n0, n4, "SIDE"],
      [n3, n0, "BACK"], [n4, n4, "LOOP"], [n1, n5, "OUT_ONLY"], [n5, n0, "FROM5"],
    ]) {
      await graph.addEdge(f, to, ty);
    }

    const d2 = await graph.traverse(n0, { maxDepth: 2 });
    const depth = Object.fromEntries(d2.nodes.map((x) => [x.id, x.depth]));
    out.graph.depthOracle = verdict(
      d2.startPresent && depth[n1] === 1 && depth[n4] === 1 && depth[n2] === 2 && depth[n5] === 2 && depth[n3] === undefined && !d2.truncated && d2.nodes.length === 4,
      `maxDepth 2: ${JSON.stringify(d2.nodes.map((x) => [x.id, x.depth, x.viaEdgeType]))}`,
    );

    const full = await graph.traverse(n0, { maxDepth: 32 });
    const ids = full.nodes.map((x) => x.id).sort((x, y) => x - y);
    out.graph.cycleSafety = verdict(
      JSON.stringify(ids) === JSON.stringify([n1, n2, n3, n4, n5].sort((x, y) => x - y)) && full.statements === 1 + 6,
      `visited ${JSON.stringify(ids)}; statements ${full.statements} = 1 GRAPH_NODE + 6 expansions (each node once despite BACK/LOOP/FROM5)`,
    );

    const typed = await graph.traverse(n0, { maxDepth: 3, edgeTypes: ["NEXT"] });
    out.graph.edgeTypeFilter = verdict(
      JSON.stringify(typed.nodes.map((x) => x.id)) === JSON.stringify([n1, n2, n3]),
      `NEXT only: ${JSON.stringify(typed.nodes.map((x) => x.id))}`,
    );

    const inward = await graph.traverse(n5, { maxDepth: 1, direction: "in" });
    const both = await graph.traverse(n5, { maxDepth: 1, direction: "both" });
    out.graph.direction = verdict(
      JSON.stringify(inward.nodes.map((x) => x.id)) === JSON.stringify([n1]) &&
        JSON.stringify(both.nodes.map((x) => x.id).sort((x, y) => x - y)) === JSON.stringify([n0, n1].sort((x, y) => x - y)),
      `in: ${JSON.stringify(inward.nodes.map((x) => x.id))}; both: ${JSON.stringify(both.nodes.map((x) => x.id))}`,
    );

    const budget = await graph.traverse(n0, { maxDepth: 32, maxNodes: 2 });
    const before = stmts.length;
    const badBounds = [];
    for (const bad of [{}, { maxDepth: 0 }, { maxDepth: 33 }, { maxDepth: 1.5 }, { maxDepth: 2, maxNodes: 0 }, { maxDepth: 2, maxNodes: 10001 }]) {
      badBounds.push((await rejects(() => graph.traverse(n0, bad), (e) => /maxDepth|maxNodes/.test(e.message))).ok === true);
    }
    out.graph.bounds = {
      budgetTruncates: verdict(budget.truncated && budget.nodes.length === 2 && budget.nodes[0].depth === 1, `maxNodes 2 -> ${JSON.stringify(budget.nodes.map((x) => [x.id, x.depth]))}`),
      invalidBoundsRejectedWithoutStatements: verdict(badBounds.every(Boolean) && stmts.length === before, `${badBounds.length} invalid bound sets, statements sent: ${stmts.length - before}`),
    };

    const bounded3 = await graph.shortestPath(n0, n3, 3);
    const engine = await graph.shortestPath(n0, n3);
    const bounded2 = await graph.shortestPath(n0, n3, 2);
    out.graph.boundedShortestPath = verdict(
      JSON.stringify(bounded3) === JSON.stringify([n0, n1, n2, n3]) && JSON.stringify(engine) === JSON.stringify(bounded3) && bounded2.length === 0,
      `bound 3 -> ${JSON.stringify(bounded3)}; engine unbounded -> ${JSON.stringify(engine)}; bound 2 -> ${JSON.stringify(bounded2)}`,
    );

    const missing = await graph.traverse(987654321, { maxDepth: 2 });
    out.graph.missingStart = verdict(missing.startPresent === false && missing.nodes.length === 0 && missing.statements === 1, "startPresent false after one GRAPH_NODE read");

    const edgesBefore = await graph.edgeCount();
    await graph.deleteNode(n5);
    const afterDelete = await graph.traverse(n1, { maxDepth: 1 });
    out.graph.deletedNode = verdict(
      (await graph.edgeCount()) === edgesBefore - 2 && !afterDelete.nodes.some((x) => x.id === n5) && (await graph.traverse(n5, { maxDepth: 1 })).startPresent === false,
      "deleting a node removes its two edges; traversal no longer reaches it; traversing from it reports startPresent false",
    );
  }

  // -----------------------------------------------------------------------
  // SQL-ID tying + cross-model failure cases.
  // -----------------------------------------------------------------------
  out.sqlTying = {};
  {
    await base.execute("CREATE TABLE x02_users (id bigint primary key, login text)");
    await base.execute("INSERT INTO x02_users VALUES (1, 'ada'), (2, 'grace'), (3, 'linus')");

    const before = stmts.length;
    const users = graph.sqlNodes({ table: "x02_users" });
    const bindStatements = stmts.length - before;
    const u1 = await users.addRowNode(1, "User", { note: "founder", sqlref_row: 999 });
    const u2 = await users.addRowNode(2, "User");
    const u3 = await users.addRowNode(3, "User");
    await users.addRowEdge(1, 2, "FOLLOWS");
    await users.addRowEdge(2, 3, "FOLLOWS");
    const u1Node = JSON.parse(await base.fetchval("SELECT GRAPH_NODE($1)", [String(u1)]));
    const dup = await rejects(() => users.addRowNode(2, "User"), (e) => e instanceof NucleusConflictError && /already has node/.test(e.message));
    const tyingStatements = stmts.slice(before);
    out.sqlTying.binding = {
      zeroStatementsAtBind: verdict(bindStatements === 0, `sqlNodes() sent ${bindStatements} statements`),
      noDdlEver: verdict(
        tyingStatements.every((s) => !/\b(CREATE|ALTER|DROP)\b/i.test(s)),
        `${tyingStatements.length} statements through the binding, none DDL`,
      ),
      stampWinsOverUserProps: verdict(u1Node.properties.sqlref_row === 1 && u1Node.properties.note === "founder", JSON.stringify(u1Node.properties)),
      oneNodePerRow: verdict(dup.ok && (await users.findNodeByRow(2)) === u2, dup.error),
    };

    // Cross-model failure: the row is deleted after its node exists.
    await base.execute("DELETE FROM x02_users WHERE id = 3");
    const hydrated = await users.hydrate([u1, u2, u3, 987654321]);
    out.sqlTying.hydrate = {
      liveRows: verdict(hydrated[0].row?.login === "ada" && hydrated[1].row?.login === "grace", JSON.stringify(hydrated.slice(0, 2).map((h) => h.row))),
      deletedRowIsNull: verdict(hydrated[2].sqlRef?.id === 3 && hydrated[2].row === null, "deleted SQL row -> row null, sqlRef intact (data, not an error)"),
      missingNode: verdict(hydrated[3].sqlRef === null && hydrated[3].row === null, "a node id that does not exist hydrates to nothing"),
      deletedRowNodeStillFound: verdict((await users.findNodeByRow(3)) === u3, "the graph keeps the node; the caller decides"),
    };

    const reach = await graph.traverse(u1, { maxDepth: 2, edgeTypes: ["FOLLOWS"] });
    const walked = await users.hydrate(reach.nodes.map((x) => x.id));
    out.sqlTying.traversalTiedToSqlIds = verdict(
      JSON.stringify(walked.map((h) => [h.sqlRef?.id, h.row?.login ?? null])) === JSON.stringify([[2, "grace"], [3, null]]),
      `walked FOLLOWS from row 1: ${JSON.stringify(walked.map((h) => [h.sqlRef?.id, h.row?.login ?? null]))}`,
    );

    const noNode = await rejects(() => users.addRowEdge(1, 999, "FOLLOWS"), (e) => e instanceof NucleusNotFoundError && /row 999 of "x02_users" has no node/.test(e.message));
    out.sqlTying.edgeToRowWithoutNode = verdict(noNode.ok, noNode.error);

    // Another table's binding never claims these nodes.
    const other = graph.sqlNodes({ table: "x02_orders" });
    const foreign = await other.hydrate([u1]);
    out.sqlTying.foreignBindingIgnoresStamp = verdict(foreign[0].sqlRef === null && (await other.findNodeByRow(1)) === null, "x02_orders binding sees no identity on x02_users nodes");

    await base.execute("DROP TABLE x02_users");
  }

  // -----------------------------------------------------------------------
  // Graph two-tenant isolation: measured absent (never advertised).
  // -----------------------------------------------------------------------
  {
    const tA = await graph.addNode(["TenantA"]);
    const tB = await graph.addNode(["TenantB"]);
    await graph.addEdge(tA, tB, "CROSSES");
    const reach = await graph.traverse(tA, { maxDepth: 1 });
    out.graphTenantIsolation = verdict(
      reach.nodes.some((x) => x.id === tB),
      "recorded unsupported and still true: tenant A's traversal reaches tenant B's node — one global graph, isolate graph tenants with separate engines",
    );
    await graph.deleteNode(tA);
    await graph.deleteNode(tB);
  }

  // -----------------------------------------------------------------------
  // Transaction scope (engine behaviour; the client offers no SQL+graph
  // transaction and advertises no atomicity).
  // -----------------------------------------------------------------------
  out.transactions = {};
  {
    await base.execute("CREATE TABLE x02_tx (id int primary key)");
    const nodeGone = async (id) => (await base.fetchval("SELECT GRAPH_NODE($1)", [String(id)])) === null;
    const docGone = async (id) => (await base.fetchval("SELECT DOC_GET($1, $2)", ["x02_tx", String(id)])) === null;
    const rows = async () => Number(await base.fetchval("SELECT COUNT(*) FROM x02_tx"));

    // a) ROLLBACK after SQL + graph + document writes on one connection.
    let tx = await base.beginTransaction();
    await tx.execute("INSERT INTO x02_tx VALUES (1)");
    let node = await tx.fetchval("SELECT GRAPH_ADD_NODE($1)", ["TX"]);
    let docId = await tx.fetchval("SELECT DOC_INSERT($1, $2)", ["x02_tx", '{"m":1}']);
    await tx.rollback();
    out.transactions.rollbackRevertsAll = verdict(
      (await rows()) === 0 && (await nodeGone(node)) && (await docGone(docId)),
      "SQL row, graph node and document all absent after ROLLBACK",
    );

    // b) A failing SQL statement after the specialty writes, then ROLLBACK.
    tx = await base.beginTransaction();
    node = await tx.fetchval("SELECT GRAPH_ADD_NODE($1)", ["TX"]);
    docId = await tx.fetchval("SELECT DOC_INSERT($1, $2)", ["x02_tx", '{"m":2}']);
    await tx.execute("INSERT INTO x02_tx VALUES (7)");
    let failed = null;
    try {
      await tx.execute("INSERT INTO x02_tx VALUES (7)");
    } catch (e) {
      failed = String(e.code ?? e.message);
    }
    await tx.rollback();
    out.transactions.failureThenRollback = verdict(
      failed !== null && (await rows()) === 0 && (await nodeGone(node)) && (await docGone(docId)),
      `duplicate key (${failed}) then ROLLBACK: no row, no node, no document`,
    );

    // c) COMMIT makes all three visible.
    tx = await base.beginTransaction();
    await tx.execute("INSERT INTO x02_tx VALUES (2)");
    node = await tx.fetchval("SELECT GRAPH_ADD_NODE($1)", ["TX"]);
    docId = await tx.fetchval("SELECT DOC_INSERT($1, $2)", ["x02_tx", '{"m":3}']);
    await tx.commit();
    out.transactions.commitPublishesAll = verdict(
      (await rows()) === 1 && !(await nodeGone(node)) && !(await docGone(docId)),
      "row, node and document visible after COMMIT",
    );

    // d) Dirty reads: the reason atomic SQL+graph writes stay unadvertised.
    tx = await base.beginTransaction();
    await tx.execute("INSERT INTO x02_tx VALUES (3)");
    node = await tx.fetchval("SELECT GRAPH_ADD_NODE($1)", ["DIRTY"]);
    docId = await tx.fetchval("SELECT DOC_INSERT($1, $2)", ["x02_tx", '{"m":4}']);
    const other = new PgTransport(url);
    const otherRows = Number(await other.fetchval("SELECT COUNT(*) FROM x02_tx WHERE id = 3"));
    const otherNode = await other.fetchval("SELECT GRAPH_NODE($1)", [String(node)]);
    const otherDoc = await other.fetchval("SELECT DOC_GET($1, $2)", ["x02_tx", String(docId)]);
    await tx.rollback();
    await other.close();
    out.transactions.dirtyReads = verdict(
      otherRows === 0 && otherNode !== null && otherDoc !== null,
      `second session during the open transaction: SQL row visible=${otherRows !== 0}, graph node visible=${otherNode !== null}, document visible=${otherDoc !== null} — specialty-session-isolation and atomic-sql-specialty-writes stay unsupported`,
    );
    await base.execute("DROP TABLE x02_tx");
  }

  // -----------------------------------------------------------------------
  // Permission surface.
  // -----------------------------------------------------------------------
  out.permission = {};
  {
    await base.execute("CREATE TABLE x02_guard (id int primary key, tenant int)");
    await base.execute("INSERT INTO x02_guard VALUES (1, 1), (2, 2)");
    await base.execute("CREATE ROLE x02_app LOGIN PASSWORD 'x02-leg'");
    await base.execute("GRANT SELECT ON x02_guard TO x02_app");
    await base.execute("ALTER TABLE x02_guard ENABLE ROW LEVEL SECURITY");
    await base.execute("CREATE POLICY x02_p ON x02_guard FOR SELECT TO x02_app USING (tenant = 1)");

    // One dedicated connection (not a pool: SET ROLE is per connection, and
    // it persists past transaction end on this engine, X00 N1), closed after.
    const roleClient = new pgLib.Client({ connectionString: url });
    roleClient.on("error", () => {});
    await roleClient.connect();
    const roleT = singleConnectionTransport(roleClient);
    await roleT.execute("SET ROLE x02_app");
    const asRole = await roleT.fetchval("SELECT CURRENT_USER");
    const visible = Number(await roleT.fetchval("SELECT COUNT(*) FROM x02_guard"));
    const rawRefusal = await rejects(() => roleT.fetchval("SELECT DOC_COUNT($1)", ["x02_tenant_a"]), (e) => /row-level security is active/.test(e.message));
    const graphRefusal = await rejects(() => roleT.fetchval("SELECT GRAPH_NODE_COUNT()"), (e) => /row-level security is active/.test(e.message));
    const roleFeatures = await detectFeatures(roleT);
    const roleDoc = withDocument.init(roleT, roleFeatures).document;
    const gated = await rejects(
      () => roleDoc.collection("x02_tenant_a").count(),
      (e) => e instanceof NucleusCapabilityError && e.capability === "document-collections" && /row-level security/.test(e.evidence),
    );
    await roleClient.end();
    out.permission.rlsPrincipal = {
      policyFilters: verdict(asRole === "x02_app" && visible === 1, `current_user ${asRole}; rows visible under USING (tenant = 1): ${visible} of 2`),
      specialtyRefusedUnderRls: verdict(rawRefusal.ok && graphRefusal.ok, `${rawRefusal.error} | ${graphRefusal.error}`),
      clientFailsClosed: verdict(gated.ok, `collection.count() under the RLS principal: ${gated.error}`),
    };

    // Default (passwordless) deployments run every login as the bootstrap
    // superuser, so neither RLS nor the specialty guard engages: collection
    // names are namespaces chosen by the application, not permissions.
    const loginAs = new PgTransport(`postgres://x02_app:x02-leg@127.0.0.1:${PORT}/postgres`);
    const loginUser = await loginAs.fetchval("SELECT CURRENT_USER");
    const loginDocCount = await loginAs.fetchval("SELECT DOC_COUNT($1)", ["x02_tenant_a"]);
    await loginAs.close();
    out.permission.passwordlessLogin = verdict(
      loginUser === "nucleus" && Number(loginDocCount) === 1,
      `login as x02_app runs as ${loginUser} and reads another tenant's collection (count ${loginDocCount}) — collections isolate by name only`,
    );
    await base.execute("DROP POLICY x02_p ON x02_guard");
    await base.execute("DROP TABLE x02_guard");
  }

  // -----------------------------------------------------------------------
  // Restart durability: committed writes survive SIGKILL; an open
  // transaction's specialty writes do not.
  // -----------------------------------------------------------------------
  {
    const durDoc = await doc.collection("x02_restart").insert({ marker: "committed-doc" });
    const durNode = await graph.addNode(["RESTART"], { marker: "committed-node" });
    // A raw pg client (with its own error listener) holds the open
    // transaction: the engine dies under it, which a pooled transaction
    // client would surface as an unhandled 'error' event.
    const openClient = new pgLib.Client({ connectionString: url });
    openClient.on("error", () => {});
    await openClient.connect();
    await openClient.query("BEGIN");
    const openNode = Number((await openClient.query("SELECT GRAPH_ADD_NODE($1) AS id", ["OPEN"])).rows[0].id);
    const openDoc = Number((await openClient.query("SELECT DOC_INSERT($1, $2) AS id", ["x02_restart", '{"marker":"open-tx-doc"}'])).rows[0].id);
    await killEngine();
    await openClient.end().catch(() => {});
    await base.close().catch(() => {});
    const again = await startEngine();
    child = again.proc;
    out.restart = { engineCameBack: verdict(again.up, "same data dir after SIGKILL") };
    const t2 = new PgTransport(url);
    const docBack = await t2.fetchval("SELECT DOC_GET($1, $2)", ["x02_restart", String(durDoc)]);
    const nodeBack = await t2.fetchval("SELECT GRAPH_NODE($1)", [String(durNode)]);
    out.restart.committedDocument = verdict(docBack !== null && JSON.parse(docBack).marker === "committed-doc", docBack ?? "lost");
    out.restart.committedGraph = verdict(nodeBack !== null && JSON.parse(nodeBack).properties.marker === "committed-node", nodeBack ?? "lost");
    const openNodeBack = await t2.fetchval("SELECT GRAPH_NODE($1)", [String(openNode)]);
    const openDocBack = await t2.fetchval("SELECT DOC_GET($1, $2)", ["x02_restart", String(openDoc)]);
    out.restart.openTransactionDiscarded = verdict(
      openNodeBack === null && openDocBack === null,
      `open-transaction node after restart: ${openNodeBack ?? "gone"}; document: ${openDocBack ?? "gone"}`,
    );
    await t2.close();
  }

  // -----------------------------------------------------------------------
  // PostgreSQL control: the new surfaces fail closed with zero statements.
  // -----------------------------------------------------------------------
  const pgUrl = process.env.NEUTRON_TEST_DATABASE_URL ?? "";
  if (pgUrl === "") {
    out.pgControl = { skipped: "NEUTRON_TEST_DATABASE_URL not set" };
    console.error("x02 leg: PostgreSQL control SKIPPED (NEUTRON_TEST_DATABASE_URL not set)");
  } else {
    const pgBase = new PgTransport(pgUrl);
    const { log: pgStmts, transport: pgT } = counting(pgBase);
    const pgFeatures = await detectFeatures(pgT);
    const afterDetect = pgStmts.length;
    const pgDoc = withDocument.init(pgT, pgFeatures).document;
    const pgGraph = withGraph.init(pgT, pgFeatures).graph;
    const isFeature = (e) => e instanceof NucleusFeatureError;
    const cases = {
      collectionInsert: await rejects(() => pgDoc.collection("x").insert({ a: 1 }), isFeature),
      collectionGet: await rejects(() => pgDoc.collection("x").get(1), isFeature),
      documentCapabilities: await rejects(() => pgDoc.capabilities(), isFeature),
      traverse: await rejects(() => pgGraph.traverse(1, { maxDepth: 1 }), isFeature),
      boundedShortestPath: await rejects(() => pgGraph.shortestPath(1, 2, 3), isFeature),
      graphCapabilities: await rejects(() => pgGraph.capabilities(), isFeature),
      sqlNodesBind: await rejects(async () => pgGraph.sqlNodes({ table: "t" }), isFeature),
    };
    await pgBase.close();
    out.pgControl = {
      engine: pgFeatures.version.slice(0, 60),
      failClosed: verdict(
        !pgFeatures.isNucleus && Object.values(cases).every((c) => c.ok) && pgStmts.length === afterDetect && afterDetect === 1,
        `${Object.keys(cases).length} calls -> NucleusFeatureError; statements: VERSION() only (${JSON.stringify(pgStmts)})`,
      ),
    };
  }
} catch (err) {
  out.fatal = String(err && err.stack ? err.stack : err).slice(0, 1200);
} finally {
  await killEngine();
  rmSync(dataDir, { recursive: true, force: true });
}

out.summary = { verdicts: verdicts.length, failed: verdicts.filter((v) => !v.pass).length, fatal: out.fatal ?? null };
const json = JSON.stringify(out, null, 2);
if (OUT) writeFileSync(OUT, json);
else console.log(json);
console.error(`x02 leg: ${out.summary.verdicts} verdicts, ${out.summary.failed} failed${out.fatal ? ", FATAL" : ""}`);
process.exit(out.started && !out.fatal && out.summary.failed === 0 ? 0 : 1);
