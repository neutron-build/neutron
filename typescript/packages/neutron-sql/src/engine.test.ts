import assert from "node:assert/strict";
import test from "node:test";
import {
  parseVersionString,
  resolveCapabilityStatus,
  capabilityGate,
  CapabilityRequirementError,
} from "./engine.js";
import {
  NeutronSqlError,
  MissingDriverError,
  ConnectionFailedError,
  ServerSqlError,
  classifyDriverError,
  connectionConstructionError,
  getSqlState,
  isConnectionError,
  isMissingDriverError,
  isModuleNotFoundError,
} from "./errors.js";
import { makeLifecycle, loadDriver, wrapPgPool, wrapPostgresJs, type Driver } from "./drivers.js";

// ---------------------------------------------------------------------------
// Engine identity (FRAMEWORK_CONTRACT.md §1)
// ---------------------------------------------------------------------------

test("I01: parseVersionString recognizes plain PostgreSQL strings", () => {
  const brew = parseVersionString("PostgreSQL 17.11 (Homebrew) aarch64-apple-darwin24.6.0, by Tom Lane");
  assert.equal(brew.product, "postgres");
  assert.equal(brew.version, "17.11");

  const debian = parseVersionString("PostgreSQL 16.4 (Debian 16.4-1.pgdg120+1) on x86_64-pc-linux-gnu");
  assert.equal(debian.product, "postgres");
  assert.equal(debian.version, "16.4");

  const majorOnly = parseVersionString("PostgreSQL 18 on x86_64-pc-linux-gnu, compiled by gcc");
  assert.equal(majorOnly.product, "postgres");
  assert.equal(majorOnly.version, "18");
});

test("I01: parseVersionString recognizes the documented Nucleus version string", () => {
  // Exact shape from FRAMEWORK_CONTRACT.md §1.
  const n = parseVersionString("PostgreSQL 16.0 (Nucleus 0.2.1 — The Definitive Database)");
  assert.equal(n.product, "nucleus");
  assert.equal(n.version, "0.2.1");
  assert.ok(n.raw.includes("Nucleus"));

  const noVersion = parseVersionString("PostgreSQL 16.0 (Nucleus — The Definitive Database)");
  assert.equal(noVersion.product, "nucleus");
  assert.equal(noVersion.version, "");
});

test("I01: parseVersionString marks unrecognized engines unknown, never postgres", () => {
  const crdb = parseVersionString("CockroachDB CCL v24.1.7 (x86_64-pc-linux-gnu, built 2024/01/01)");
  assert.equal(crdb.product, "unknown");
  assert.equal(crdb.version, "");
  assert.equal(parseVersionString("").product, "unknown");
});

// ---------------------------------------------------------------------------
// Tri-state capability resolution
// ---------------------------------------------------------------------------

test("I01: jsonb-functions resolves by documented PostgreSQL version fact", async () => {
  const modern = await resolveCapabilityStatus(parseVersionString("PostgreSQL 17.11"), "jsonb-functions");
  assert.equal(modern.status, "supported");
  assert.match(modern.evidence, /9\.4/);

  const old = await resolveCapabilityStatus(parseVersionString("PostgreSQL 9.2.24"), "jsonb-functions");
  assert.equal(old.status, "unsupported");
  assert.match(old.evidence, /9\.2\.24 < 9\.4/);
});

test("I01: Nucleus without probe evidence is unknown — not all-enabled", async () => {
  const nucleus = parseVersionString("PostgreSQL 16.0 (Nucleus 0.2.1 — The Definitive Database)");
  const noProbe = await resolveCapabilityStatus(nucleus, "jsonb-functions");
  assert.equal(noProbe.status, "unknown");
  assert.match(noProbe.evidence, /never assumed supported/);
});

test("I01: a successful probe proves support on engines without version facts", async () => {
  const nucleus = parseVersionString("PostgreSQL 16.0 (Nucleus 0.2.1 — The Definitive Database)");
  const probed = await resolveCapabilityStatus(nucleus, "jsonb-functions", async (sql) => {
    assert.match(sql, /^select to_jsonb/);
  });
  assert.equal(probed.status, "supported");
  assert.match(probed.evidence, /probe succeeded on nucleus 0\.2\.1/);

  const unrecognized = parseVersionString("CockroachDB CCL v24.1.7");
  const probed2 = await resolveCapabilityStatus(unrecognized, "jsonb-functions", async () => {});
  assert.equal(probed2.status, "supported");
  assert.match(probed2.evidence, /unrecognized engine/);
});

test("I01: a probe rejected by the server is positive unsupported evidence", async () => {
  const nucleus = parseVersionString("PostgreSQL 16.0 (Nucleus 0.1.0)");
  const probed = await resolveCapabilityStatus(nucleus, "jsonb-functions", async () => {
    throw new ServerSqlError("nucleus: function to_jsonb does not exist", { sqlstate: "42883" });
  });
  assert.equal(probed.status, "unsupported");
  assert.match(probed.evidence, /42883/);
});

test("I01: transport errors during a probe propagate — they prove nothing", async () => {
  const nucleus = parseVersionString("PostgreSQL 16.0 (Nucleus 0.1.0)");
  await assert.rejects(
    () =>
      resolveCapabilityStatus(nucleus, "jsonb-functions", async () => {
        throw new ConnectionFailedError("postgres: connect ECONNREFUSED 127.0.0.1:1", { code: "ECONNREFUSED" });
      }),
    ConnectionFailedError,
  );
});

test("I01: unregistered capabilities are unknown on every engine — fail closed", async () => {
  for (const raw of ["PostgreSQL 17.11", "PostgreSQL 16.0 (Nucleus 0.2.1)", "CockroachDB CCL v24"] as const) {
    const ev = await resolveCapabilityStatus(parseVersionString(raw), "hypothetical-future-capability" as never);
    assert.equal(ev.status, "unknown", raw);
    assert.match(ev.evidence, /no resolution rule/);
  }
});

test("I01: unparseable PostgreSQL version is unknown even with a registered fact", async () => {
  const weird = { product: "postgres" as const, version: "", raw: "PostgreSQL (custom build, no number)" };
  const ev = await resolveCapabilityStatus(weird, "jsonb-functions");
  assert.equal(ev.status, "unknown");
});

// ---------------------------------------------------------------------------
// Gate: memoization, fail-closed assert, retry after transport failure
// ---------------------------------------------------------------------------

function gateDriver(versions: string[], failFirst = 0): { driver: Driver; stats: { versionQueries: number } } {
  const stats = { versionQueries: 0 };
  let failures = failFirst;
  const driver: Driver = {
    async query<T>(sqlText: string): Promise<T[]> {
      if (sqlText.startsWith("select version()")) {
        stats.versionQueries++;
        if (failures > 0) {
          failures--;
          throw new ConnectionFailedError("stub: connection down", { code: "ECONNREFUSED" });
        }
        return [{ version: versions[0] }] as T[];
      }
      return [] as T[];
    },
    async execute(): Promise<number> {
      return 0;
    },
    begin: () => Promise.reject(new Error("unused")),
    close: () => Promise.resolve(),
    lifecycle: makeLifecycle("borrowed", () => Promise.resolve()),
  };
  return { driver, stats };
}

test("I01: gate assert passes supported and fails closed on unknown/unsupported", async () => {
  const { driver } = gateDriver(["PostgreSQL 17.11"]);
  const gate = capabilityGate(driver);
  await gate.assert(["jsonb-functions"]); // 17.11 -> supported by fact
  await gate.assert([]); // no-op

  const { driver: nucleusDriver } = gateDriver(["PostgreSQL 16.0 (Nucleus 0.2.1 — The Definitive Database)"]);
  // The stub answers probe-shaped SQL with a server error -> unsupported.
  const throwing = { ...nucleusDriver, query: async <T>(sql: string): Promise<T[]> => {
    if (sql.startsWith("select to_jsonb")) {
      throw new ServerSqlError("stub: unsupported", { sqlstate: "42883" });
    }
    return nucleusDriver.query<T>(sql);
  } } as Driver;
  const unsupportedErr = await capabilityGate(throwing).assert(["jsonb-functions"]).then(
    () => null,
    (e: unknown) => e,
  ) as CapabilityRequirementError;
  assert.ok(unsupportedErr instanceof CapabilityRequirementError);
  assert.equal(unsupportedErr.results[0].status, "unsupported");

  // Unknown: an unregistered capability has no rule and no probe — even on a
  // known engine it resolves unknown, never all-enabled.
  const unregisteredDriver = { ...driver, query: async <T>(sql: string): Promise<T[]> => {
    if (sql.startsWith("select version()")) return [{ version: "PostgreSQL 17.11" }] as T[];
    return [] as T[];
  } } as Driver;
  const err = await capabilityGate(unregisteredDriver).assert(["future-capability" as never]).then(
    () => null,
    (e: unknown) => e,
  ) as CapabilityRequirementError;
  assert.ok(err instanceof CapabilityRequirementError);
  assert.equal(err.results.length, 1);
  assert.equal(err.results[0].status, "unknown");
  assert.match(err.message, /failing closed/);
  assert.match(err.message, /future-capability/);
});

test("I01: gate memoizes engine identity and capability status; transport failure retries", async () => {
  const { driver, stats } = gateDriver(["PostgreSQL 17.11"], 1);
  const gate = capabilityGate(driver);
  await assert.rejects(() => gate.engine(), ConnectionFailedError); // first attempt fails
  const identity = await gate.engine(); // retry succeeds
  assert.equal(identity.product, "postgres");
  await gate.engine();
  await gate.status("jsonb-functions");
  await gate.status("jsonb-functions");
  assert.equal(stats.versionQueries, 2, "one failed probe + one successful identity query, then memoized");
});

// ---------------------------------------------------------------------------
// Error taxonomy
// ---------------------------------------------------------------------------

test("I01: node transport errors classify as ConnectionFailedError with fields", () => {
  const raw = Object.assign(new Error("connect ECONNREFUSED 127.0.0.1:1"), {
    code: "ECONNREFUSED",
    errno: -61,
    syscall: "connect",
    address: "127.0.0.1",
    port: 1,
  });
  const err = classifyDriverError(raw, "pg");
  assert.ok(err instanceof ConnectionFailedError);
  assert.equal(err.code, "ECONNREFUSED");
  assert.equal(err.address, "127.0.0.1");
  assert.equal(err.port, 1);
  assert.equal(err.cause, raw);
  assert.equal(err.name, "ConnectionFailedError");
});

test("I01: postgres.js synthetic connection codes and pg pool-ended errors classify as connection failures", () => {
  const ended = Object.assign(new Error("write CONNECTION_ENDED 127.0.0.1:5432"), {
    code: "CONNECTION_ENDED",
    errno: "CONNECTION_ENDED",
    address: "127.0.0.1",
    port: 5432,
  });
  assert.ok(classifyDriverError(ended, "postgres") instanceof ConnectionFailedError);

  // Exact message thrown by pg 8.22.0's pg-pool 3.14.0 (index.js connect()).
  // Regression: an earlier regex expected "...after calling end on it" and
  // never matched, misclassifying query-after-close as a generic error.
  const poolEnded = new Error("Cannot use a pool after calling end on the pool");
  const classified = classifyDriverError(poolEnded, "pg");
  assert.ok(classified instanceof ConnectionFailedError, `pg pool-ended must be ConnectionFailedError, got ${classified.constructor.name}`);
  assert.equal((classified as ConnectionFailedError).cause, poolEnded);
});

test("I01: SQLSTATE errors classify as ServerSqlError with codes retained", () => {
  const unique = Object.assign(new Error("duplicate key value violates unique constraint"), {
    code: "23505",
    severity: "ERROR",
    detail: "Key (id)=(1) already exists.",
  });
  const err = classifyDriverError(unique, "pg");
  assert.ok(err instanceof ServerSqlError);
  assert.equal(err.sqlstate, "23505");
  assert.equal(err.severity, "ERROR");
  assert.equal(err.detail, "Key (id)=(1) already exists.");
  assert.equal((err.cause as { code?: string }).code, "23505", "original cause keeps its code");
  assert.equal(err.name, "ServerSqlError");

  // postgres.js generic() shape (code only, string errno)
  const canceled = Object.assign(new Error("57014: canceling statement due to user request"), { code: "57014" });
  const classified = classifyDriverError(canceled, "postgres");
  assert.ok(classified instanceof ServerSqlError);
  assert.equal(classified.sqlstate, "57014");
});

test("I01: unknown-shape errors stay generic, never mislabeled as connection or server errors", () => {
  const weird = new TypeError("unexpected driver internals");
  const err = classifyDriverError(weird, "pg");
  assert.ok(err instanceof NeutronSqlError);
  assert.ok(!(err instanceof ConnectionFailedError));
  assert.ok(!(err instanceof ServerSqlError));
  assert.equal(err.cause, weird);
});

test("I01: already-classified errors pass through unchanged (no double wrap)", () => {
  const first = classifyDriverError(Object.assign(new Error("x"), { code: "42P01" }), "pg");
  assert.equal(classifyDriverError(first, "pg"), first);
});

test("I01: getSqlState sees through wrappers, raw driver errors and cause chains", () => {
  const raw = Object.assign(new Error("x"), { code: "42P01" });
  assert.equal(getSqlState(raw), "42P01");
  const wrapped = classifyDriverError(raw, "pg");
  assert.equal(getSqlState(wrapped), "42P01");
  const nested = new NeutronSqlError("outer", { cause: wrapped });
  assert.equal(getSqlState(nested), "42P01");
  assert.equal(getSqlState(new Error("no code")), undefined);
  assert.equal(getSqlState(Object.assign(new Error("sys"), { code: "ECONNREFUSED", errno: -61 })), undefined);
});

test("I01: a genuinely absent module import is recognized as module-not-found", async () => {
  // Real dynamic import of a package that cannot exist in any environment —
  // Node rejects with code ERR_MODULE_NOT_FOUND. This is the live shape the
  // loader turns into MissingDriverError; connection failures never carry it.
  const specifier = "@neutron-build/i01-definitely-not-installed-probe";
  const err = await import(specifier).then(
    () => null,
    (e: unknown) => e,
  );
  assert.ok(err !== null, "importing a nonexistent package must reject");
  assert.ok(isModuleNotFoundError(err), `expected module-not-found recognition, got: ${String(err)}`);
  const classified = classifyDriverError(err, "postgres");
  assert.ok(!(classified instanceof ConnectionFailedError), "module-not-found must not look like a connection failure");
});

test("I01: guards classify by class, construction failures are connection-class", () => {
  assert.ok(isConnectionError(new ConnectionFailedError("x")));
  assert.ok(!isConnectionError(new ServerSqlError("x", { sqlstate: "23505" })));
  assert.ok(isMissingDriverError(new MissingDriverError("pg", "not installed")));
  const construction = connectionConstructionError("postgres", new TypeError("Invalid URL"));
  assert.ok(construction instanceof ConnectionFailedError);
  assert.ok(!isMissingDriverError(construction), "construction failure must not look like a missing driver");
});

// ---------------------------------------------------------------------------
// Adapter lifecycle
// ---------------------------------------------------------------------------

test("I01: owned lifecycle terminates exactly once; borrowed never terminates", async () => {
  let closes = 0;
  const owned = makeLifecycle("owned", async () => {
    closes++;
  });
  assert.equal(owned.ownership, "owned");
  assert.equal(owned.terminated, false);
  await Promise.all([owned.terminate(), owned.terminate()]); // concurrent
  await owned.terminate(); // sequential
  assert.equal(closes, 1, "exactly one close across concurrent and repeated terminate calls");
  assert.equal(owned.terminated, true);

  let borrowedCloses = 0;
  const borrowed = makeLifecycle("borrowed", async () => {
    borrowedCloses++;
  });
  await borrowed.terminate();
  await borrowed.terminate();
  assert.equal(borrowedCloses, 0, "borrowed terminate never closes the owner's resource");
  assert.equal(borrowed.terminated, false);
});

test("I01: wrapPgPool borrowed default never ends the injected pool; owned ends it once", async () => {
  let ends = 0;
  const pool = {
    query: async () => ({ rows: [], rowCount: 0 }),
    connect: async () => ({ query: async () => ({ rows: [], rowCount: 0 }), release: () => {} }),
    end: async () => {
      ends++;
    },
  };
  const borrowed = wrapPgPool(pool);
  assert.equal(borrowed.lifecycle.ownership, "borrowed");
  await borrowed.close();
  await borrowed.close();
  assert.equal(ends, 0, "borrowed close leaves the owner's pool functional");
  await pool.end(); // owner still can
  assert.equal(ends, 1);

  ends = 0;
  const owned = wrapPgPool({ ...pool, end: async () => { ends++; } }, { ownership: "owned" });
  await owned.close();
  await owned.close();
  assert.equal(ends, 1);
});

test("I01: wrapPostgresJs borrowed default never ends the injected client", async () => {
  let ends = 0;
  type Client = Parameters<typeof wrapPostgresJs>[0];
  // local lifecycle double (no database boundary involved): unsafe/reserve
  // return cancelable promises matching the postgres.js Query surface
  const fakeUnsafe = (): ReturnType<Client["unsafe"]> =>
    Object.assign(Promise.resolve(Object.assign([], { count: 0 })), { cancel: () => Promise.resolve() });
  const client: Client = {
    unsafe: fakeUnsafe,
    begin: async <T>(fn: (tx: Client) => Promise<T>): Promise<T> => fn(client),
    end: async () => {
      ends++;
    },
    reserve: async () => ({ unsafe: fakeUnsafe, release: () => {} }),
  };
  const borrowed = wrapPostgresJs(client);
  await borrowed.close();
  assert.equal(ends, 0);
  assert.equal(borrowed.query("select 1") instanceof Promise, true);
});

test("I01: adapter query errors are classified at the boundary", async () => {
  const pool = {
    query: async () => {
      throw Object.assign(new Error("duplicate key"), { code: "23505", severity: "ERROR" });
    },
    connect: async () => ({ query: async () => ({ rows: [], rowCount: 0 }), release: () => {} }),
    end: async () => {},
  };
  const driver = wrapPgPool(pool);
  await assert.rejects(() => driver.query("insert into t values (1)"), ServerSqlError);
});

// ---------------------------------------------------------------------------
// Fallback honesty (fail-after companion to the recorded fail-before)
// ---------------------------------------------------------------------------

test("I01: auto driver selection does not fall back on connection-class failures", async () => {
  // Before I01 this exact call silently returned a pg driver (postgres.js
  // constructor threw on the malformed URL; the catch-all treated it as
  // missing-driver). Recorded fail-before: evidence/I01/attempt-1.md.
  await assert.rejects(
    () => loadDriver("not a url at all", { driver: "auto" }),
    (err: unknown) => err instanceof ConnectionFailedError && !(err instanceof MissingDriverError),
  );
  await assert.rejects(() => loadDriver("not a url at all", { driver: "postgres" }), ConnectionFailedError);
});

// ---------------------------------------------------------------------------
// X02 (X01 review M1): vector probes assert VALUES, not acceptance.
// ---------------------------------------------------------------------------

const X02_VECTOR_CAPS = [
  "vector-type",
  "vector-operator-l2",
  "vector-operator-inner-product",
  "vector-operator-cosine",
  "vector-operator-l1",
] as const;

test("X02: every vector probe carries a value assertion with a 1/0 negative-control arm", async () => {
  // Structural guard: a regression to parse-only probes (`select
  // '[1]'::vector`) would certify engines that accept the syntax and compute
  // wrong values. Every probe must be a case expression that fires 22012
  // through `1/0` when the asserted literal is wrong.
  const expectedLiterals: Record<(typeof X02_VECTOR_CAPS)[number], RegExp> = {
    "vector-type": /::vector\)::text = '\[1\]' then 1/,
    "vector-operator-l2": /<-> '\[2\]'::vector\) = 1/,
    "vector-operator-inner-product": /<#> '\[2\]'::vector\) = -2/,
    "vector-operator-cosine": /<=> '\[2\]'::vector\) = 0/,
    "vector-operator-l1": /<\+> '\[2\]'::vector\) = 1/,
  };
  for (const cap of X02_VECTOR_CAPS) {
    const seen: string[] = [];
    await resolveCapabilityStatus(
      parseVersionString("PostgreSQL 16.0 (Nucleus 1.0.2 — The Definitive Database)"),
      cap,
      async (sql) => {
        seen.push(sql);
      },
    );
    assert.equal(seen.length, 1, `${cap} must run exactly one probe`);
    const probeSql = seen[0];
    assert.match(probeSql, /case when .+ then 1 else 1\/0 end/, `${cap} probe must be value-asserting`);
    assert.match(probeSql, expectedLiterals[cap], `${cap} probe must assert the pgvector-verified literal`);
  }
});

test("X02: a wrong-value vector engine resolves unsupported through the negative control", async () => {
  // A server that ACCEPTS the vector syntax but computes wrong values: the
  // case arm's condition is false, the engine evaluates 1/0 and answers
  // 22012 — the probe converts that into positive unsupported evidence (the
  // same catch that exposed Nucleus's fake FTS semantics).
  const fakeVectorEngine = async (sql: string): Promise<void> => {
    if (/else 1\/0/.test(sql)) {
      throw new ServerSqlError("nucleus: division by zero", { sqlstate: "22012" });
    }
  };
  for (const cap of X02_VECTOR_CAPS) {
    const verdict = await resolveCapabilityStatus(
      parseVersionString("PostgreSQL 16.0 (Nucleus 1.0.2 — The Definitive Database)"),
      cap,
      fakeVectorEngine,
    );
    assert.equal(verdict.status, "unsupported", cap);
    assert.match(verdict.evidence, /22012/);
  }
});

test("X02: a correct-value vector engine resolves supported", async () => {
  // The pgvector-faithful engine executes the case expression without error
  // (literals verified against pgvector 0.8.6 / PostgreSQL 17).
  for (const cap of X02_VECTOR_CAPS) {
    const verdict = await resolveCapabilityStatus(
      parseVersionString("PostgreSQL 16.0 (Nucleus 1.0.2 — The Definitive Database)"),
      cap,
      async () => {},
    );
    assert.equal(verdict.status, "supported", cap);
  }
});
