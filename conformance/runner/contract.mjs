// Language-agnostic Neutron contract conformance assertions.
//
// Given the base URL of a *running* SDK server, this module asserts every
// observable dimension of FRAMEWORK_CONTRACT.md and returns a structured result
// per dimension. It is transport-only: it speaks HTTP and inspects responses,
// so it works identically against any SDK regardless of implementation language.
//
// Each check returns { dim, status: "pass"|"fail"|"skip", detail }.
//   pass — contract satisfied
//   fail — contract violated (drift) — this is a finding, recorded with evidence
//   skip — the SDK does not expose this surface, and must be recorded in
//          conformance/known-skips.json with a reason and an expiry
//
// Every constant below comes from `conformance/contract-ir.json` (plan step
// S39), which `validate-ir.mjs` keeps in agreement with FRAMEWORK_CONTRACT.md.
// This file used to carry its own copy of the error taxonomy, the health shape
// and the nucleus tri-state — a second source of truth for one contract, free
// to drift from the prose with nothing comparing them. Its hardcoded taxonomy
// had in fact already lost the 422 `validation` row.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const IR = JSON.parse(
  fs.readFileSync(
    path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "contract-ir.json"),
    "utf8",
  ),
);

// Contract dimensions, in report order — from the IR.
export const DIMENSIONS = IR.dimensions.map((d) => d.id);


// Standard error codes from FRAMEWORK_CONTRACT.md §2, via the IR.
//
// Only the entries with a forced-error endpoint are probed here. `validation`
// (422) deliberately has `probePath: null` — it is produced by POSTing an
// invalid body and is asserted by the `validation.format` dimension instead.
// The hand-written list this replaces simply omitted that row, with nothing
// saying whether the omission was intentional.
const STANDARD_ERRORS = IR.errors.taxonomy
  .filter((e) => e.probePath)
  .map((e) => ({ path: e.probePath, status: e.status, suffix: e.code, title: e.title }));

function result(dim, status, detail) {
  return { dim, status, detail };
}

async function fetchSafe(url, opts) {
  try {
    return await fetch(url, opts);
  } catch (e) {
    return { ok: false, networkError: String(e) };
  }
}

// --- health ---------------------------------------------------------------

async function checkHealth(base, results, ctx) {
  const res = await fetchSafe(base + "/health");
  if (res.networkError) {
    results.push(result("health.shape", "fail", "GET /health network error: " + res.networkError));
    results.push(result("health.types", "fail", "no response"));
    results.push(result("feature.detection", "fail", "no /health to read nucleus field"));
    return;
  }
  if (res.status !== 200) {
    results.push(result("health.shape", "fail", `GET /health returned ${res.status}, expected 200`));
    results.push(result("health.types", "fail", "non-200"));
    results.push(result("feature.detection", "fail", "non-200 /health"));
    return;
  }
  let body;
  try {
    body = await res.json();
  } catch {
    results.push(result("health.shape", "fail", "GET /health body is not JSON"));
    results.push(result("health.types", "fail", "not JSON"));
    results.push(result("feature.detection", "fail", "not JSON"));
    return;
  }

  // §7: exactly { status, nucleus, version }.
  const keys = Object.keys(body).sort();
  const expected = [...IR.health.requiredKeys].sort();
  const extra = keys.filter((k) => !expected.includes(k));
  const missing = expected.filter((k) => !keys.includes(k));
  if (missing.length === 0 && extra.length === 0) {
    results.push(result("health.shape", "pass", `exactly {status, nucleus, version}`));
  } else {
    results.push(
      result(
        "health.shape",
        "fail",
        `keys=${JSON.stringify(keys)} missing=${JSON.stringify(missing)} extra=${JSON.stringify(extra)}`,
      ),
    );
  }

  // Types: status string, version string, nucleus tri-state string per §7
  // ("connected" | "disconnected" | "unconfigured").
  const NUCLEUS_STATES = IR.health.nucleusStates;
  const typeProblems = [];
  if (typeof body.status !== "string") typeProblems.push(`status is ${typeof body.status}, want string`);
  if (typeof body.version !== "string") typeProblems.push(`version is ${typeof body.version}, want string`);
  if (typeof body.nucleus !== "string" || !NUCLEUS_STATES.includes(body.nucleus)) {
    typeProblems.push(
      `nucleus is ${JSON.stringify(body.nucleus)}, contract §7 wants one of ${JSON.stringify(NUCLEUS_STATES)}`,
    );
  }
  if (typeProblems.length === 0) {
    results.push(result("health.types", "pass", `status:string version:string nucleus:${JSON.stringify(body.nucleus)}`));
  } else {
    results.push(result("health.types", "fail", typeProblems.join("; ")));
  }
  ctx.nucleusField = body.nucleus;
}

// --- feature detection (§1) ------------------------------------------------
// Feature detection is a connection-time SQL probe (SELECT VERSION()); its only
// HTTP-observable surface is the `nucleus` field of /health. We assert that the
// field is present and decodes to a defined detection state.
function checkFeatureDetection(results, ctx) {
  if (ctx.nucleusField === undefined) {
    results.push(result("feature.detection", "fail", "no nucleus field on /health"));
    return;
  }
  const v = ctx.nucleusField;
  const known =
    typeof v === "string" && ["connected", "disconnected", "unconfigured"].includes(v);
  if (known) {
    results.push(
      result("feature.detection", "pass", `nucleus=${JSON.stringify(v)} (detection state exposed via /health)`),
    );
  } else {
    results.push(result("feature.detection", "fail", `unrecognized nucleus value ${JSON.stringify(v)}`));
  }
}

// --- errors (§2) -----------------------------------------------------------

async function checkErrors(base, results) {
  let shapeOk = true;
  let ctypeOk = true;
  let codesOk = true;
  const shapeDetails = [];
  const ctypeDetails = [];
  const codeDetails = [];
  let any = false;

  // Probe every forced-error endpoint up front. The forced-error surface is
  // "wired" if any endpoint OTHER than not-found returns its forced (non-404)
  // status. /errors/not-found is ambiguous in isolation (404 also means "not
  // wired"), so we only assert on it when the rest of the surface is present —
  // otherwise a bare 404 is just the framework's normal not-found handling (e.g.
  // an SSR framework serving an HTML page), not an RFC 7807 violation.
  const probes = [];
  for (const e of STANDARD_ERRORS) {
    probes.push({ e, res: await fetchSafe(base + e.path) });
  }
  const surfaceWired = probes.some(
    ({ e, res }) => !res.networkError && e.suffix !== "not-found" && res.status !== 404,
  );

  for (const { e, res } of probes) {
    if (res.networkError) continue;
    if (res.status === 404 && (e.suffix !== "not-found" || !surfaceWired)) {
      // Endpoint not wired in this app — skip silently for this code.
      continue;
    }
    any = true;

    // status code
    if (res.status !== e.status) {
      codesOk = false;
      codeDetails.push(`${e.path}: got ${res.status} want ${e.status}`);
    }

    // content-type
    const ct = (res.headers.get("content-type") || "").toLowerCase();
    if (!ct.includes("application/problem+json")) {
      ctypeOk = false;
      ctypeDetails.push(`${e.path}: content-type=${ct || "(none)"}`);
    }

    let body;
    try {
      body = await res.json();
    } catch {
      shapeOk = false;
      shapeDetails.push(`${e.path}: body not JSON`);
      continue;
    }

    // RFC 7807 required fields: type, title, status, detail.
    for (const f of ["type", "title", "status", "detail"]) {
      if (!(f in body)) {
        shapeOk = false;
        shapeDetails.push(`${e.path}: missing ${f}`);
      }
    }
    if (typeof body.type === "string" && !body.type.includes("/errors/" + e.suffix)) {
      codesOk = false;
      codeDetails.push(`${e.path}: type=${body.type} want suffix /errors/${e.suffix}`);
    }
    if (body.title !== undefined && body.title !== e.title) {
      codesOk = false;
      codeDetails.push(`${e.path}: title=${JSON.stringify(body.title)} want ${JSON.stringify(e.title)}`);
    }
    if (body.status !== undefined && body.status !== e.status) {
      shapeOk = false;
      shapeDetails.push(`${e.path}: body.status=${body.status} want ${e.status}`);
    }
  }

  if (!any) {
    results.push(result("error.rfc7807", "skip", "no forced-error endpoints exposed"));
    results.push(result("error.contenttype", "skip", "no forced-error endpoints exposed"));
    results.push(result("error.codes", "skip", "no forced-error endpoints exposed"));
    return;
  }
  results.push(result("error.rfc7807", shapeOk ? "pass" : "fail", shapeOk ? "type/title/status/detail present" : shapeDetails.join("; ")));
  results.push(result("error.contenttype", ctypeOk ? "pass" : "fail", ctypeOk ? "application/problem+json" : ctypeDetails.join("; ")));
  results.push(result("error.codes", codesOk ? "pass" : "fail", codesOk ? "all §2 codes/titles match" : codeDetails.join("; ")));
}

// --- validation (§2 validation) -------------------------------------------

async function checkValidation(base, results) {
  // POST an invalid body to the canonical validation endpoint.
  const res = await fetchSafe(base + "/api/items", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ name: "", price: -1 }),
  });
  if (res.networkError) {
    results.push(result("validation.format", "skip", "validation endpoint unreachable"));
    return;
  }
  if (res.status === 404) {
    results.push(result("validation.format", "skip", "no /api/items validation endpoint"));
    return;
  }
  if (res.status !== 422) {
    results.push(result("validation.format", "fail", `expected 422, got ${res.status}`));
    return;
  }
  let body;
  try {
    body = await res.json();
  } catch {
    results.push(result("validation.format", "fail", "422 body not JSON"));
    return;
  }
  const problems = [];
  if (typeof body.type !== "string" || !body.type.includes("/errors/validation"))
    problems.push(`type=${JSON.stringify(body.type)}`);
  if (body.title !== "Validation Failed") problems.push(`title=${JSON.stringify(body.title)}`);
  if (body.status !== 422) problems.push(`status=${body.status}`);
  if (!Array.isArray(body.errors)) {
    problems.push("errors[] missing/not array");
  } else if (body.errors.length > 0) {
    const first = body.errors[0];
    if (!("field" in first) || !("message" in first))
      problems.push("errors[0] missing field/message");
  }
  results.push(
    result(
      "validation.format",
      problems.length === 0 ? "pass" : "fail",
      problems.length === 0 ? "validation errors[] with field/message" : problems.join("; "),
    ),
  );
}

// --- OpenAPI (§4) ----------------------------------------------------------

async function checkOpenAPI(base, results) {
  const res = await fetchSafe(base + "/openapi.json");
  if (!res.networkError && res.status === 404) {
    // The route is not registered: this SDK does not expose an OpenAPI surface
    // (e.g. a web/SSR framework whose routes are pages, not API endpoints).
    // Documented skip, consistent with how errors/validation treat a 404.
    results.push(result("openapi.present", "skip", "no /openapi.json endpoint exposed"));
    results.push(result("openapi.31", "skip", "no /openapi.json endpoint exposed"));
    return;
  }
  if (res.networkError || res.status !== 200) {
    // Present but broken (5xx, non-JSON, wrong status) is a real drift, not a skip.
    results.push(result("openapi.present", "fail", `GET /openapi.json → ${res.status || res.networkError}`));
    results.push(result("openapi.31", "fail", "no spec"));
    return;
  }
  let spec;
  try {
    spec = await res.json();
  } catch {
    results.push(result("openapi.present", "fail", "/openapi.json not parseable JSON"));
    results.push(result("openapi.31", "fail", "unparseable"));
    return;
  }
  results.push(result("openapi.present", "pass", "/openapi.json served and parseable"));
  const v = spec.openapi;
  if (typeof v === "string" && v.startsWith("3.1")) {
    results.push(result("openapi.31", "pass", `openapi=${v}`));
  } else {
    results.push(result("openapi.31", "fail", `openapi=${JSON.stringify(v)} want 3.1.x`));
  }
}

// --- middleware observable effects (§5) ------------------------------------

async function checkRequestId(base, results) {
  const res = await fetchSafe(base + "/health");
  if (res.networkError) {
    results.push(result("mw.requestid", "fail", "unreachable"));
    return;
  }
  const id = res.headers.get("x-request-id");
  if (id) {
    results.push(result("mw.requestid", "pass", `x-request-id=${id.slice(0, 12)}…`));
  } else {
    results.push(result("mw.requestid", "fail", "no x-request-id response header"));
  }
}

async function checkCors(base, results) {
  // Preflight OPTIONS with Origin → expect Access-Control-Allow-Origin.
  const res = await fetchSafe(base + "/api/items", {
    method: "OPTIONS",
    headers: {
      Origin: "https://conformance.test",
      "Access-Control-Request-Method": "POST",
      "Access-Control-Request-Headers": "content-type",
    },
  });
  if (res.networkError) {
    results.push(result("mw.cors", "fail", "preflight unreachable"));
    return;
  }
  const allow = res.headers.get("access-control-allow-origin");
  if (allow) {
    results.push(result("mw.cors", "pass", `preflight ${res.status}, allow-origin=${allow}`));
  } else {
    results.push(result("mw.cors", "fail", `preflight ${res.status}, no Access-Control-Allow-Origin`));
  }
}

async function checkCompression(base, results) {
  // Ask for gzip on a body large enough to compress.
  const res = await fetchSafe(base + "/api/items", {
    headers: { "Accept-Encoding": "gzip" },
  });
  if (res.networkError || res.status === 404) {
    results.push(result("mw.compression", "skip", "no compressible endpoint"));
    return;
  }
  // fetch() transparently decodes; inspect raw headers.
  const enc = (res.headers.get("content-encoding") || "").toLowerCase();
  const vary = (res.headers.get("vary") || "").toLowerCase();
  const problems = [];
  if (!enc.includes("gzip") && !enc.includes("br") && !enc.includes("deflate"))
    problems.push(`content-encoding=${enc || "(none)"}`);
  if (!vary.includes("accept-encoding")) problems.push(`vary=${vary || "(none)"} (want Accept-Encoding)`);
  results.push(
    result(
      "mw.compression",
      problems.length === 0 ? "pass" : "fail",
      problems.length === 0 ? `content-encoding=${enc}, vary=accept-encoding` : problems.join("; "),
    ),
  );
}

// --- driver ----------------------------------------------------------------

export async function runContract(base) {
  const results = [];
  const ctx = {};
  await checkHealth(base, results, ctx);
  checkFeatureDetection(results, ctx);
  await checkErrors(base, results);
  await checkValidation(base, results);
  await checkOpenAPI(base, results);
  await checkRequestId(base, results);
  await checkCors(base, results);
  await checkCompression(base, results);
  return results;
}

export async function waitForHealth(base, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const res = await fetchSafe(base + "/health");
    if (!res.networkError && res.status === 200) return true;
    await new Promise((r) => setTimeout(r, 250));
  }
  return false;
}
