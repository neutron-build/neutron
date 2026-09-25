// ---------------------------------------------------------------------------
// @neutron-build/sql/columnar — optional columnar storage inspection (X03)
// ---------------------------------------------------------------------------
// Import as `@neutron-build/sql/columnar`. The SQL-only root never loads
// this file; importing it is the explicit opt-in.
//
// READ-ONLY inspection of what columnar storage the connected engine
// actually offers — never a claim of columnar execution. PostgreSQL core
// stores every table HEAP-only through the `heap` table access method
// (PG 12 table access methods); a columnar AM exists only when an
// extension provides one (e.g. citus columnar). The inspection reads
// pg_am and reports the truth: which table access methods exist, whether
// any is columnar-shaped, and what that means here (nothing is integrated
// or proven — creating columnar tables through this ORM is NOT offered).
//
// On engines whose catalogs cannot answer (Nucleus: the X00 capability
// report records catalog introspection 12/14 unsupported), the status is
// `unknown` with the server's own reason — never a silent "absent", and
// never an invented capability.

import type { Driver } from "./drivers.js";

export interface ColumnarAccessMethod {
  readonly name: string;
  /** True when the method's name is columnar-shaped ("columnar"). The
   *  determination is by catalog identity only — actual storage semantics
   *  of third-party access methods are the extension's business. */
  readonly columnar: boolean;
}

export interface ColumnarStorageReport {
  /** "heap-only" — core PostgreSQL: no columnar access method exists.
   *  "columnar-available" — an access method named "columnar" exists (an
   *  extension provided it; using it is out of scope here).
   *  "other" — some non-heap table AM exists, none columnar-named.
   *  "unknown" — the catalog read failed; the reason carries the server's
   *  own error. Unknown never becomes a silent absence. */
  readonly status: "heap-only" | "columnar-available" | "other" | "unknown";
  readonly accessMethods: readonly ColumnarAccessMethod[];
  readonly detail: string;
  /** Always false: this module inspects only. No columnar table creation,
   *  conversion or execution claims are made anywhere in this ORM. */
  readonly integrated: false;
}

/** Inspect the connected engine's table access methods and report honestly
 *  whether columnar storage exists. Read-only (one pg_am query). */
export async function inspectColumnarStorage(driver: Driver): Promise<ColumnarStorageReport> {
  let rows: Array<Record<string, unknown>>;
  try {
    rows = await driver.query<{ amname: string }>("select amname from pg_am where amtype = 't'");
  } catch (err) {
    return {
      status: "unknown",
      accessMethods: [],
      detail: `pg_am read failed (${err instanceof Error ? err.message : String(err)}) — columnar storage cannot be determined, never assumed present or absent`,
      integrated: false,
    };
  }
  const methods: ColumnarAccessMethod[] = rows
    .map((r) => String(r.amname))
    .filter((n) => n !== "")
    .map((name) => ({ name, columnar: name === "columnar" }));
  const columnar = methods.filter((m) => m.columnar);
  const nonHeap = methods.filter((m) => m.name !== "heap");
  if (columnar.length > 0) {
    return {
      status: "columnar-available",
      accessMethods: methods,
      detail: `a "columnar" table access method exists (provided by an extension) — this ORM integrates no columnar storage: inspection only, no creation/conversion/execution claims`,
      integrated: false,
    };
  }
  if (nonHeap.length > 0) {
    return {
      status: "other",
      accessMethods: methods,
      detail: `non-heap table access methods exist (${nonHeap.map((m) => m.name).join(", ")}) but none is columnar — this ORM integrates none of them`,
      integrated: false,
    };
  }
  return {
    status: "heap-only",
    accessMethods: methods,
    detail: "PostgreSQL core stores tables heap-only (table access method: heap) — no columnar storage exists on this engine; a columnar AM would require an extension (not integrated, not proven here)",
    integrated: false,
  };
}
