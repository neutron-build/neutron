// TypeScript reference consumer for the Neutron schema contract v2.
//
// Recomputes, independently of the Go implementation (cli/internal/db/
// schema_v2.go), the canonical bytes and SHA-256 hashes recorded by the Go
// side in golden/manifest.json, re-validates every invalid fixture for the
// same rejection reason, and runs its own hash-sensitivity and
// formatting-absorption checks. Run from anywhere:
//
//   node --experimental-strip-types contracts/data/consumer.ts
//
// Exit code 0 means full agreement with the recorded golden values.

import { createHash } from "node:crypto";
import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

const goldenDir = path.join(path.dirname(fileURLToPath(import.meta.url)), "golden");

class ContractError extends Error {
  code: string;
  at: string;
  constructor(code: string, at: string, detail: string) {
    super(`[${code}] ${at}: ${detail}`);
    this.code = code;
    this.at = at;
  }
}

const fail = (code: string, at: string, detail: string): never => {
  throw new ContractError(code, at, detail);
};

// ---------------------------------------------------------------------------
// Shared vocabulary (kept in lockstep with schema-v2.json and schema_v2.go)
// ---------------------------------------------------------------------------

const ROOT_FIELDS = ["version", "dialect", "capabilities", "schemas", "tables", "enums", "views", "opaque"] as const;
const CAPABILITIES = new Set(["pgvector", "nucleus"]);
const TYPE_CODECS: Record<string, string> = {
  bool: "boolean", int2: "number", int4: "number", int8: "bigint",
  float4: "number", float8: "number", numeric: "decimal-string",
  text: "string", varchar: "string",
  timestamp: "timestamp-string", timestamptz: "timestamptz-string", date: "date-string",
  bytea: "binary", uuid: "uuid", json: "json", jsonb: "json",
  vector: "vector", enum: "enum",
};
const TYPE_PARAMS: Record<string, Set<string>> = {
  varchar: new Set(["length"]),
  numeric: new Set(["precision", "scale"]),
  timestamp: new Set(["precision"]),
  timestamptz: new Set(["precision"]),
  vector: new Set(["dimensions"]),
};
const REFERENTIAL_ACTIONS = new Set(["cascade", "restrict", "no action", "set null", "set default"]);
const FK_MATCHES = new Set(["simple", "full", "partial"]);
const INDEX_METHODS = new Set(["btree", "hash", "gin", "gist", "spgist", "brin"]);
// Default-operator-class facts for the index methods and column types in
// the vocabulary: exactly these combinations apply on PostgreSQL without
// naming an operator class (built-in pg_opclass defaults; arrays uniformly
// via array_ops). Everything else is refused by the server with SQLSTATE
// 42704, so the consumer rejects it — the contract has no operator-class
// slot. Kept in lockstep with schema_v2.go and export.ts.
const INDEX_METHOD_SCALARS: Record<string, ReadonlySet<string>> = {
  btree: new Set(["text", "varchar", "bool", "int2", "int4", "int8", "float4", "float8", "numeric", "timestamp", "timestamptz", "date", "uuid", "bytea", "enum", "jsonb"]),
  hash: new Set(["text", "varchar", "bool", "int2", "int4", "int8", "float4", "float8", "numeric", "timestamp", "timestamptz", "date", "uuid", "bytea", "enum", "jsonb"]),
  gin: new Set(["jsonb"]),
  gist: new Set(),
  spgist: new Set(["text", "varchar"]),
  brin: new Set(["text", "varchar", "int2", "int4", "int8", "float4", "float8", "numeric", "timestamp", "timestamptz", "date", "uuid", "bytea"]),
};
const INDEX_METHOD_ARRAYS = new Set(["btree", "hash", "gin"]);
const TYPE_ALIASES: Record<string, string> = {
  boolean: "bool", int: "int4", integer: "int4", smallint: "int2",
  bigint: "int8", real: "float4", decimal: "numeric",
};
const literalCast = (sql: string): { elem: string; depth: number } | null => {
  const i = sql.indexOf("::");
  if (i < 0) return null;
  let cast = sql.slice(i + 2);
  let depth = 0;
  while (cast.endsWith("[]")) {
    depth++;
    cast = cast.slice(0, -2);
  }
  const dot = cast.lastIndexOf(".");
  if (dot >= 0) cast = cast.slice(dot + 1);
  return { elem: cast, depth };
};
const normalizeTypeName = (name: string): string => TYPE_ALIASES[name] ?? name;
const OPAQUE_KINDS = new Set(["extension-table", "extension-object", "unsupported-table", "unsupported-object"]);
const INTEGER_TYPES = new Set(["int2", "int4", "int8"]);
const SAFE_INTEGER_LIMIT = 2 ** 53 - 1;

const LITERAL_PATTERN = new RegExp(
  "^('([^']|'')*'(::[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?(\\[\\])*)?|-?(\\d+(\\.\\d*)?|\\.\\d+)([eE][+-]?\\d+)?|true|false|null)$"
);

type Json = null | boolean | number | string | Json[] | { [k: string]: Json };
type Obj = { [k: string]: Json };

// ---------------------------------------------------------------------------
// Raw-input hygiene
// ---------------------------------------------------------------------------

// Mirrors Go's encoding/json nesting limit (max depth 10000): deeper input
// is rejected invalid-json deterministically in both languages, independent
// of the environment's recursion headroom.
const MAX_NESTING_DEPTH = 10000;

const checkDepth = (v: Json): void => {
  const stack: { v: Json; d: number }[] = [{ v, d: 0 }];
  while (stack.length > 0) {
    const { v: cur, d } = stack.pop()!;
    if (d > MAX_NESTING_DEPTH) {
      fail("invalid-json", "$", "nesting depth exceeds 10000");
    }
    if (Array.isArray(cur)) cur.forEach((item) => stack.push({ v: item, d: d + 1 }));
    else if (cur !== null && typeof cur === "object") {
      for (const k of Object.keys(cur)) stack.push({ v: (cur as Obj)[k] as Json, d: d + 1 });
    }
  }
};

const scanStrings = (v: Json, at: string): void => {
  if (typeof v === "string") {
    if (v.includes("\uFFFD")) fail("invalid-value", at, "string contains U+FFFD");
    for (const ch of v) {
      const cp = ch.codePointAt(0)!;
      if (cp >= 0xd800 && cp <= 0xdfff) {
        fail("invalid-value", at, "string contains an unpaired surrogate");
      }
    }
    return;
  }
  if (Array.isArray(v)) v.forEach((item, i) => scanStrings(item, `${at}[${i}]`));
  else if (v !== null && typeof v === "object") {
    for (const k of Object.keys(v).sort()) scanStrings(v[k] as Json, `${at}.${k}`);
  }
};

// ---------------------------------------------------------------------------
// Validation (mirrors v2ValidateDocument)
// ---------------------------------------------------------------------------

const isObj = (v: Json, at: string): Obj => {
  if (v === null || typeof v !== "object" || Array.isArray(v)) {
    fail("invalid-value", at, "expected an object");
  }
  return v as Obj;
};
const isArr = (v: Json, at: string): Json[] => {
  if (!Array.isArray(v)) fail("invalid-value", at, "expected an array");
  return v as Json[];
};
const isStr = (v: Json, at: string): string => {
  if (typeof v !== "string") fail("invalid-value", at, "expected a string");
  return v as string;
};
const isBool = (v: Json, at: string): boolean => {
  if (typeof v !== "boolean") fail("invalid-value", at, "expected a boolean");
  return v as boolean;
};
const isInt = (v: Json, at: string): number => {
  if (typeof v !== "number" || !Number.isInteger(v) || Math.abs(v) > SAFE_INTEGER_LIMIT) {
    fail("invalid-number", at, `value ${String(v)} is not a safe integer`);
  }
  return v as number;
};
const checkName = (s: string, at: string): void => {
  if (s === "") fail("invalid-identity", at, "name must not be empty");
  for (const ch of s) {
    const cp = ch.codePointAt(0)!;
    if (cp < 0x20 || cp === 0x7f) fail("invalid-identity", at, "name must not contain control characters");
  }
};
const checkSQLText = (s: string, at: string, expression: boolean): void => {
  if (s === "") fail("invalid-value", at, "SQL text must not be empty");
  if (s.includes("\u0000")) fail("invalid-value", at, "SQL text must not contain NUL");
  if (expression && s.includes(";")) {
    fail("invalid-value", at, "expression must not contain a statement separator");
  }
};
const checkIdentity = (v: Json, at: string): { schema: string; name: string } => {
  const m = isObj(v, at);
  for (const k of Object.keys(m)) {
    if (k !== "schema" && k !== "name") fail("unknown-field", `${at}.${k}`, `unknown identity field ${JSON.stringify(k)}`);
  }
  for (const k of ["schema", "name"]) {
    if (!(k in m)) fail("missing-field", `${at}.${k}`, `identity requires ${JSON.stringify(k)}`);
  }
  const schema = isStr(m.schema, `${at}.schema`);
  const name = isStr(m.name, `${at}.name`);
  checkName(schema, `${at}.schema`);
  checkName(name, `${at}.name`);
  return { schema, name };
};
const onlyFields = (m: Obj, at: string, allowed: string[], required: string[]): void => {
  for (const k of Object.keys(m).sort()) {
    if (!allowed.includes(k)) fail("unknown-field", `${at}.${k}`, `unknown field ${JSON.stringify(k)}`);
  }
  for (const k of required) {
    if (!(k in m)) fail("missing-field", `${at}.${k}`, `required field ${JSON.stringify(k)} is absent`);
  }
};

interface State {
  capabilities: Set<string>;
  schemas: Set<string>;
  tables: Map<string, Obj>;
  tableColumns: Map<string, Set<string>>;
  tableKeyTuples: Map<string, string[][]>;
  enums: Set<string>;
  views: Set<string>;
  indexIdents: Map<string, string>;
  opaqueIdents: Set<string>;
  sequenceSchemas: string[];
  enumRefs: { at: string; key: string }[];
}

const tableKey = (schema: string, name: string): string => `${schema}.${name}`;

const validateDocument = (root: Obj): void => {
  onlyFields(root, "$", [...ROOT_FIELDS], [...ROOT_FIELDS]);
  if (isInt(root.version, "$.version") !== 2) {
    fail("unknown-version", "$.version", "schema document must declare version 2");
  }
  if (isStr(root.dialect, "$.dialect") !== "postgresql") {
    fail("invalid-value", "$.dialect", 'dialect must be "postgresql"');
  }

  const st: State = {
    capabilities: new Set(),
    schemas: new Set(),
    tables: new Map(),
    tableColumns: new Map(),
    tableKeyTuples: new Map(),
    enums: new Set(),
    views: new Set(),
    indexIdents: new Map(),
    opaqueIdents: new Set(),
    sequenceSchemas: [],
    enumRefs: [],
  };

  isArr(root.capabilities, "$.capabilities").forEach((c, i) => {
    const at = `$.capabilities[${i}]`;
    const s = isStr(c, at);
    if (!CAPABILITIES.has(s)) fail("invalid-value", at, `unknown capability ${JSON.stringify(s)}`);
    if (st.capabilities.has(s)) fail("invalid-value", at, `duplicate capability ${JSON.stringify(s)}`);
    st.capabilities.add(s);
  });

  isArr(root.schemas, "$.schemas").forEach((item, i) => {
    const at = `$.schemas[${i}]`;
    const m = isObj(item, at);
    onlyFields(m, at, ["name"], ["name"]);
    const name = isStr(m.name, `${at}.name`);
    checkName(name, `${at}.name`);
    if (st.schemas.has(name)) fail("duplicate-schema", at, `schema ${JSON.stringify(name)} is declared twice`);
    st.schemas.add(name);
  });

  isArr(root.enums, "$.enums").forEach((item, i) => {
    const at = `$.enums[${i}]`;
    const m = isObj(item, at);
    onlyFields(m, at, ["identity", "managed", "values"], ["identity", "managed", "values"]);
    const id = checkIdentity(m.identity, `${at}.identity`);
    isBool(m.managed, `${at}.managed`);
    const values = isArr(m.values, `${at}.values`);
    if (values.length === 0) fail("invalid-value", `${at}.values`, "enum must declare at least one value");
    const seen = new Set<string>();
    values.forEach((v, j) => {
      const vat = `${at}.values[${j}]`;
      const s = isStr(v, vat);
      checkName(s, vat);
      if (seen.has(s)) fail("invalid-value", vat, `duplicate enum value ${JSON.stringify(s)}`);
      seen.add(s);
    });
    const key = tableKey(id.schema, id.name);
    if (st.enums.has(key)) fail("duplicate-enum", `${at}.identity`, `enum ${JSON.stringify(key)} is declared twice`);
    st.enums.add(key);
  });

  isArr(root.tables, "$.tables").forEach((item, i) => {
    validateTable(item, `$.tables[${i}]`, st);
  });

  isArr(root.views, "$.views").forEach((item, i) => {
    const at = `$.views[${i}]`;
    const m = isObj(item, at);
    onlyFields(m, at, ["identity", "managed", "definition", "checkOption", "securityInvoker"], ["identity", "managed", "definition"]);
    const id = checkIdentity(m.identity, `${at}.identity`);
    isBool(m.managed, `${at}.managed`);
    checkSQLText(isStr(m.definition, `${at}.definition`), `${at}.definition`, false);
    if ("checkOption" in m) {
      const co = isStr(m.checkOption, `${at}.checkOption`);
      if (co !== "local" && co !== "cascaded") fail("invalid-value", `${at}.checkOption`, 'checkOption must be "local" or "cascaded"');
    }
    if ("securityInvoker" in m) isBool(m.securityInvoker, `${at}.securityInvoker`);
    const key = tableKey(id.schema, id.name);
    if (st.views.has(key)) fail("duplicate-view", `${at}.identity`, `view ${JSON.stringify(key)} is declared twice`);
    st.views.add(key);
  });

  isArr(root.opaque, "$.opaque").forEach((item, i) => {
    const at = `$.opaque[${i}]`;
    const m = isObj(item, at);
    onlyFields(m, at, ["kind", "identity", "owner", "reason"], ["kind", "identity", "reason"]);
    const kind = isStr(m.kind, `${at}.kind`);
    if (!OPAQUE_KINDS.has(kind)) fail("invalid-value", `${at}.kind`, `unknown opaque kind ${JSON.stringify(kind)}`);
    const id = checkIdentity(m.identity, `${at}.identity`);
    if ("owner" in m) checkName(isStr(m.owner, `${at}.owner`), `${at}.owner`);
    if (isStr(m.reason, `${at}.reason`) === "") fail("invalid-value", `${at}.reason`, "opaque entries must state a reason");
    const triple = `${kind}|${tableKey(id.schema, id.name)}`;
    if (st.opaqueIdents.has(triple)) fail("invalid-value", at, "opaque entry is declared twice");
    st.opaqueIdents.add(triple);
  });
  // opaque entry schemas
  isArr(root.opaque, "$.opaque").forEach((item) => {
    const m = item as Obj;
    const id = m.identity as Obj;
    const schema = isStr(id.schema, "$.opaque");
    if (!st.schemas.has(schema)) fail("undeclared-schema", "$.opaque", `opaque entry lives in undeclared schema ${JSON.stringify(schema)}`);
  });

  crossReferences(st);
};

const validateTable = (item: Json, at: string, st: State): void => {
  const m = isObj(item, at);
  onlyFields(m, at, ["identity", "managed", "columns", "constraints", "indexes"], ["identity", "managed", "columns", "constraints", "indexes"]);
  const id = checkIdentity(m.identity, `${at}.identity`);
  isBool(m.managed, `${at}.managed`);
  const key = tableKey(id.schema, id.name);
  if (st.tables.has(key)) fail("duplicate-table", `${at}.identity`, `table ${JSON.stringify(key)} is declared twice`);

  const columns = isArr(m.columns, `${at}.columns`);
  if (columns.length === 0) {
    fail("invalid-value", `${at}.columns`, `table ${JSON.stringify(key)} must declare at least one column`);
  }
  const colNames = new Set<string>();
  const colTypes = new Map<string, { name: string; isArray: boolean }>();
  let pkCount = 0;
  columns.forEach((col, j) => {
    const [name, colType] = validateColumn(col, `${at}.columns[${j}]`, st);
    if (colNames.has(name)) {
      fail("duplicate-column", `${at}.columns[${j}].name`, `column ${JSON.stringify(name)} is declared twice on table ${JSON.stringify(key)}`);
    }
    colNames.add(name);
    colTypes.set(name, colType);
  });

  const constraints = isArr(m.constraints, `${at}.constraints`);
  const consNames = new Set<string>();
  constraints.forEach((con, j) => {
    const [name, isPK] = validateConstraint(con, `${at}.constraints[${j}]`, colNames, key);
    if (consNames.has(name)) {
      fail("duplicate-constraint", `${at}.constraints[${j}].name`, `constraint ${JSON.stringify(name)} is declared twice on table ${JSON.stringify(key)}`);
    }
    consNames.add(name);
    if (isPK) pkCount++;
  });
  if (pkCount > 1) {
    fail("multiple-primary-key", `${at}.constraints`, `table ${JSON.stringify(key)} declares ${pkCount} primary-key constraints`);
  }

  isArr(m.indexes, `${at}.indexes`).forEach((idx, j) => {
    validateIndex(idx, `${at}.indexes[${j}]`, colNames, colTypes, key, st);
  });

  st.tables.set(key, m);
  st.tableColumns.set(key, colNames);
};

const validateColumn = (item: Json, at: string, st: State): [string, { name: string; isArray: boolean }] => {
  const m = isObj(item, at);
  onlyFields(m, at, ["name", "type", "notNull", "default", "generated"], ["name", "type", "notNull"]);
  const name = isStr(m.name, `${at}.name`);
  checkName(name, `${at}.name`);
  isBool(m.notNull, `${at}.notNull`);

  if ("generated" in m) {
    const gm = isObj(m.generated, `${at}.generated`);
    onlyFields(gm, `${at}.generated`, ["expression"], ["expression"]);
    checkSQLText(isStr(gm.expression, `${at}.generated.expression`), `${at}.generated.expression`, true);
    if ("default" in m) {
      fail("invalid-default", `${at}.default`, "a generated column cannot also declare a default");
    }
  }

  const typeObj = isObj(m.type, `${at}.type`);
  const { typeName, isArray } = validateColumnType(typeObj, `${at}.type`, st);

  if ("default" in m) {
    validateDefault(m.default, `${at}.default`, typeName, isArray);
    const d = m.default as Obj;
    if (d.kind === "sequence") {
      st.sequenceSchemas.push(checkIdentity(d.sequence, `${at}.default.sequence`).schema);
    } else if (d.kind === "identity" && "sequence" in d) {
      st.sequenceSchemas.push(checkIdentity(d.sequence as Json, `${at}.default.sequence`).schema);
    }
  }
  return [name, { name: typeName, isArray }];
};

const validateColumnType = (m: Obj, at: string, st: State): { typeName: string; isArray: boolean } => {
  onlyFields(m, at, ["name", "params", "array", "codec", "enum"], ["name", "codec"]);
  const typeName = isStr(m.name, `${at}.name`);
  const codec = isStr(m.codec, `${at}.codec`);
  const defaultCodec = TYPE_CODECS[typeName];
  if (defaultCodec === undefined) fail("unknown-type", `${at}.name`, `unknown column type ${JSON.stringify(typeName)}`);

  let isArray = false;
  if ("array" in m) isArray = isBool(m.array, `${at}.array`);
  const wantCodec = isArray ? "array" : defaultCodec;
  if (codec !== wantCodec) {
    fail("codec-mismatch", `${at}.codec`, `type ${typeName} (array=${isArray}) requires codec ${JSON.stringify(wantCodec)}, got ${JSON.stringify(codec)}`);
  }

  if (typeName === "enum") {
    if (!("enum" in m)) fail("missing-field", `${at}.enum`, 'type name "enum" requires an "enum" identity reference');
    const ref = checkIdentity(m.enum, `${at}.enum`);
    st.enumRefs.push({ at: `${at}.enum`, key: tableKey(ref.schema, ref.name) });
  } else if ("enum" in m) {
    fail("unknown-field", `${at}.enum`, 'enum reference is only valid on type "enum"');
  }

  if ("params" in m) {
    const pm = isObj(m.params, `${at}.params`);
    const allowed = TYPE_PARAMS[typeName];
    for (const k of Object.keys(pm).sort()) {
      if (allowed === undefined || !allowed.has(k)) {
        fail("invalid-type-params", `${at}.params.${k}`, `type ${typeName} accepts no parameter ${JSON.stringify(k)}`);
      }
      const iv = isInt(pm[k] as Json, `${at}.params.${k}`);
      if (k === "length" && (iv < 1 || iv > 10485760)) {
        fail("invalid-type-params", `${at}.params.length`, "length must be within 1..10485760");
      }
      if (k === "precision") {
        if (typeName === "numeric" && (iv < 1 || iv > 1000)) {
          fail("invalid-type-params", `${at}.params.precision`, "numeric precision must be within 1..1000");
        }
        if ((typeName === "timestamp" || typeName === "timestamptz") && (iv < 0 || iv > 6)) {
          fail("invalid-type-params", `${at}.params.precision`, "timestamp precision must be within 0..6");
        }
      }
      if (k === "scale") {
        if (!("precision" in pm)) fail("invalid-type-params", `${at}.params.scale`, "scale requires precision");
        const p = isInt(pm.precision as Json, `${at}.params.precision`);
        if (iv < 0 || iv > p) fail("invalid-type-params", `${at}.params.scale`, "scale must be within 0..precision");
      }
      if (k === "dimensions" && (iv < 1 || iv > 16000)) {
        fail("invalid-type-params", `${at}.params.dimensions`, "vector dimensions must be within 1..16000");
      }
    }
    if (typeName === "vector" && !("dimensions" in pm)) {
      fail("invalid-type-params", `${at}.params.dimensions`, "vector type requires dimensions");
    }
  } else if (typeName === "vector") {
    fail("invalid-type-params", `${at}.params`, "vector type requires params.dimensions");
  }

  if (typeName === "vector" && !st.capabilities.has("pgvector") && !st.capabilities.has("nucleus")) {
    fail("vector-capability", `${at}.name`, 'vector columns require capability "pgvector" or "nucleus"');
  }
  return { typeName, isArray };
};

const validateDefault = (v: Json, at: string, typeName: string, isArray: boolean): void => {
  const m = isObj(v, at);
  const kind = isStr(m.kind, `${at}.kind`);
  if (kind === "literal") {
    onlyFields(m, at, ["kind", "sql"], ["kind", "sql"]);
    const sql = isStr(m.sql, `${at}.sql`);
    if (!LITERAL_PATTERN.test(sql)) {
      fail("invalid-literal", `${at}.sql`, `default tagged literal must be exactly one SQL literal token, got ${JSON.stringify(sql)}`);
    }
    if (isArray && typeName === "enum") {
      fail("invalid-default", at, "enum array column defaults are explicitly unsupported — the enum element cast cannot be spelled as a contract literal");
    }
    const cast = literalCast(sql);
    if (cast !== null && (isArray || cast.depth > 0)) {
      if (!isArray || cast.depth === 0) {
        fail("invalid-default", `${at}.sql`, `default cast ::${cast.elem} does not match the array-ness of column type ${typeName} (PostgreSQL refuses it with SQLSTATE 42804 at apply)`);
      }
      if (normalizeTypeName(cast.elem) !== typeName) {
        fail("invalid-default", `${at}.sql`, `array default cast ::${cast.elem} does not match the column element type ${typeName} (PostgreSQL refuses it with SQLSTATE 42804 at apply)`);
      }
    }
    return;
  }
  if (kind === "expression") {
    onlyFields(m, at, ["kind", "sql"], ["kind", "sql"]);
    checkSQLText(isStr(m.sql, `${at}.sql`), `${at}.sql`, true);
    return;
  }
  if (kind === "identity") {
    onlyFields(m, at, ["kind", "generated", "sequence"], ["kind", "generated"]);
    if (!INTEGER_TYPES.has(typeName)) {
      fail("invalid-default", at, `identity defaults are only valid on integer columns, not ${typeName}`);
    }
    const gen = isStr(m.generated, `${at}.generated`);
    if (gen !== "always" && gen !== "by default") {
      fail("invalid-value", `${at}.generated`, 'generated must be "always" or "by default"');
    }
    return;
  }
  if (kind === "sequence") {
    onlyFields(m, at, ["kind", "sequence"], ["kind", "sequence"]);
    if (!INTEGER_TYPES.has(typeName)) {
      fail("invalid-default", at, `sequence defaults are only valid on integer columns, not ${typeName}`);
    }
    checkIdentity(m.sequence, `${at}.sequence`);
    return;
  }
  fail("invalid-value", `${at}.kind`, `unknown default kind ${JSON.stringify(kind)}`);
};

const checkDeferrable = (m: Obj, at: string): void => {
  if ("deferrable" in m) isBool(m.deferrable, `${at}.deferrable`);
  if ("initiallyDeferred" in m) {
    isBool(m.initiallyDeferred, `${at}.initiallyDeferred`);
    if (!("deferrable" in m)) fail("invalid-value", `${at}.initiallyDeferred`, "initiallyDeferred requires deferrable");
  }
};

const validateConstraint = (item: Json, at: string, colNames: Set<string>, table: string): [string, boolean] => {
  const m = isObj(item, at);
  const ctype = isStr(m.type, `${at}.type`);
  if (!["primary-key", "unique", "check", "foreign-key"].includes(ctype)) {
    fail("invalid-value", `${at}.type`, `unknown constraint type ${JSON.stringify(ctype)}`);
  }
  const allowed =
    ctype === "check"
      ? ["name", "type", "expression", "deferrable", "initiallyDeferred"]
      : ctype === "foreign-key"
        ? ["name", "type", "columns", "references", "deferrable", "initiallyDeferred"]
        : ["name", "type", "columns", "deferrable", "initiallyDeferred"];
  onlyFields(m, at, allowed, ["name", "type"]);
  const name = isStr(m.name, `${at}.name`);
  checkName(name, `${at}.name`);
  checkDeferrable(m, at);

  const checkColumns = (): string[] => {
    const arr = isArr(m.columns, `${at}.columns`);
    if (arr.length === 0) fail("invalid-value", `${at}.columns`, "constraint must list at least one column");
    const cols: string[] = [];
    const seen = new Set<string>();
    arr.forEach((c, i) => {
      const cat = `${at}.columns[${i}]`;
      const cs = isStr(c, cat);
      if (!colNames.has(cs)) {
        fail("constraint-column", cat, `constraint ${JSON.stringify(name)} references unknown column ${JSON.stringify(cs)}`);
      }
      if (seen.has(cs)) fail("constraint-column", cat, `constraint ${JSON.stringify(name)} lists column ${JSON.stringify(cs)} twice`);
      seen.add(cs);
      cols.push(cs);
    });
    return cols;
  };

  if (ctype === "primary-key" || ctype === "unique") {
    checkColumns();
    return [name, ctype === "primary-key"];
  }
  if (ctype === "check") {
    const expr = isStr(m.expression, `${at}.expression`);
    if (expr === "" || expr.includes(";")) {
      fail("check-expression", `${at}.expression`, "check expression must be non-empty and must not contain a statement separator");
    }
    return [name, false];
  }
  // foreign-key
  checkColumns();
  const refObj = isObj(m.references, `${at}.references`);
  onlyFields(refObj, `${at}.references`, ["table", "columns", "onDelete", "onUpdate", "match"], ["table", "columns"]);
  checkIdentity(refObj.table, `${at}.references.table`);
  const refCols = isArr(refObj.columns, `${at}.references.columns`);
  if (refCols.length === 0) fail("invalid-value", `${at}.references.columns`, "foreign-key references must list at least one column");
  refCols.forEach((c, i) => isStr(c, `${at}.references.columns[${i}]`));
  const fkCols = isArr(m.columns, `${at}.columns`);
  if (refCols.length !== fkCols.length) {
    fail("invalid-value", `${at}.references.columns`, `foreign-key references ${refCols.length} columns but the constraint lists ${fkCols.length}`);
  }
  for (const k of ["onDelete", "onUpdate"]) {
    if (k in refObj) {
      const s = isStr(refObj[k], `${at}.references.${k}`);
      if (!REFERENTIAL_ACTIONS.has(s)) fail("invalid-value", `${at}.references.${k}`, `unknown referential action ${JSON.stringify(s)}`);
    }
  }
  if ("match" in refObj) {
    const s = isStr(refObj.match, `${at}.references.match`);
    if (!FK_MATCHES.has(s)) fail("invalid-value", `${at}.references.match`, `unknown match type ${JSON.stringify(s)}`);
  }
  return [name, false];
};

const validateIndex = (item: Json, at: string, colNames: Set<string>, colTypes: Map<string, { name: string; isArray: boolean }>, table: string, st: State): void => {
  const m = isObj(item, at);
  onlyFields(m, at, ["identity", "unique", "method", "key", "where", "include"], ["identity", "unique", "method", "key"]);
  const id = checkIdentity(m.identity, `${at}.identity`);
  const ident = tableKey(id.schema, id.name);
  if (st.indexIdents.has(ident)) {
    fail("duplicate-index", `${at}.identity`, `index ${JSON.stringify(ident)} is declared twice; index names are schema-global`);
  }
  isBool(m.unique, `${at}.unique`);
  const method = isStr(m.method, `${at}.method`);
  if (!INDEX_METHODS.has(method)) fail("invalid-value", `${at}.method`, `unknown index method ${JSON.stringify(method)}`);
  const key = isArr(m.key, `${at}.key`);
  if (key.length === 0) fail("index-key", `${at}.key`, "index must declare at least one key part");
  key.forEach((part, i) => {
    const pat = `${at}.key[${i}]`;
    const pm = isObj(part, pat);
    onlyFields(pm, pat, ["column", "expression", "order", "nulls"], []);
    const hasCol = "column" in pm;
    const hasExpr = "expression" in pm;
    if (hasCol && hasExpr) fail("index-key", pat, "key part sets both column and expression");
    if ("order" in pm) {
      const o = isStr(pm.order, `${pat}.order`);
      if (o !== "asc" && o !== "desc") {
        fail("invalid-value", `${pat}.order`, `index key part order must be "asc" or "desc", got ${JSON.stringify(o)}`);
      }
    }
    if ("nulls" in pm) {
      const n = isStr(pm.nulls, `${pat}.nulls`);
      if (n !== "first" && n !== "last") {
        fail("invalid-value", `${pat}.nulls`, `index key part nulls must be "first" or "last", got ${JSON.stringify(n)}`);
      }
    }
    if (hasCol) {
      const cs = isStr(pm.column, `${pat}.column`);
      if (!colNames.has(cs)) {
        fail("constraint-column", `${pat}.column`, `index ${JSON.stringify(ident)} references unknown column ${JSON.stringify(cs)}`);
      }
      const ct = colTypes.get(cs);
      if (ct !== undefined) {
        const applicable = ct.isArray ? INDEX_METHOD_ARRAYS.has(method) : (INDEX_METHOD_SCALARS[method] ?? new Set()).has(ct.name);
        if (!applicable) {
          const desc = ct.isArray ? `${ct.name}[]` : ct.name;
          fail("invalid-index", pat, `index method ${JSON.stringify(method)} over column ${JSON.stringify(cs)} (${desc}) has no default operator class on any supported server (PostgreSQL refuses it with SQLSTATE 42704) — explicitly unsupported: the contract has no operator-class slot`);
        }
      }
    } else if (hasExpr) {
      checkSQLText(isStr(pm.expression, `${pat}.expression`), `${pat}.expression`, true);
      if (method !== "btree") {
        fail("invalid-index", pat, 'expression keys are only definable with method "btree" — the default operator class of an expression result type cannot be verified at definition time (the contract has no operator-class slot)');
      }
    } else {
      fail("index-key", pat, "key part needs either column or expression");
    }
  });
  if ("where" in m) checkSQLText(isStr(m.where, `${at}.where`), `${at}.where`, true);
  if ("include" in m) {
    const arr = isArr(m.include, `${at}.include`);
    const seen = new Set<string>();
    arr.forEach((c, i) => {
      const iat = `${at}.include[${i}]`;
      const cs = isStr(c, iat);
      if (!colNames.has(cs)) {
        fail("constraint-column", iat, `index ${JSON.stringify(ident)} includes unknown column ${JSON.stringify(cs)}`);
      }
      if (seen.has(cs)) fail("invalid-value", iat, `index ${JSON.stringify(ident)} includes column ${JSON.stringify(cs)} twice`);
      seen.add(cs);
    });
  }
  st.indexIdents.set(ident, table);
};

const crossReferences = (st: State): void => {
  const schemaOf = (key: string): string => key.slice(0, key.indexOf("."));
  for (const key of st.tables.keys()) {
    if (!st.schemas.has(schemaOf(key))) {
      fail("undeclared-schema", "$.tables", `table ${JSON.stringify(key)} lives in undeclared schema ${JSON.stringify(schemaOf(key))}`);
    }
  }
  for (const key of st.enums.keys()) {
    if (!st.schemas.has(schemaOf(key))) fail("undeclared-schema", "$.enums", `enum ${JSON.stringify(key)} lives in an undeclared schema`);
  }
  for (const key of st.views.keys()) {
    if (!st.schemas.has(schemaOf(key))) fail("undeclared-schema", "$.views", `view ${JSON.stringify(key)} lives in an undeclared schema`);
  }
  for (const ident of st.indexIdents.keys()) {
    if (!st.schemas.has(schemaOf(ident))) fail("undeclared-schema", "$.tables", `index ${JSON.stringify(ident)} lives in an undeclared schema`);
  }
  for (const schema of st.sequenceSchemas) {
    if (!st.schemas.has(schema)) {
      fail("undeclared-schema", "$.tables", `default sequence lives in undeclared schema ${JSON.stringify(schema)}`);
    }
  }
  for (const ref of st.enumRefs) {
    if (!st.enums.has(ref.key)) {
      fail("enum-unresolved", ref.at, `column references enum ${JSON.stringify(ref.key)}, which is not declared`);
    }
  }

  const sortedKeys = [...st.tables.keys()].sort();
  for (const key of sortedKeys) {
    const table = st.tables.get(key)!;
    const columns = new Map<string, Obj>();
    for (const c of table.columns as Json[]) {
      const cm = c as Obj;
      columns.set(cm.name as string, cm);
    }
    const tuples: string[][] = st.tableKeyTuples.get(key) ?? [];
    for (const con of table.constraints as Json[]) {
      const cm = con as Obj;
      const ctype = cm.type as string;
      if (ctype === "primary-key" || ctype === "unique") {
        const cols = (cm.columns as Json[]).map((c) => c as string);
        if (ctype === "primary-key") {
          for (const cn of cols) {
            const col = columns.get(cn);
            if (col && col.notNull !== true) {
              fail("pk-not-null", "$.tables", `primary-key column ${JSON.stringify(cn)} on table ${JSON.stringify(key)} must declare notNull: true`);
            }
          }
        }
        tuples.push(cols);
      }
    }
    st.tableKeyTuples.set(key, tuples);
  }
  for (const key of sortedKeys) {
    const table = st.tables.get(key)!;
    for (const con of table.constraints as Json[]) {
      const cm = con as Obj;
      if (cm.type !== "foreign-key") continue;
      const ref = cm.references as Obj;
      const refTable = ref.table as Obj;
      const target = tableKey(refTable.schema as string, refTable.name as string);
      if (!st.tables.has(target)) {
        fail("fk-target", "$.tables", `foreign key on ${JSON.stringify(key)} references table ${JSON.stringify(target)}, which is not declared`);
      }
      const refCols = (ref.columns as Json[]).map((c) => c as string);
      const targetCols = st.tableColumns.get(target)!;
      for (const rc of refCols) {
        if (!targetCols.has(rc)) {
          fail("fk-column", "$.tables", `foreign key on ${JSON.stringify(key)} references column ${JSON.stringify(rc)}, which does not exist on ${JSON.stringify(target)}`);
        }
      }
      const tuples = st.tableKeyTuples.get(target) ?? [];
      const covered = tuples.some((t) => t.length === refCols.length && t.every((c, i) => c === refCols[i]));
      if (!covered) {
        fail("fk-not-unique", "$.tables", `foreign key on ${JSON.stringify(key)} references (${refCols.join(", ")}) on ${JSON.stringify(target)}, which is not covered by a primary-key or unique constraint`);
      }
    }
  }
};

// ---------------------------------------------------------------------------
// Canonical form (mirrors v2CanonicalBytes / CANONICAL.md)
// ---------------------------------------------------------------------------

const byteCompare = (a: string, b: string): number => {
  const ac = [...a];
  const bc = [...b];
  const n = Math.min(ac.length, bc.length);
  for (let i = 0; i < n; i++) {
    const x = ac[i].codePointAt(0)!;
    const y = bc[i].codePointAt(0)!;
    if (x !== y) return x - y;
  }
  return ac.length - bc.length;
};

const sortBy = (arr: Json[], keyOf: (m: Obj) => string): Json[] =>
  [...arr].sort((a, b) => byteCompare(keyOf(a as Obj), keyOf(b as Obj)));

const fieldKey = (m: Obj, field: string): string => {
  const v = m[field];
  if (typeof v === "string") return v;
  if (v !== null && typeof v === "object" && !Array.isArray(v)) {
    const id = v as Obj;
    return `${id.schema as string}|${id.name as string}`;
  }
  return "";
};

const canonicalize = (v: Json): Json => {
  if (v === null || typeof v !== "object") return v;
  if (Array.isArray(v)) return v.map(canonicalize);
  const out: Obj = {};
  const src = v as Obj;
  if ("version" in src && "dialect" in src) {
    // document root: sort set collections
    for (const k of Object.keys(src)) {
      const val = src[k] as Json;
      if (k === "capabilities") out[k] = sortBy(val as Json[], (m) => m as string) as Json;
      else if (k === "schemas") out[k] = sortBy(val as Json[], (m) => fieldKey(m, "name")) as Json;
      else if (k === "enums" || k === "views") out[k] = sortBy(val as Json[], (m) => fieldKey(m, "identity")) as Json;
      else if (k === "opaque") {
        out[k] = sortBy(val as Json[], (m) => {
          const id = m.identity as Obj;
          return `${id.schema}|${id.name}|${m.kind}`;
        }) as Json;
      } else if (k === "tables") {
        out[k] = sortBy(val as Json[], (m) => fieldKey(m, "identity")).map((tv) => {
          const t = { ...(tv as Obj) } as Obj;
          // columns are an ordered tuple: physical column order is semantic
          // (CANONICAL.md §2) and is preserved verbatim.
          t.constraints = sortBy(t.constraints as Json[], (m) => fieldKey(m, "name")) as Json;
          t.indexes = sortBy(t.indexes as Json[], (m) => fieldKey(m, "identity")).map((iv) => {
            const idx = { ...(iv as Obj) } as Obj;
            if ("include" in idx) idx.include = sortBy(idx.include as Json[], (m) => m as string) as Json;
            return idx as Json;
          }) as Json;
          return t as Json;
        }) as Json;
      } else out[k] = canonicalize(val);
    }
    return out;
  }
  for (const k of Object.keys(src)) out[k] = canonicalize(src[k] as Json);
  return out;
};

const canonicalString = (s: string): string => {
  let out = '"';
  for (const ch of s) {
    const cp = ch.codePointAt(0)!;
    if (cp === 0x22) out += '\\"';
    else if (cp === 0x5c) out += "\\\\";
    else if (cp === 8) out += "\\b";
    else if (cp === 9) out += "\\t";
    else if (cp === 10) out += "\\n";
    else if (cp === 12) out += "\\f";
    else if (cp === 13) out += "\\r";
    else if (cp < 0x20) out += "\\u00" + cp.toString(16).padStart(2, "0");
    else out += ch;
  }
  return out + '"';
};

const serialize = (v: Json): string => {
  if (v === null) return "null";
  if (typeof v === "boolean") return v ? "true" : "false";
  if (typeof v === "string") return canonicalString(v);
  if (typeof v === "number") return String(v); // validated safe integer
  if (Array.isArray(v)) return "[" + v.map(serialize).join(",") + "]";
  const keys = Object.keys(v).sort(byteCompare);
  return "{" + keys.map((k) => canonicalString(k) + ":" + serialize((v as Obj)[k] as Json)).join(",") + "}";
};

const parseValidateHash = (raw: string): { canonical: string; sha256: string } => {
  let parsed: Json;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    // Malformed JSON and parser stack overflow (deep nesting) both fail
    // cleanly with the same code Go reports (CANONICAL.md §5).
    fail("invalid-json", "$", err instanceof RangeError ? "parser recursion depth exceeded" : String(err));
  }
  let canonical: string;
  try {
    checkDepth(parsed);
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
      fail("not-object", "$", "schema document must be a JSON object");
    }
    scanStrings(parsed, "$");
    validateDocument(parsed as Obj);
    canonical = serialize(canonicalize(parsed));
  } catch (err) {
    if (err instanceof RangeError) fail("invalid-json", "$", "recursion depth exceeded while processing the document");
    throw err;
  }
  return { canonical, sha256: createHash("sha256").update(canonical, "utf8").digest("hex") };
};

// ---------------------------------------------------------------------------
// Fixture runner
// ---------------------------------------------------------------------------

interface Manifest {
  valid: { name: string; document: string; canonical: string; sha256: string }[];
  invalid: { name: string; document: string; code: string }[];
  v1: { name: string; document: string; canonical?: string; sha256?: string; code?: string }[];
}

const readFixture = (rel: string): string => readFileSync(path.join(goldenDir, rel), "utf8");

let passed = 0;
let failed = 0;
const problems: string[] = [];
const note = (ok: boolean, what: string, detail?: string): void => {
  if (ok) passed++;
  else {
    failed++;
    problems.push(detail ? `${what}: ${detail}` : what);
  }
};

const manifest: Manifest = JSON.parse(readFixture("manifest.json"));

for (const fx of manifest.valid) {
  try {
    const { canonical, sha256 } = parseValidateHash(readFixture(fx.document));
    note(sha256 === fx.sha256, `${fx.name}: sha256 agreement`, `manifest ${fx.sha256} vs ts ${sha256}`);
    const expected = readFixture(fx.canonical);
    note(canonical === expected, `${fx.name}: canonical bytes agreement`, `expected ${expected.slice(0, 80)}... got ${canonical.slice(0, 80)}...`);

    // formatting absorption: reversed key order + reindentation
    const reshuffled = JSON.stringify(reverseKeys(JSON.parse(readFixture(fx.document))), null, 3);
    const redone = parseValidateHash(reshuffled);
    note(redone.sha256 === fx.sha256, `${fx.name}: formatting-only reformat keeps the hash`, `${fx.sha256} vs ${redone.sha256}`);
  } catch (err) {
    note(false, `${fx.name}: expected valid`, String(err));
  }
}

// order-insensitive must equal basic-users byte-for-byte (independent of Go)
{
  const basic = parseValidateHash(readFixture("valid/basic-users.json"));
  const shuffled = parseValidateHash(readFixture("valid/order-insensitive.json"));
  note(basic.canonical === shuffled.canonical && basic.sha256 === shuffled.sha256, "order-insensitive equals basic-users");
  const respelled = readFixture("valid/basic-users.json").replace('"length": 255', '"length": 2.55e2');
  note(parseValidateHash(respelled).sha256 === basic.sha256, "numeric respelling 255 -> 2.55e2 keeps the hash");

  // Column order is an ordered tuple: rotating column arrays changes the hash.
  const rotated = parseValidateHash(readFixture("valid/column-order.json"));
  note(rotated.sha256 !== basic.sha256, "column-order hashes differently from basic-users", `both ${basic.sha256}`);
}

// Bytewise set sorting pinned by non-BMP names (U+FFFF vs U+1F600): UTF-16
// code-unit order (JS default sort) would put the emoji first.
{
  const canon = JSON.parse(readFixture("valid/unicode-sort.canonical.json")) as Obj;
  const names = (canon.tables as Json[]).map((tv) => ((tv as Obj).identity as Obj).name as string);
  note(names[0] === "zz\uFFFF" && names[1] === "zz\u{1F600}", "unicode-sort canonical order is bytewise, not UTF-16 code-unit order", JSON.stringify(names));
}

// Rejection-code hygiene both languages must agree on (CANONICAL.md §5).
{
  try {
    parseValidateHash("[".repeat(15000) + "]".repeat(15000));
    note(false, "deeply nested input rejected", "accepted instead");
  } catch (err) {
    note(err instanceof ContractError && err.code === "invalid-json", "deeply nested input rejected [invalid-json]", String(err));
  }
  try {
    parseValidateHash(`{"version":"2","dialect":"postgresql","capabilities":[],"schemas":[],"tables":[],"enums":[],"views":[],"opaque":[]}`);
    note(false, 'version "2" rejected', "accepted instead");
  } catch (err) {
    note(err instanceof ContractError && err.code === "invalid-number", 'version "2" rejected [invalid-number]', String(err));
  }
}

for (const fx of manifest.invalid) {
  try {
    parseValidateHash(readFixture(fx.document));
    note(false, `${fx.name}: expected rejection [${fx.code}]`, "accepted instead");
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    note(msg.includes(`[${fx.code}]`), `${fx.name}: rejection code [${fx.code}]`, `got ${msg}`);
  }
}

let v1GoOnly = 0;
for (const fx of manifest.v1) {
  if (fx.code) {
    // The v1 upgrade reader is a Go-side component; the consumer only checks
    // the fixture is a well-formed v1 document.
    const doc = JSON.parse(readFixture(fx.document));
    note(doc.version === 1 && Array.isArray(doc.tables), `${fx.name}: v1 shape`);
    v1GoOnly++;
    continue;
  }
  try {
    // The expected canonical output of the Go upgrade must itself be a valid
    // v2 document hashing to the recorded value: this is the cross-language
    // agreement point for upgrade results.
    const { sha256 } = parseValidateHash(readFixture(fx.canonical!));
    note(sha256 === fx.sha256, `${fx.name}: upgraded canonical revalidates with the recorded hash`, `${fx.sha256} vs ${sha256}`);
  } catch (err) {
    note(false, `${fx.name}: expected valid upgraded canonical`, String(err));
  }
}

// Hash sensitivity: independent mutations, same classes as the Go test.
const mutations: { fixture: string; what: string; apply: (root: Obj) => void }[] = [
  {
    fixture: "basic-users",
    what: "type parameter value",
    apply: (r) => {
      ((findCol(findTable(r, "public.users"), "email").type as Obj).params as Obj).length = 256;
    },
  },
  {
    fixture: "basic-users",
    what: "nullability flip",
    apply: (r) => {
      findCol(findTable(r, "public.users"), "name").notNull = true;
    },
  },
  {
    fixture: "basic-users",
    what: "column declaration order",
    apply: (r) => {
      const users = findTable(r, "public.users");
      const cols = users.columns as Json[];
      users.columns = [cols[cols.length - 1], ...cols.slice(0, -1)];
    },
  },
  {
    fixture: "basic-users",
    what: "literal default value",
    apply: (r) => {
      (findCol(findTable(r, "public.posts"), "views_count").default as Obj).sql = "1";
    },
  },
  {
    fixture: "basic-users",
    what: "default tag",
    apply: (r) => {
      findCol(findTable(r, "public.users"), "active").default = { kind: "expression", sql: "true" };
    },
  },
  {
    fixture: "tenant-composite",
    what: "composite primary-key column order",
    apply: (r) => {
      const con = findCon(findTable(r, "app.memberships"), "memberships_pkey");
      con.columns = [(con.columns as Json[])[1], (con.columns as Json[])[0]];
    },
  },
  {
    fixture: "tenant-composite",
    what: "index key order",
    apply: (r) => {
      const idx = findIdx(findTable(r, "app.org_settings"), "org_settings_note_idx");
      idx.key = [(idx.key as Json[])[1], (idx.key as Json[])[0]];
    },
  },
  {
    fixture: "tenant-composite",
    what: "fk referential action",
    apply: (r) => {
      (findCon(findTable(r, "public.users"), "users_tenant_fkey").references as Obj).onDelete = "restrict";
    },
  },
  {
    fixture: "tagged-defaults",
    what: "enum value order",
    apply: (r) => {
      const ev = (r.enums as Json[])[0] as Obj;
      ev.values = [(ev.values as Json[])[1], (ev.values as Json[])[0], (ev.values as Json[])[2]];
    },
  },
  {
    fixture: "tagged-defaults",
    what: "literal that looks like SQL",
    apply: (r) => {
      (findCol(findTable(r, "public.events"), "note").default as Obj).sql = "'later()'";
    },
  },
  {
    fixture: "catalog-surface",
    what: "view definition text",
    apply: (r) => {
      for (const vv of r.views as Json[]) {
        const vm = vv as Obj;
        const id = vm.identity as Obj;
        if (id.name === "v_active") vm.definition = "select name from public.indexes where type = 'inactive'";
      }
    },
  },
  {
    fixture: "catalog-surface",
    what: "metadata-like table rename",
    apply: (r) => {
      (findTable(r, "public.indexes").identity as Obj).name = "indexes2";
    },
  },
  {
    fixture: "empty-managed",
    what: "empty document gains a capability",
    apply: (r) => {
      r.capabilities = ["pgvector"];
    },
  },
];

const findTable = (root: Obj, key: string): Obj => {
  for (const tv of root.tables as Json[]) {
    const tm = tv as Obj;
    const id = tm.identity as Obj;
    if (tableKey(id.schema as string, id.name as string) === key) return tm;
  }
  throw new Error(`table ${key} not found`);
};
const findCol = (table: Obj, name: string): Obj => {
  for (const cv of table.columns as Json[]) {
    if ((cv as Obj).name === name) return cv as Obj;
  }
  throw new Error(`column ${name} not found`);
};
const findCon = (table: Obj, name: string): Obj => {
  for (const cv of table.constraints as Json[]) {
    if ((cv as Obj).name === name) return cv as Obj;
  }
  throw new Error(`constraint ${name} not found`);
};
const findIdx = (table: Obj, name: string): Obj => {
  for (const iv of table.indexes as Json[]) {
    if (((iv as Obj).identity as Obj).name === name) return iv as Obj;
  }
  throw new Error(`index ${name} not found`);
};

const baseHashes = new Map(manifest.valid.map((f) => [f.name, f.sha256]));
for (const m of mutations) {
  try {
    const canonical = readFixture(manifest.valid.find((f) => f.name === m.fixture)!.canonical);
    const tree = JSON.parse(canonical) as Obj;
    m.apply(tree);
    const { sha256 } = parseValidateHash(JSON.stringify(tree));
    note(sha256 !== baseHashes.get(m.fixture), `sensitivity: ${m.fixture}: ${m.what} changes the hash`, `hash unchanged at ${sha256}`);
  } catch (err) {
    note(false, `sensitivity: ${m.fixture}: ${m.what}`, String(err));
  }
}

function reverseKeys(v: unknown): unknown {
  if (Array.isArray(v)) return v.map(reverseKeys);
  if (v !== null && typeof v === "object") {
    const out: Record<string, unknown> = {};
    for (const k of Object.keys(v).reverse()) out[k] = reverseKeys((v as Record<string, unknown>)[k]);
    return out;
  }
  return v;
}

// Orphan check: every fixture file on disk appears in the manifest.
{
  const referenced = new Set<string>([
    ...manifest.valid.flatMap((f) => [f.document, f.canonical]),
    ...manifest.invalid.map((f) => f.document),
    ...manifest.v1.flatMap((f) => (f.code ? [f.document] : [f.document, f.canonical!])),
  ]);
  for (const dir of ["valid", "invalid", "v1"]) {
    for (const f of readdirSync(path.join(goldenDir, dir))) {
      if (f.endsWith(".json")) {
        note(referenced.has(`${dir}/${f}`), `manifest covers ${dir}/${f}`);
      }
    }
  }
}

console.log(`schema contract consumer: ${passed} passed, ${failed} failed, ${v1GoOnly} go-only v1 entries`);
if (problems.length > 0) {
  console.error(problems.map((p) => `  - ${p}`).join("\n"));
  process.exit(1);
}
