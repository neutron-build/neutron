// ---------------------------------------------------------------------------
// @neutron-build/sql — value codecs (master codec table)
// ---------------------------------------------------------------------------
// Read path: schema-known columns are acquired as lossless text IN SQL
// (`to_jsonb(col)::text` temporal forms, `::text` int8/numeric inside JSON) or
// as driver-native values that are already exact (int8/numeric strings, bytea
// buffers), then decoded per the column's mode. Nothing here mutates global
// driver parsers; postgres.js re-encodes server-typed date/json params through
// Date/JSON.stringify, so temporal and json/jsonb writes bind canonical text
// at explicitly text-typed sites (`$n::text::<sqltype>`), which both drivers
// pass through untouched and which keeps microsecond and scale digits intact.

import type { AnyColumnBuilder, ColumnDataType } from "./schema.js";

export type BigintMode = "bigint" | "string" | "number";
export type TemporalMode = "string" | "date";

export interface BigintOptions<M extends BigintMode = BigintMode> {
  mode?: M;
}

export interface TemporalOptions<M extends TemporalMode = TemporalMode> {
  mode?: M;
}

export interface NumericOptions<D extends (raw: string) => unknown = (raw: string) => unknown> {
  /** Optional user decoder for the exact decimal string. The default keeps
   *  the string; a decoder that narrows to number owns the precision loss. */
  decoder?: D;
}

export const JSON_NULL_BRAND = Symbol.for("@neutron-build/sql.jsonNull");

/** SQL NULL and JSON null are different values. `null` binds SQL NULL on
 *  json/jsonb columns; `jsonNull` writes the JSON null value. */
export interface JsonNullValue {
  readonly [JSON_NULL_BRAND]: true;
}

export const jsonNull: JsonNullValue = Object.freeze({ [JSON_NULL_BRAND]: true }) as JsonNullValue;

export function isJsonNull(value: unknown): value is JsonNullValue {
  return typeof value === "object" && value !== null && (value as { [JSON_NULL_BRAND]?: unknown })[JSON_NULL_BRAND] === true;
}

export interface ColumnContext {
  propertyKey: string;
  columnName: string;
  tableName: string;
}

function codecError(ctx: ColumnContext, reason: string): Error {
  return new Error(`column "${ctx.propertyKey}" ("${ctx.columnName}") on ${ctx.tableName}: ${reason}`);
}

// ---------------------------------------------------------------------------
// Read acquisition
// ---------------------------------------------------------------------------

/** SQL expression acquiring a lossless value for one column reference, or
 *  null when the driver-native value is already exact for this type. */
export function wireReadExpr(dataType: ColumnDataType, ref: string): string | null {
  switch (dataType) {
    case "timestamp":
    case "date":
      // to_jsonb renders temporal values in fixed ISO form (T separator,
      // microsecond digits preserved, DateStyle-independent) as a JSON string.
      return `to_jsonb(${ref})::text`;
    case "timestamptz":
      // Render the UTC wall clock (session-timezone independent).
      return `to_jsonb(${ref} at time zone 'UTC')::text`;
    default:
      // int8/numeric arrive as exact strings natively on both drivers;
      // bytea arrives as a buffer; the rest are exact JS scalars.
      return null;
  }
}

/** Whether a column's projected value must be text-acquired via wireReadExpr. */
export function usesTextWire(dataType: ColumnDataType): boolean {
  return wireReadExpr(dataType, "x") !== null;
}

function stripJsonQuotes(raw: unknown, ctx: ColumnContext): string {
  if (typeof raw !== "string" || raw.length < 2 || !raw.startsWith('"') || !raw.endsWith('"')) {
    throw codecError(ctx, `expected a JSON-quoted wire string, got ${typeof raw}`);
  }
  return raw.slice(1, -1);
}

const CANONICAL_TIMESTAMP = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d{1,6})?$/;
const CANONICAL_TIMESTAMPTZ = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+-]\d{2}:\d{2})$/;
const CANONICAL_DATE = /^\d{4}-\d{2}-\d{2}$/;
const TEMPORAL_INFINITY = /^(infinity|-infinity)$/;

function timestampToInstantMs(canonical: string, ctx: ColumnContext): number {
  // timezone-free wall clocks interpret as UTC so a Date-mode value is the
  // same instant on every machine; canonical strings with offsets parse exact.
  const hasOffset = /[Zz]$|[+-]\d{2}:\d{2}$/.test(canonical);
  const ms = Date.parse(`${canonical.replace(" ", "T")}${hasOffset ? "" : "Z"}`);
  if (Number.isNaN(ms)) {
    throw codecError(ctx, `cannot convert "${canonical}" to a Date`);
  }
  return ms;
}

/** Decode a to_jsonb text wire value (quoted string) to the column's value. */
export function decodeTextWire(column: AnyColumnBuilder, ctx: ColumnContext, raw: unknown): unknown {
  const inner = stripJsonQuotes(raw, ctx);
  const dt = column.dataType;
  if (dt === "timestamp") {
    if (!CANONICAL_TIMESTAMP.test(inner) && !TEMPORAL_INFINITY.test(inner)) {
      throw codecError(ctx, `unexpected timestamp wire form "${inner}"`);
    }
    if (column.readMode === "date") {
      if (TEMPORAL_INFINITY.test(inner)) {
        throw codecError(ctx, `"${inner}" cannot be represented as a Date (Date mode)`);
      }
      return new Date(timestampToInstantMs(inner, ctx));
    }
    return inner;
  }
  if (dt === "timestamptz") {
    if (TEMPORAL_INFINITY.test(inner)) {
      if (column.readMode === "date") {
        throw codecError(ctx, `"${inner}" cannot be represented as a Date (Date mode)`);
      }
      return inner;
    }
    const canonical = `${inner}Z`;
    if (!CANONICAL_TIMESTAMPTZ.test(canonical)) {
      throw codecError(ctx, `unexpected timestamptz wire form "${inner}"`);
    }
    if (column.readMode === "date") {
      return new Date(timestampToInstantMs(canonical, ctx));
    }
    return canonical;
  }
  if (dt === "date") {
    if (!CANONICAL_DATE.test(inner)) {
      throw codecError(ctx, `unexpected date wire form "${inner}"`);
    }
    return inner;
  }
  throw codecError(ctx, `text wire decode is not defined for ${dt} columns`);
}

function decodeBigintString(text: string, column: AnyColumnBuilder, ctx: ColumnContext): unknown {
  if (!/^[+-]?\d+$/.test(text)) {
    throw codecError(ctx, `unexpected int8 wire form "${text}"`);
  }
  const mode = column.readMode ?? "bigint";
  if (mode === "string") return text;
  const value = BigInt(text);
  if (mode === "number") {
    if (value > 9007199254740991n || value < -9007199254740991n) {
      throw codecError(
        ctx,
        `int8 value ${text} overflows the JS safe integer range — use mode "bigint" or "string" for this column`,
      );
    }
    return Number(value);
  }
  return value;
}

function decodeNumericString(text: string, column: AnyColumnBuilder, ctx: ColumnContext): unknown {
  if (!/^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$/.test(text)) {
    throw codecError(ctx, `unexpected numeric wire form "${text}"`);
  }
  return column.valueDecoder ? column.valueDecoder(text) : text;
}

/** Decode a driver-native flat-path value (int8/numeric arrive as strings on
 *  both drivers; bytea arrives as a buffer) to the column's final value. */
export function decodeNativeValue(column: AnyColumnBuilder, ctx: ColumnContext, raw: unknown): unknown {
  if (raw === null || raw === undefined) return raw;
  switch (column.dataType) {
    case "bigint":
      if (typeof raw !== "string" && typeof raw !== "bigint" && typeof raw !== "number") {
        throw codecError(ctx, `unexpected int8 value of type ${typeof raw}`);
      }
      return decodeBigintString(String(raw), column, ctx);
    case "numeric":
      if (typeof raw !== "string" && typeof raw !== "number") {
        throw codecError(ctx, `unexpected numeric value of type ${typeof raw}`);
      }
      return decodeNumericString(String(raw), column, ctx);
    case "bytea":
      if (!(raw instanceof Uint8Array)) {
        throw codecError(ctx, `unexpected bytea value of type ${typeof raw}`);
      }
      return raw;
    default:
      return raw;
  }
}

export function byteaFromHex(text: string, ctx: ColumnContext): Uint8Array {
  if (!/^\\x[0-9a-f]*$/i.test(text)) {
    throw codecError(ctx, `unexpected bytea wire form (expected \\x hex text)`);
  }
  const hex = text.slice(2);
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) {
    out[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

/** Decode one relation-child leaf after JSON parsing. The JSON projection
 *  renders int8/numeric as ::text and bytea as its \x hex text form (see
 *  relations.ts), so precision survives JSON.parse and decodes here. */
export function decodeJsonLeaf(column: AnyColumnBuilder, ctx: ColumnContext, raw: unknown): unknown {
  if (raw === null || raw === undefined) return raw;
  switch (column.dataType) {
    case "bigint":
    case "numeric": {
      if (typeof raw !== "string" && typeof raw !== "number") {
        throw codecError(ctx, `unexpected ${column.dataType} child leaf of type ${typeof raw}`);
      }
      return column.dataType === "bigint"
        ? decodeBigintString(String(raw), column, ctx)
        : decodeNumericString(String(raw), column, ctx);
    }
    case "timestamptz": {
      if (typeof raw !== "string") {
        throw codecError(ctx, `unexpected timestamptz child leaf of type ${typeof raw}`);
      }
      if (TEMPORAL_INFINITY.test(raw)) {
        if (column.readMode === "date") {
          throw codecError(ctx, `"${raw}" cannot be represented as a Date (Date mode)`);
        }
        return raw;
      }
      const canonical = `${raw}Z`;
      if (!CANONICAL_TIMESTAMPTZ.test(canonical)) {
        throw codecError(ctx, `unexpected timestamptz wire form "${raw}"`);
      }
      if (column.readMode === "date") {
        return new Date(timestampToInstantMs(canonical, ctx));
      }
      return canonical;
    }
    case "timestamp":
    case "date": {
      if (typeof raw !== "string") {
        throw codecError(ctx, `unexpected ${column.dataType} child leaf of type ${typeof raw}`);
      }
      if (column.readMode === "date") {
        if (TEMPORAL_INFINITY.test(raw)) {
          throw codecError(ctx, `"${raw}" cannot be represented as a Date (Date mode)`);
        }
        return new Date(timestampToInstantMs(raw.replace(" ", "T"), ctx));
      }
      return raw;
    }
    case "bytea": {
      if (typeof raw !== "string") {
        throw codecError(ctx, `unexpected bytea child leaf of type ${typeof raw}`);
      }
      return byteaFromHex(raw, ctx);
    }
    default:
      return raw;
  }
}

// ---------------------------------------------------------------------------
// Write encoding (values bind as parameters; temporal/json sites cast text)
// ---------------------------------------------------------------------------

export interface EncodedValue {
  bind: unknown;
  /** When set, the parameter site must render `$n::text::<cast>` so the
   *  parameter stays a text value on both drivers. */
  cast?: string;
}

export function writeCastTarget(dataType: ColumnDataType): string | undefined {
  switch (dataType) {
    case "timestamp":
    case "timestamptz":
    case "date":
    case "json":
    case "jsonb":
      return sqlTypeName(dataType);
    default:
      return undefined;
  }
}

function sqlTypeName(dataType: ColumnDataType): string {
  return dataType === "timestamptz" ? "timestamptz" : dataType;
}

const INT8_MIN = -9223372036854775808n;
const INT8_MAX = 9223372036854775807n;

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null) return false;
  const proto: unknown = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

function jsonRepresentable(value: unknown): boolean {
  if (value === null) return true;
  if (isJsonNull(value)) return true;
  const t = typeof value;
  if (t === "string" || t === "boolean") return true;
  if (t === "number") return Number.isFinite(value);
  if (t === "object") {
    if (Array.isArray(value)) return value.every(jsonRepresentable);
    if (isPlainObject(value)) return Object.values(value).every(jsonRepresentable);
  }
  return false;
}

/** Validate and encode a non-null, non-undefined write value for a column.
 *  Returns the bind value (plus a cast requirement for temporal/json sites)
 *  or throws with column context. */
export function encodeWriteValue(column: AnyColumnBuilder, ctx: ColumnContext, value: unknown): EncodedValue {
  const dt = column.dataType;
  const t = typeof value;
  if (t === "symbol" || t === "function" || t === "undefined") {
    throw codecError(ctx, `${t} values cannot be bound`);
  }
  switch (dt) {
    case "serial":
    case "integer": {
      if (typeof value !== "number" || !Number.isInteger(value)) {
        throw codecError(ctx, `${dt} columns accept integer numbers`);
      }
      if (value < -2147483648 || value > 2147483647) {
        throw codecError(ctx, `integer value ${value} is out of int4 range`);
      }
      return { bind: value };
    }
    case "smallint": {
      if (typeof value !== "number" || !Number.isInteger(value)) {
        throw codecError(ctx, "smallint columns accept integer numbers");
      }
      if (value < -32768 || value > 32767) {
        throw codecError(ctx, `integer value ${value} is out of int2 range`);
      }
      return { bind: value };
    }
    case "bigint": {
      if (t === "bigint") return { bind: value };
      if (t === "number") {
        if (!Number.isSafeInteger(value)) {
          throw codecError(ctx, `number ${value} is not a safe integer for an int8 column — pass the exact value as a string or bigint`);
        }
        return { bind: value };
      }
      if (typeof value === "string") {
        if (!/^[+-]?\d+$/.test(value)) {
          throw codecError(ctx, `"${value}" is not an integer string`);
        }
        const asBig = BigInt(value);
        if (asBig < INT8_MIN || asBig > INT8_MAX) {
          throw codecError(ctx, `int8 value ${value} is out of range`);
        }
        return { bind: value };
      }
      throw codecError(ctx, `${describeValue(value)} is not bindable for bigint columns`);
    }
    case "numeric": {
      if (typeof value === "number") {
        if (!Number.isFinite(value)) throw codecError(ctx, "non-finite numbers are not bindable");
        return { bind: value };
      }
      if (typeof value === "string") {
        if (!/^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$/.test(value)) {
          throw codecError(ctx, `"${value}" is not a decimal string`);
        }
        return { bind: value };
      }
      throw codecError(ctx, `${describeValue(value)} is not bindable for numeric columns`);
    }
    case "double":
    case "real": {
      if (typeof value !== "number" || !Number.isFinite(value)) {
        throw codecError(ctx, `${dt} columns accept finite numbers (NaN/Infinity are rejected)`);
      }
      return { bind: value };
    }
    case "text":
    case "varchar": {
      if (t !== "string") throw codecError(ctx, `${dt} columns accept strings`);
      return { bind: value };
    }
    case "uuid": {
      if (typeof value !== "string" || !/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(value)) {
        throw codecError(ctx, `"${String(value)}" is not a canonical UUID string`);
      }
      return { bind: value };
    }
    case "boolean": {
      if (t !== "boolean") throw codecError(ctx, "boolean columns accept booleans");
      return { bind: value };
    }
    case "timestamp": {
      if (value instanceof Date) {
        return { bind: dateToCanonicalTimestamp(value), cast: "timestamp" };
      }
      if (typeof value === "string") {
        if (!CANONICAL_TIMESTAMP.test(value) && !TEMPORAL_INFINITY.test(value)) {
          throw codecError(
            ctx,
            `"${value}" is not a canonical timestamp string (YYYY-MM-DDTHH:MM:SS[.ffffff], no offset — timestamps carry no timezone)`,
          );
        }
        return { bind: value, cast: "timestamp" };
      }
      throw codecError(ctx, `${describeValue(value)} is not bindable for timestamp columns (string or Date)`);
    }
    case "timestamptz": {
      if (value instanceof Date) {
        return { bind: value.toISOString(), cast: "timestamptz" };
      }
      if (typeof value === "string") {
        if (!CANONICAL_TIMESTAMPTZ.test(value) && !TEMPORAL_INFINITY.test(value)) {
          throw codecError(
            ctx,
            `"${value}" is not a canonical timestamptz string (YYYY-MM-DDTHH:MM:SS[.ffffff]Z or with a ±HH:MM offset)`,
          );
        }
        return { bind: value, cast: "timestamptz" };
      }
      throw codecError(ctx, `${describeValue(value)} is not bindable for timestamptz columns (string or Date)`);
    }
    case "date": {
      if (typeof value !== "string" || !CANONICAL_DATE.test(value)) {
        throw codecError(
          ctx,
          `${describeValue(value)} is not bindable for date columns — dates take "YYYY-MM-DD" strings with no timezone interpretation`,
        );
      }
      return { bind: value, cast: "date" };
    }
    case "bytea": {
      if (!(value instanceof Uint8Array)) {
        throw codecError(ctx, "bytea columns accept Uint8Array values");
      }
      return { bind: value };
    }
    case "json":
    case "jsonb": {
      if (isJsonNull(value)) return { bind: "null", cast: sqlTypeName(dt) };
      if (!jsonRepresentable(value)) {
        throw codecError(ctx, "values for json/jsonb columns must be JSON-representable (use jsonNull for the JSON null value)");
      }
      // Encode here so a JS string arrives as a JSON string ("null" the string
      // stays "null" the string, never JSON null) on both drivers.
      return { bind: JSON.stringify(value), cast: sqlTypeName(dt) };
    }
    case "vector": {
      if (!Array.isArray(value) || !value.every((e) => typeof e === "number" && Number.isFinite(e))) {
        throw codecError(ctx, "vector columns accept arrays of finite numbers");
      }
      return { bind: value };
    }
  }
}

function dateToCanonicalTimestamp(value: Date): string {
  // A Date carries an instant only; timestamp columns store its UTC wall
  // clock, deterministically on every machine/timezone.
  return `${value.toISOString().slice(0, -1)}`;
}

function describeValue(value: unknown): string {
  if (value === null) return "null";
  if (Array.isArray(value)) return "an array";
  if (value instanceof Date) return "a Date";
  if (value instanceof Uint8Array) return "a Uint8Array";
  if (typeof value === "object") return "a plain object";
  return `a ${typeof value} value`;
}

// ---------------------------------------------------------------------------
// Codec plan (serializable per-column record — the F04 seam)
// ---------------------------------------------------------------------------

export type CodecRead =
  | "identity"
  | "bigint-mode"
  | "numeric"
  | "timestamp-text"
  | "timestamptz-text"
  | "date-text";

/** Plain-data codec description for one column. Compiled statements carry
 *  these with their projections (F04); the decode functions above consume
 *  the column record directly today. */
export interface ColumnCodec {
  readonly dataType: ColumnDataType;
  readonly read: CodecRead;
  readonly textWire: boolean;
  readonly mode?: BigintMode | TemporalMode;
}

export function codecOf(column: AnyColumnBuilder): ColumnCodec {
  const dt = column.dataType;
  switch (dt) {
    case "bigint":
      return { dataType: dt, read: "bigint-mode", textWire: false, mode: (column.readMode as BigintMode) ?? "bigint" };
    case "numeric":
      return { dataType: dt, read: "numeric", textWire: false };
    case "timestamp":
      return { dataType: dt, read: "timestamp-text", textWire: true, mode: (column.readMode as TemporalMode) ?? "string" };
    case "timestamptz":
      return { dataType: dt, read: "timestamptz-text", textWire: true, mode: (column.readMode as TemporalMode) ?? "string" };
    case "date":
      return { dataType: dt, read: "date-text", textWire: true };
    default:
      return { dataType: dt, read: "identity", textWire: false };
  }
}

/** Whether a column needs any post-driver decode on the flat path. */
export function needsFlatDecode(column: AnyColumnBuilder): boolean {
  const dt = column.dataType;
  return (
    dt === "bigint" ||
    dt === "numeric" && column.valueDecoder !== undefined ||
    dt === "timestamp" ||
    dt === "timestamptz" ||
    dt === "date"
  );
}
