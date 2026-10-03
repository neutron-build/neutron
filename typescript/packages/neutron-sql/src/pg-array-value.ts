import { formatArrayLiteral } from "./pg-array.js";

export interface PgArrayDimension { readonly length: number; readonly lowerBound: number }
export const MAX_PG_ARRAY_ELEMENTS = 1_000_000;
const JSON_NULL = Symbol.for("@neutron-build/sql.jsonNull");

function snapshot<T>(value: T, depth = 0, ancestry = new Set<object>()): T {
  if (depth > 64) throw new Error("array JSON element depth budget exceeded");
  if (value === null || typeof value !== "object") {
    if (value === undefined || typeof value === "function" || typeof value === "symbol") throw new Error("array element requires a native scalar");
    return value;
  }
  if ((value as Record<symbol, unknown>)[JSON_NULL] === true) return Object.freeze({ [JSON_NULL]: true }) as T;
  if (value instanceof Uint8Array) return new Uint8Array(value) as T;
  if (value instanceof Date) return new Date(value.getTime()) as T;
  if (ancestry.has(value)) throw new Error("array JSON elements cannot contain cycles");
  ancestry.add(value);
  try {
  if (Array.isArray(value)) return Object.freeze(Array.from({ length: value.length }, (_, index) => {
    if (!Object.hasOwn(value, index)) throw new Error("array elements cannot be sparse");
    const descriptor = Object.getOwnPropertyDescriptor(value, String(index))!;
    if (!Object.hasOwn(descriptor, "value")) throw new Error("array JSON elements cannot contain accessors");
    return snapshot(descriptor.value, depth + 1, ancestry);
  })) as T;
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) throw new Error("array element requires a supported immutable scalar or JSON value");
  const result: Record<string, unknown> = {};
  for (const [key, descriptor] of Object.entries(Object.getOwnPropertyDescriptors(value))) {
    if (!Object.hasOwn(descriptor, "value")) throw new Error("array JSON elements cannot contain accessors");
    Object.defineProperty(result, key, { value: snapshot(descriptor.value, depth + 1, ancestry), enumerable: descriptor.enumerable });
  }
  return Object.freeze(result) as T;
  } finally { ancestry.delete(value); }
}

/** Flat immutable elements plus explicit native dimensions. SQL NULL is null;
 * an empty non-NULL array has no dimensions and no elements. */
export class PgArray<T> {
  readonly dimensions: readonly PgArrayDimension[];
  readonly #elements: readonly (T | null)[];
  constructor(dimensions: readonly PgArrayDimension[], elements: readonly (T | null)[]) {
    if (dimensions.length > 6) throw new Error("PostgreSQL arrays support at most six dimensions");
    let count = dimensions.length ? 1 : 0;
    this.dimensions = Object.freeze(Array.from({ length: dimensions.length }, (_, index) => {
      if (!Object.hasOwn(dimensions, index)) throw new Error("array dimensions cannot be sparse");
      const dimension = dimensions[index];
      const { length, lowerBound } = dimension;
      if (!Number.isInteger(length) || length <= 0 || length > 2147483647 || !Number.isInteger(lowerBound) || lowerBound < -2147483648 || lowerBound > 2147483647 || lowerBound + length - 1 > 2147483647) throw new Error("invalid PostgreSQL array dimension");
      count *= length;
      if (count > MAX_PG_ARRAY_ELEMENTS) throw new Error("array element budget exceeded");
      return Object.freeze({ length, lowerBound });
    }));
    if (count !== elements.length) throw new Error("array dimensions/cardinality mismatch");
    this.#elements = Object.freeze(Array.from({ length: elements.length }, (_, index) => {
      if (!Object.hasOwn(elements, index)) throw new Error("array elements cannot be sparse");
      const descriptor = Object.getOwnPropertyDescriptor(elements, String(index))!;
      if (!Object.hasOwn(descriptor, "value")) throw new Error("array elements cannot contain accessors");
      return snapshot(descriptor.value);
    }));
    Object.freeze(this);
  }
  get elements(): readonly (T | null)[] { return Object.freeze(this.#elements.map(value => snapshot(value))); }
}

/** Parse native array text independently of either driver's array parser. */
export function parsePgArray(text: string): PgArray<string> {
  if (text.length > 64 * 1024 * 1024) throw new Error("array text budget exceeded");
  let offset = 0;
  const declared: PgArrayDimension[] = [];
  while (text[offset] === "[") {
    const match = /^\[(-?\d+):(-?\d+)\]/.exec(text.slice(offset));
    if (!match || declared.length === 6) throw new Error("invalid array bounds");
    const lowerBound = Number(match[1]), upperBound = Number(match[2]);
    declared.push({ lowerBound, length: upperBound - lowerBound + 1 });
    offset += match[0].length;
  }
  if (declared.length && text[offset++] !== "=") throw new Error("array bounds require equals");
  const elements: Array<string | null> = [];
  const scalar = (): string | null => {
    const quoted = text[offset] === '"';
    let value = "", escaped = false;
    if (quoted) offset++;
    while (offset < text.length) {
      const char = text[offset];
      if (char === "\\") {
        if (++offset >= text.length) throw new Error("truncated array escape");
        value += text[offset++]; escaped = true; continue;
      }
      if (quoted && char === '"') { offset++; return value; }
      if (!quoted && (char === "," || char === "}")) {
        if (!value.length) throw new Error("empty unquoted array element");
        return !escaped && value.toUpperCase() === "NULL" ? null : value;
      }
      if (!quoted && (char === '"' || char === "{")) throw new Error("invalid unquoted array element");
      value += char; offset++;
    }
    throw new Error("unterminated array element");
  };
  const array = (depth: number): number[] => {
    if (depth > 6 || text[offset++] !== "{") throw new Error("invalid array nesting");
    if (text[offset] === "}") { offset++; return [0]; }
    let count = 0, shape: number[] | undefined, nested: boolean | undefined;
    for (;;) {
      const child = text[offset] === "{";
      if (nested !== undefined && nested !== child) throw new Error("mixed scalar/nested array shape");
      nested = child;
      if (child) {
        const next = array(depth + 1);
        if (shape && (shape.length !== next.length || shape.some((value, index) => value !== next[index]))) throw new Error("ragged array shape");
        shape = next;
      } else {
        if (elements.length >= MAX_PG_ARRAY_ELEMENTS) throw new Error("array element budget exceeded");
        elements.push(scalar());
      }
      count++;
      if (text[offset] === "}") { offset++; return [count, ...(shape ?? [])]; }
      if (text[offset++] !== ",") throw new Error("invalid array separator");
    }
  };
  const shape = array(1);
  if (offset !== text.length) throw new Error("trailing array text");
  if (shape.length === 1 && shape[0] === 0 && !declared.length) return new PgArray([], []);
  if (declared.length && (declared.length !== shape.length || declared.some((dimension, index) => dimension.length !== shape[index]))) throw new Error("declared array bounds/shape mismatch");
  return new PgArray(declared.length ? declared : shape.map(length => ({ length, lowerBound: 1 })), elements);
}

export function formatPgArray(value: PgArray<string>): string {
  if (!(value instanceof PgArray)) throw new Error("dimensioned array requires PgArray");
  if (!value.dimensions.length) return "{}";
  const elements = value.elements;
  let offset = 0;
  const level = (depth: number): string => {
    const length = value.dimensions[depth].length;
    if (depth === value.dimensions.length - 1) {
      const result = formatArrayLiteral(elements.slice(offset, offset + length)); offset += length; return result;
    }
    return "{" + Array.from({ length }, () => level(depth + 1)).join(",") + "}";
  };
  return value.dimensions.map(dimension => `[${dimension.lowerBound}:${dimension.lowerBound + dimension.length - 1}]`).join("") + "=" + level(0);
}
