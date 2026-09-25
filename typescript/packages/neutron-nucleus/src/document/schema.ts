// ---------------------------------------------------------------------------
// @neutron-build/nucleus/document/schema — schema-aware document validation
// (X02)
//
// The engine's document store accepts ANY JSON (DOC_INSERT parses and
// stores; there is no server-side schema surface). Validation is therefore
// CLIENT-SIDE and honest about it: a validated collection rejects invalid
// documents BEFORE any statement is sent (statement counter provably zero),
// with precise errors naming the path, the expectation and the offending
// value. It does not pretend the engine enforces anything.
//
// One engine boundary drives a real rule: the document store coerces JSON
// numbers to f64 at parse time (nucleus/docs/MODEL_SEMANTICS.md). An
// "integer" field therefore rejects integers beyond Number.MAX_SAFE_INTEGER
// — the engine would silently round them, so validation fails closed
// instead.
// ---------------------------------------------------------------------------

/** Field types a document schema can express. */
export type DocumentFieldType = 'string' | 'number' | 'integer' | 'boolean' | 'array' | 'object';

/** One field's contract. */
export interface DocumentField {
  type: DocumentFieldType;
  /** Field may be absent (default: required — absence is an error). */
  optional?: boolean;
  /** Field may be JSON null (default: null is an error). */
  nullable?: boolean;
  /** Element contract for `type: 'array'`. Unconstrained when omitted. */
  items?: DocumentField;
  /** Member contracts for `type: 'object'`. Unconstrained when omitted. */
  fields?: Record<string, DocumentField>;
  /** Whether keys not named in `fields` are allowed at THIS level (default: true). */
  additionalProperties?: boolean;
}

/** A document schema: top-level field contracts plus an openness rule. */
export interface DocumentSchema {
  fields: Record<string, DocumentField>;
  /**
   * Whether keys not named in `fields` are allowed (default: true —
   * documents stay extensible; validation constrains the known shape).
   * `false` rejects any undeclared key at any validated level that declares
   * `fields`.
   */
  additionalProperties?: boolean;
}

/** One validation failure, with the exact location and expectation. */
export interface DocumentValidationIssue {
  /** JSON path of the offending value, e.g. `$.tags[2].name`. */
  path: string;
  /** What the schema requires at that path. */
  expected: string;
  /** What the document actually contains (truncated value description). */
  got: string;
}

const MAX_SAFE = Number.MAX_SAFE_INTEGER;
const MIN_SAFE = Number.MIN_SAFE_INTEGER;

/** A JSON object value: a plain object (not an array, Date, Map, class instance...). */
function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

function describe(value: unknown): string {
  if (value === null) return 'null';
  if (Array.isArray(value)) return `array(${value.length})`;
  const t = typeof value;
  if (t === 'object') {
    if (!isPlainObject(value)) return `${(value as object).constructor?.name ?? 'object'} instance`;
    return `object(${Object.keys(value).length} keys)`;
  }
  if (t === 'bigint') return `bigint ${String(value)}n`;
  if (t === 'function' || t === 'symbol') return t;
  if (t === 'string') {
    const s = value as string;
    return JSON.stringify(s.length > 24 ? `${s.slice(0, 24)}…` : s);
  }
  return String(value);
}

function typeExpectation(field: DocumentField): string {
  switch (field.type) {
    case 'array':
      return field.items ? `array of ${typeExpectation(field.items)}` : 'array';
    case 'object':
      return field.fields ? 'object with declared fields' : 'object';
    default:
      return field.type;
  }
}

function validateValue(
  value: unknown,
  field: DocumentField,
  path: string,
  issues: DocumentValidationIssue[],
): void {
  if (value === null) {
    if (!field.nullable) {
      issues.push({ path, expected: `${typeExpectation(field)} (not nullable)`, got: 'null' });
    }
    return;
  }

  switch (field.type) {
    case 'string':
      if (typeof value !== 'string') {
        issues.push({ path, expected: 'string', got: describe(value) });
      }
      return;
    case 'boolean':
      if (typeof value !== 'boolean') {
        issues.push({ path, expected: 'boolean', got: describe(value) });
      }
      return;
    case 'number':
      if (typeof value !== 'number' || !Number.isFinite(value)) {
        issues.push({ path, expected: 'finite number', got: describe(value) });
      }
      return;
    case 'integer':
      if (typeof value !== 'number' || !Number.isInteger(value)) {
        issues.push({ path, expected: 'integer', got: describe(value) });
      } else if (value > MAX_SAFE || value < MIN_SAFE) {
        // The engine stores JSON numbers as f64; a beyond-safe integer would
        // round silently on the way in. Fail closed with the boundary named.
        issues.push({
          path,
          expected: `integer within [${MIN_SAFE}, ${MAX_SAFE}] (the document store coerces numbers to double precision and would round it)`,
          got: describe(value),
        });
      }
      return;
    case 'array': {
      if (!Array.isArray(value)) {
        issues.push({ path, expected: typeExpectation(field), got: describe(value) });
        return;
      }
      if (field.items) {
        for (let i = 0; i < value.length; i++) {
          validateValue(value[i], field.items, `${path}[${i}]`, issues);
        }
      }
      return;
    }
    case 'object': {
      if (!isPlainObject(value)) {
        issues.push({ path, expected: `${typeExpectation(field)} (plain JSON object)`, got: describe(value) });
        return;
      }
      validateFields(value, field, path, issues);
      return;
    }
  }
}

function validateFields(
  value: Record<string, unknown>,
  parent: { fields?: Record<string, DocumentField>; additionalProperties?: boolean },
  path: string,
  issues: DocumentValidationIssue[],
): void {
  const declared = parent.fields;
  if (!declared) return;
  const additional = parent.additionalProperties !== false;
  for (const [key, field] of Object.entries(declared)) {
    const at = path === '$' ? `$.${key}` : `${path}.${key}`;
    // `undefined` is absence: JSON.stringify drops the key, so the stored
    // document will not have it.
    if (!Object.prototype.hasOwnProperty.call(value, key) || value[key] === undefined) {
      if (!field.optional) {
        issues.push({ path: at, expected: `${typeExpectation(field)} (required field)`, got: 'absent' });
      }
      continue;
    }
    validateValue(value[key], field, at, issues);
  }
  if (!additional) {
    for (const key of Object.keys(value)) {
      if (value[key] === undefined) continue; // dropped by serialization
      if (!Object.prototype.hasOwnProperty.call(declared, key)) {
        const at = path === '$' ? `$.${key}` : `${path}.${key}`;
        issues.push({ path: at, expected: 'undeclared (additionalProperties is false)', got: describe(value[key]) });
      }
    }
  }
}

/**
 * Validate a document against a schema. Pure: returns every issue instead of
 * throwing, so callers decide the error shape; the collection API throws a
 * `DocumentValidationError` carrying all issues.
 */
export function validateDocument(schema: DocumentSchema, doc: unknown): DocumentValidationIssue[] {
  if (!isPlainObject(doc)) {
    return [{ path: '$', expected: 'object (plain JSON object)', got: describe(doc) }];
  }
  const issues: DocumentValidationIssue[] = [];
  validateFields(doc, schema, '$', issues);
  return issues;
}
