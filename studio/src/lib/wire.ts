// Tagged wire values for lossless cells over the Studio HTTP transport.
//
// The master codec contract pins this format: bigint, decimal, binary and
// temporal values cross HTTP as tagged strings so precision survives
// JSON.parse (an int8 sent as a JSON number would arrive as a rounded
// double). The backend emits tagged cells for those types (cli/internal/
// studio, landed with the typed row-identity protocol); plain values pass
// through unchanged, so connections that send untagged rows keep working.

export type WireTag = 'int8' | 'numeric' | 'date' | 'timestamp' | 'timestamptz' | 'bytea' | 'vector' | 'tsvector'

export interface TaggedCell {
  t: WireTag
  v: string
}

export class WireDecodeError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'WireDecodeError'
  }
}

const TAGS: readonly WireTag[] = ['int8', 'numeric', 'date', 'timestamp', 'timestamptz', 'bytea', 'vector', 'tsvector']

export function isTaggedCell(value: unknown): value is TaggedCell {
  if (typeof value !== 'object' || value === null) return false
  const cell = value as { t?: unknown; v?: unknown }
  return typeof cell.t === 'string' && TAGS.includes(cell.t as WireTag) && typeof cell.v === 'string'
}

/** Decode one tagged cell to its exact value. Anything else passes through. */
export function decodeCell(value: unknown): unknown {
  if (!isTaggedCell(value)) return value
  switch (value.t) {
    case 'int8': {
      if (!/^[+-]?\d+$/.test(value.v)) {
        throw new WireDecodeError(`int8 wire cell has invalid payload "${value.v}"`)
      }
      return BigInt(value.v)
    }
    case 'bytea': {
      if (!/^[0-9a-f]*$/i.test(value.v) || value.v.length % 2 !== 0) {
        throw new WireDecodeError(`bytea wire cell has invalid hex payload "${value.v}"`)
      }
      const out = new Uint8Array(value.v.length / 2)
      for (let i = 0; i < out.length; i++) {
        out[i] = Number.parseInt(value.v.slice(i * 2, i * 2 + 2), 16)
      }
      return out
    }
    case 'numeric':
    case 'date':
    case 'timestamp':
    case 'timestamptz':
      // Exact strings per the codec table — decimals keep their scale,
      // temporals keep microseconds in their canonical form.
      return value.v
  }
}

/** Decode every cell of a query-result row matrix in place. */
export function decodeRows(rows: unknown[][]): unknown[][] {
  for (const row of rows) {
    for (let i = 0; i < row.length; i++) {
      row[i] = decodeCell(row[i])
    }
  }
  return rows
}

function toHex(bytes: Uint8Array): string {
  let out = ''
  for (const b of bytes) out += b.toString(16).padStart(2, '0')
  return out
}

/**
 * Encode one value back to its wire form for mutation requests. The decoded
 * lossless types re-tag exactly from the value (bigint -> int8 cell,
 * Uint8Array -> bytea cell). The server's strict per-column decoder refuses
 * bare text for tagged columns, so a string value WITH a column tag re-tags
 * as {t, v} — the server validates the payload against the catalog type
 * (parse, hex shape, canonical temporal form). Without a tag, strings pass
 * through untouched.
 */
export function encodeCell(value: unknown, tag?: WireTag | null): unknown {
  if (value === null || value === undefined) return value ?? null
  if (typeof value === 'bigint') return { t: 'int8', v: value.toString() }
  if (value instanceof Uint8Array) return { t: 'bytea', v: toHex(value) }
  if (tag && typeof value === 'string') {
    // Display form of bytea is \x-prefixed hex; the wire payload is bare hex.
    const v = tag === 'bytea' && /^\\x/i.test(value) ? value.slice(2) : value
    return { t: tag, v }
  }
  if (tag === 'int8' && typeof value === 'number' && Number.isInteger(value)) {
    return { t: 'int8', v: String(value) }
  }
  return value
}

/**
 * Render a decoded cell as editable/display text. Exact for every decoded
 * type: bigint keeps all digits, bytea shows \x-prefixed hex (which
 * encodeCell accepts back), objects (json/jsonb) show JSON text.
 */
export function formatCell(value: unknown): string {
  if (typeof value === 'bigint') return value.toString()
  if (value instanceof Uint8Array) return '\\x' + toHex(value)
  if (typeof value === 'object' && value !== null) {
    try {
      return JSON.stringify(value, (_k, v) => typeof v === 'bigint' ? v.toString() : v)
    } catch {
      return String(value)
    }
  }
  return String(value)
}

/**
 * Encode a staged cell edit to its wire form for one column (S03). The
 * three-way discipline is preserved exactly: 'null' is SQL NULL, 'default'
 * omits the column (insert DEFAULT), and 'value' encodes per the column's
 * authoritative type — tagged columns re-tag (bigint/decimal digits never
 * cross through Number), booleans cross as booleans, JSON crosses as
 * validated JSON text. Throws WireEncodeError for text that cannot be the
 * column's value; the server re-validates strictly regardless.
 */
export class WireEncodeError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'WireEncodeError'
  }
}

export interface EditableColumnShape {
  name: string
  type: string
  tag: WireTag | null
}

export type EncodedEdit =
  | { kind: 'value'; value: unknown }
  | { kind: 'null' }
  | { kind: 'omit' }

export function encodeEdit(edit: { kind: 'value'; text: string } | { kind: 'null' } | { kind: 'default' }, col: EditableColumnShape): EncodedEdit {
  if (edit.kind === 'null') return { kind: 'null' }
  if (edit.kind === 'default') return { kind: 'omit' }
  const text = edit.text
  if (col.type === 'boolean' || col.type === 'bool') {
    if (text === 'true') return { kind: 'value', value: true }
    if (text === 'false') return { kind: 'value', value: false }
    throw new WireEncodeError(`column ${col.name}: boolean value must be true or false`)
  }
  if (/^json(b)?$/.test(col.type)) {
    try {
      JSON.parse(text)
    } catch (err) {
      throw new WireEncodeError(`column ${col.name}: invalid JSON text (${err instanceof Error ? err.message : String(err)})`)
    }
    return { kind: 'value', value: text }
  }
  if (col.tag) {
    return { kind: 'value', value: encodeCell(text, col.tag) }
  }
  return { kind: 'value', value: text }
}
