// Tagged wire values for lossless cells over the Studio HTTP transport.
//
// The master codec contract pins this format: bigint, decimal, binary and
// temporal values cross HTTP as tagged strings so precision survives
// JSON.parse (an int8 sent as a JSON number would arrive as a rounded
// double). The backend emits tagged cells for those types (cli/internal/
// studio, landed with the typed row-identity protocol); plain values pass
// through unchanged, so connections that send untagged rows keep working.

export type WireTag = 'int8' | 'numeric' | 'date' | 'timestamp' | 'timestamptz' | 'bytea'

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

const TAGS: readonly WireTag[] = ['int8', 'numeric', 'date', 'timestamp', 'timestamptz', 'bytea']

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
