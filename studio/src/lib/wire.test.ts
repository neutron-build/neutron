import { describe, it, expect } from 'vitest'
import { decodeCell, decodeRows, encodeCell, formatCell, isTaggedCell, WireDecodeError } from './wire'

// Fixtures pin the tagged wire format (the backend side lands with the typed
// row-identity protocol; these tests are the format contract both ends meet):
// { t: <tag>, v: <exact string> } cells decode to exact values, everything
// else passes through untouched.

describe('wire', () => {
  it('isTaggedCell accepts only well-formed tagged cells', () => {
    expect(isTaggedCell({ t: 'int8', v: '9007199254740993' })).toBe(true)
    expect(isTaggedCell({ t: 'bytea', v: '00ff10' })).toBe(true)
    expect(isTaggedCell({ t: 'bogus', v: 'x' })).toBe(false)
    expect(isTaggedCell({ t: 'int8' })).toBe(false)
    expect(isTaggedCell({ v: '1' })).toBe(false)
    expect(isTaggedCell('int8')).toBe(false)
    expect(isTaggedCell(null)).toBe(false)
  })

  it('decodes int8 beyond the safe range to bigint — never a rounded double', () => {
    const decoded = decodeCell({ t: 'int8', v: '9007199254740993' })
    expect(decoded).toBe(9007199254740993n)
    expect(typeof decoded).toBe('bigint')
    expect(decodeCell({ t: 'int8', v: '-9223372036854775808' })).toBe(-9223372036854775808n)
  })

  it('decodes numeric/date/timestamp/timestamptz to exact strings', () => {
    expect(decodeCell({ t: 'numeric', v: '99999999999999999999.99' })).toBe('99999999999999999999.99')
    expect(decodeCell({ t: 'numeric', v: '1.50' })).toBe('1.50')
    expect(decodeCell({ t: 'date', v: '2026-01-02' })).toBe('2026-01-02')
    expect(decodeCell({ t: 'timestamp', v: '2026-01-01T19:04:05.678123' })).toBe('2026-01-01T19:04:05.678123')
    expect(decodeCell({ t: 'timestamptz', v: '2026-03-08T07:30:00.123456Z' })).toBe('2026-03-08T07:30:00.123456Z')
  })

  it('decodes bytea hex payloads to Uint8Array', () => {
    const decoded = decodeCell({ t: 'bytea', v: '00ff10' }) as Uint8Array
    expect(decoded).toBeInstanceOf(Uint8Array)
    expect(Array.from(decoded)).toEqual([0x00, 0xff, 0x10])
    expect(decodeCell({ t: 'bytea', v: '' })).toEqual(new Uint8Array(0))
  })

  it('passes plain JSON values through untouched', () => {
    for (const plain of [null, 1.5, 'text', true, { a: 1 }, [1, 2]]) {
      expect(decodeCell(plain)).toBe(plain)
    }
  })

  it('rejects malformed payloads with clear errors', () => {
    expect(() => decodeCell({ t: 'int8', v: '1.5' })).toThrow(WireDecodeError)
    expect(() => decodeCell({ t: 'int8', v: '1.5' })).toThrow(/int8 wire cell has invalid payload/)
    expect(() => decodeCell({ t: 'bytea', v: '0ff' })).toThrow(/bytea wire cell has invalid hex/)
    expect(() => decodeCell({ t: 'bytea', v: 'zz' })).toThrow(WireDecodeError)
  })

  it('decodes full result row matrices in place', () => {
    const rows: unknown[][] = [
      [{ t: 'int8', v: '9007199254740993' }, 'plain', null],
      [{ t: 'numeric', v: '1.50' }, { t: 'bytea', v: '00ff10' }, 7],
    ]
    const decoded = decodeRows(rows)
    expect(decoded[0][0]).toBe(9007199254740993n)
    expect(decoded[0][1]).toBe('plain')
    expect(decoded[0][2]).toBeNull()
    expect(decoded[1][0]).toBe('1.50')
    expect(Array.from(decoded[1][1] as Uint8Array)).toEqual([0x00, 0xff, 0x10])
    expect(decoded[1][2]).toBe(7)
  })

  // Round-trip: what decodeCell produces, encodeCell sends back exactly —
  // this is what row identities use (keys decoded from reads re-tag on
  // mutation requests).
  it('encodes bigint back to an int8 cell', () => {
    expect(encodeCell(9007199254740993n)).toEqual({ t: 'int8', v: '9007199254740993' })
    expect(encodeCell(-9223372036854775808n)).toEqual({ t: 'int8', v: '-9223372036854775808' })
  })

  it('encodes Uint8Array back to a bytea hex cell', () => {
    expect(encodeCell(new Uint8Array([0x00, 0xff, 0x10]))).toEqual({ t: 'bytea', v: '00ff10' })
    expect(encodeCell(new Uint8Array(0))).toEqual({ t: 'bytea', v: '' })
  })

  it('encodes plain values untouched (temporal/numeric strings pass as text)', () => {
    for (const v of ['2026-01-02', '123.4560', 'x', true]) {
      expect(encodeCell(v)).toBe(v)
    }
    expect(encodeCell(1.5)).toBe(1.5)
  })

  it('encodes an integral number as a tagged int8 when the column tag says int8', () => {
    expect(encodeCell(7, 'int8')).toEqual({ t: 'int8', v: '7' })
    // A mistyped value is not silently re-tagged.
    expect(encodeCell(1.5, 'int8')).toBe(1.5)
  })

  it('re-tags textual values for tagged columns (the strict server requires cells)', () => {
    // The server's decodeColumnValue refuses bare text for tagged columns:
    // numeric/temporal/date strings and int8 text must cross as cells.
    expect(encodeCell('123.4560', 'numeric')).toEqual({ t: 'numeric', v: '123.4560' })
    expect(encodeCell('2026-01-02', 'date')).toEqual({ t: 'date', v: '2026-01-02' })
    expect(encodeCell('2026-01-02T03:04:05Z', 'timestamptz')).toEqual({ t: 'timestamptz', v: '2026-01-02T03:04:05Z' })
    expect(encodeCell('2026-01-02T03:04:05', 'timestamp')).toEqual({ t: 'timestamp', v: '2026-01-02T03:04:05' })
    expect(encodeCell('9007199254740993', 'int8')).toEqual({ t: 'int8', v: '9007199254740993' })
    expect(encodeCell('00ff10', 'bytea')).toEqual({ t: 'bytea', v: '00ff10' })
    // Without a column tag, text passes through untouched.
    expect(encodeCell('123.4560')).toBe('123.4560')
  })

  it('null stays null; undefined normalizes to null', () => {
    expect(encodeCell(null)).toBe(null)
    expect(encodeCell(undefined)).toBe(null)
  })

  it('decode->encode round-trips identity cells exactly', () => {
    // Binary types re-tag from the decoded value; textual canonical forms
    // (numeric/temporal) re-tag through the column tag from table metadata.
    expect(encodeCell(decodeCell({ t: 'int8', v: '9007199254740993' }))).toEqual({ t: 'int8', v: '9007199254740993' })
    expect(encodeCell(decodeCell({ t: 'bytea', v: '00ff10' }))).toEqual({ t: 'bytea', v: '00ff10' })
    expect(encodeCell(decodeCell({ t: 'numeric', v: '1.50' }), 'numeric')).toEqual({ t: 'numeric', v: '1.50' })
    expect(encodeCell(decodeCell({ t: 'date', v: '2026-01-02' }), 'date')).toEqual({ t: 'date', v: '2026-01-02' })
    expect(encodeCell(decodeCell({ t: 'timestamptz', v: '2026-03-08T07:30:00.123456Z' }), 'timestamptz')).toEqual({ t: 'timestamptz', v: '2026-03-08T07:30:00.123456Z' })
  })
})

describe('wire display and bytea editing', () => {
  it('formatCell is exact and encodeCell accepts the bytea display form back', () => {
    expect(formatCell(9007199254740993n)).toBe('9007199254740993')
    expect(formatCell(new Uint8Array([0, 255]))).toBe('\\x00ff')
    expect(formatCell({ n: 1n })).toBe('{"n":"1"}')
    expect(encodeCell('\\x00ff', 'bytea')).toEqual({ t: 'bytea', v: '00ff' })
    expect(encodeCell('00ff', 'bytea')).toEqual({ t: 'bytea', v: '00ff' })
  })
})
