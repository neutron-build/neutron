import { describe, it, expect } from 'vitest'
import { JsonRecordSplitter, parseJsonRecord, parseJsonRecords, JsonImportError } from './losslessJson'

// The importer's lossless JSON reader (S06). JSON.parse routes every number
// through an IEEE double (9007199254740993 becomes 9007199254740992); these
// tests pin that the importer never does: numbers keep their literal digits
// and nested values their exact source slice, whatever their size.

describe('parseJsonRecord keeps values lossless', () => {
  it('numbers keep their exact literal text beyond 2^53 and 40-digit decimals', () => {
    const members = parseJsonRecord('{"big": 9007199254740993, "neg": -9223372036854775808, "dec": 1234567890123456789012345678901234567890.0123456789, "e": 1.5e-9}')
    expect(members.get('big')).toEqual({ kind: 'number', raw: '9007199254740993' })
    expect(members.get('neg')).toEqual({ kind: 'number', raw: '-9223372036854775808' })
    expect(members.get('dec')).toEqual({ kind: 'number', raw: '1234567890123456789012345678901234567890.0123456789' })
    expect(members.get('e')).toEqual({ kind: 'number', raw: '1.5e-9' })
    // the hazard this module exists for: the double-rounded number differs
    expect(Number(members.get('big')!.raw)).toBe(2 ** 53)
    expect(BigInt(members.get('big')!.raw)).toBe(9007199254740993n)
  })

  it('strings decode exactly (escapes, unicode), booleans and null typed', () => {
    const members = parseJsonRecord('{"s": "a\\"b\\\\c\\nd\\u00e9", "t": true, "f": false, "n": null}')
    expect(members.get('s')).toEqual({ kind: 'string', value: 'a"b\\c\ndé' })
    expect(members.get('t')).toEqual({ kind: 'boolean', value: true })
    expect(members.get('f')).toEqual({ kind: 'boolean', value: false })
    expect(members.get('n')).toEqual({ kind: 'null' })
  })

  it('nested objects and arrays keep their exact source slice', () => {
    const members = parseJsonRecord('{"doc": {  "a" : [1, 2.50, 90071992547409931] }, "arr": [true, null, "x"]}')
    expect(members.get('doc')).toEqual({ kind: 'object', raw: '{  "a" : [1, 2.50, 90071992547409931] }' })
    expect(members.get('arr')).toEqual({ kind: 'array', raw: '[true, null, "x"]' })
  })

  it('an empty object and an empty array member parse', () => {
    const members = parseJsonRecord('{"o": {}, "a": []}')
    expect(members.get('o')).toEqual({ kind: 'object', raw: '{}' })
    expect(members.get('a')).toEqual({ kind: 'array', raw: '[]' })
  })

  it('duplicate member names are an error, never a guess', () => {
    expect(() => parseJsonRecord('{"a": 1, "a": 2}')).toThrow(/duplicate member "a"/)
  })

  it('refuses non-object records, trailing data and malformed values', () => {
    expect(() => parseJsonRecord('[1,2]')).toThrow(/must be a JSON object/)
    expect(() => parseJsonRecord('{} trailing')).toThrow(/unexpected data after the record/)
    expect(() => parseJsonRecord('{"a": 1} {"b": 2}')).toThrow(/unexpected data after the record/)
    expect(() => parseJsonRecord('{"a": tru}')).toThrow()
    expect(() => parseJsonRecord('{"a": 01}')).toThrow() // leading zero is not JSON
    expect(() => parseJsonRecord("{'a': 1}")).toThrow() // single quotes are not JSON
    expect(() => parseJsonRecord('{"a": "unterminated}')).toThrow(/unterminated string/)
    expect(() => parseJsonRecord('{"a" 1}')).toThrow(/expected ":"/)
    expect(() => parseJsonRecord('{a: 1}')).toThrow(/expected a member name/)
  })

  it('a raw control character inside a string is refused (escapes are the way)', () => {
    expect(() => parseJsonRecord('{"a": "x\ty"}')).toThrow(/control character/)
    expect(() => parseJsonRecord('{"a": "line\nbreak"}')).toThrow(/control character/)
    expect(() => parseJsonRecord('{"a": "tab\\tescaped"}')).not.toThrow()
  })
})

describe('JsonRecordSplitter: arrays, streams and boundaries', () => {
  it('detects a top-level array and yields each element record', () => {
    const out = parseJsonRecords('[\n {"a": 1},\n {"a": 2.5}\n]')
    expect(out).toHaveLength(2)
    expect(out[0].members.get('a')).toEqual({ kind: 'number', raw: '1' })
    expect(out[1].line).toBe(3)
  })

  it('detects newline-delimited / concatenated objects', () => {
    const out = parseJsonRecords('{"a": 1}\n{"a": 2}\n{"a": 3}')
    expect(out.map(r => r.members.get('a'))).toEqual([
      { kind: 'number', raw: '1' }, { kind: 'number', raw: '2' }, { kind: 'number', raw: '3' }])
  })

  it('records containing braces and brackets inside strings do not confuse the splitter', () => {
    const out = parseJsonRecords('{"s": "a}b]c{\\"d: 1"}\n{"x": [1, {"y": "}"}]}')
    expect(out).toHaveLength(2)
    expect(out[0].members.get('s')).toEqual({ kind: 'string', value: 'a}b]c{"d: 1' })
    expect(out[1].members.get('x')!.kind).toBe('array')
  })

  it('an empty array is zero records; whitespace-only input is zero records', () => {
    expect(parseJsonRecords('[]')).toEqual([])
    expect(parseJsonRecords('  \n\t ')).toEqual([])
  })

  it('a BOM at the start is skipped', () => {
    expect(parseJsonRecords('\ufeff{"a": 1}')).toHaveLength(1)
  })

  it('refuses: trailing comma in array, missing ], data after ], scalars at top level', () => {
    expect(() => parseJsonRecords('[{"a": 1},]')).toThrow(/trailing comma/)
    expect(() => parseJsonRecords('[{"a": 1}')).toThrow(/unterminated array/)
    expect(() => parseJsonRecords('[{"a": 1}] {"b": 2}')).toThrow(/after the closing/)
    expect(() => parseJsonRecords('1\n2')).toThrow()
    expect(() => parseJsonRecords('{"a": 1}\n"scalar"')).toThrow(/must be a JSON object/)
    expect(() => parseJsonRecords('{"a": 1}\n[')).toThrow()
  })

  it('is incremental: records straddle arbitrary chunk boundaries identically', () => {
    const input = '[{"a": 9007199254740993, "s": "brace } inside"}, {"b": {"c": [1, 2]}}, {"d": null}]\n'
    let all: string[] | null = null
    for (const size of [1, 2, 3, 4, 7, 13]) {
      const p = new JsonRecordSplitter()
      const texts: string[] = []
      for (let i = 0; i < input.length; i += size) texts.push(...p.push(input.slice(i, i + size)).map(r => r.text))
      texts.push(...p.end().map(r => r.text))
      if (all === null) all = texts
      expect(texts).toEqual(all)
      expect(() => parseJsonRecord(texts[0])).not.toThrow()
    }
  })

  it('a record above the size cap is refused', () => {
    const p = new JsonRecordSplitter({ maxRecordChars: 10 })
    expect(() => p.push('{"aaaaaaaaaaaaaaaaaa": 1}')).toThrow(/exceeds 10 characters/)
  })

  it('errors carry the line the record started on', () => {
    try {
      parseJsonRecords('{\n"a": 1\n}\n{\n"b": tru\n}')
      expect.unreachable()
    } catch (err) {
      expect(err).toBeInstanceOf(JsonImportError)
      expect((err as JsonImportError).line).toBe(4)
    }
  })
})
