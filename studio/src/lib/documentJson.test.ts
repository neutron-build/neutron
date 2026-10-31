import { describe, it, expect } from 'vitest'
import { parseDocument, serializeDocument, replaceDocument } from './documentJson'

describe('document source precision and path identity', () => {
  it.each(['9007199254740993', '-9223372036854775808', '9223372036854775807', '0.12345678901234567890123456789', '1.234567890123456789e+123'])('retains numeric lexeme %s across unrelated edits', number => {
    const raw = `{"n":${number},"s":"old","a":[${number},"${number}"]}`
    const tree = parseDocument(raw)
    expect(serializeDocument(tree)).toBe(raw)
    expect(serializeDocument(replaceDocument(tree, ['s'], 'new'))).toBe(raw.replace('old', 'new'))
  })
  it.each(['a.b', 'a[0]', '', '"', '/', '~', '雪', '__proto__', 'constructor', 'toString'])('resolves literal key %s, preserving its neighbor', key => {
    const raw = `{"${key.replace(/"/g, '\\"')}":"old","a":{"b":"neighbor"}}`
    expect(serializeDocument(replaceDocument(parseDocument(raw), [key], 'new'))).toBe(raw.replace('old', 'new'))
  })
  it.each(['null', 'true', '42', '"root"'])('supports replacing scalar root %s', raw => {
    expect(serializeDocument(replaceDocument(parseDocument(raw), [], 'new'))).toBe('"new"')
  })
  it('rejects absent/inherited paths rather than changing another property', () => {
    expect(() => replaceDocument(parseDocument('{"a":1}'), ['constructor'], 'x')).toThrow('path')
    expect(() => replaceDocument(parseDocument('[1]'), ['0'], 'x')).toThrow('path')
    expect(() => replaceDocument(parseDocument('[1]'), [2], 'x')).toThrow('path')
  })
  it.each(['', '01', '[1,]', '{"a":1,"a":2}', '{', '1e', 'NaN'])('rejects malformed source %s', raw => {
    expect(() => parseDocument(raw)).toThrow()
  })
})
