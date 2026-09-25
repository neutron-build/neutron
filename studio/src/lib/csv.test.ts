import { describe, it, expect } from 'vitest'
import { CsvParser, parseCsv, CsvParseError } from './csv'

// The importer's CSV reader (S06). These cases pin the exactness the import
// round-trip depends on: quoted vs unquoted fields (SQL NULL vs empty
// string), embedded delimiters/quotes/newlines, BOM, CR/CRLF, chunk
// boundaries and refusals. The Go e2e leg pins the SERVER's export half of
// the dialect; these tests pin that the client reads it back losslessly.

function texts(records: ReturnType<typeof parseCsv>): string[][] {
  return records.map(r => r.fields.map(f => f.text))
}
function quoted(records: ReturnType<typeof parseCsv>): (boolean | undefined)[][] {
  return records.map(r => r.fields.map(f => f.quoted))
}

describe('CsvParser basics', () => {
  it('parses a header and simple rows', () => {
    const recs = parseCsv('id,name\n1,Alice\n2,Bob\n')
    expect(texts(recs)).toEqual([['id', 'name'], ['1', 'Alice'], ['2', 'Bob']])
    expect(recs[1].line).toBe(2)
  })

  it('parses without a trailing newline', () => {
    expect(texts(parseCsv('a,b\n1,2'))).toEqual([['a', 'b'], ['1', '2']])
  })

  it('keeps the quoted flag: "" is a quoted empty field, distinct from unquoted empty', () => {
    const recs = parseCsv('a,b,c\n,,x\n"",,"y"\n')
    expect(recs[1].fields.map(f => f.quoted)).toEqual([false, false, false])
    expect(recs[2].fields.map(f => f.quoted)).toEqual([true, false, true])
    expect(texts(recs)[1]).toEqual(['', '', 'x'])
    expect(texts(recs)[2]).toEqual(['', '', 'y'])
  })

  it('doubles quotes inside a quoted field are one literal quote', () => {
    const recs = parseCsv('a\n"he said ""hi"""\n')
    expect(recs[1].fields[0]).toEqual({ text: 'he said "hi"', quoted: true })
  })

  it('a quote inside an unquoted field is a literal character', () => {
    expect(texts(parseCsv('a\n5"9\n'))).toEqual([['a'], ['5"9']])
    expect(parseCsv('a\n5"9\n')[1].fields[0].quoted).toBe(false)
  })

  it('delimiters and line breaks inside quotes are data', () => {
    const recs = parseCsv('id,note\n1,"a,b"\n2,"line1\nline2"\n3,"cr\rhere"\n')
    expect(texts(recs)).toEqual([
      ['id', 'note'],
      ['1', 'a,b'],
      ['2', 'line1\nline2'],
      ['3', 'cr\rhere'],
    ])
  })

  it('CRLF and lone CR end records; a quoted CRLF stays data and counts one line', () => {
    const recs = parseCsv('a,b\r\n1,2\r3,4\r\n')
    expect(texts(recs)).toEqual([['a', 'b'], ['1', '2'], ['3', '4']])
    const embedded = parseCsv('"r1\r\nr2",x\n')
    expect(embedded[0].fields[0].text).toBe('r1\r\nr2')
    expect(parseCsv('a\n"r1\r\nr2",x\n')[1].line).toBe(2)
  })

  it('a UTF-8 BOM at the very start is dropped, not a field character', () => {
    const recs = parseCsv('\ufeffid,name\n1,x\n')
    expect(recs[0].fields[0].text).toBe('id')
  })

  it('a BOM after the first character is data', () => {
    expect(texts(parseCsv('a\ufeffb\n'))).toEqual([['a\ufeffb']])
  })

  it('skips completely blank lines between records', () => {
    const recs = parseCsv('a,b\n\n1,2\n\n\n3,4\n')
    expect(texts(recs)).toEqual([['a', 'b'], ['1', '2'], ['3', '4']])
    expect(recs[2].line).toBe(6)
  })

  it('a trailing field after a delimiter is an (unquoted) empty field', () => {
    const recs = parseCsv('a,b\n1,\n')
    expect(recs[1].fields[1]).toEqual({ text: '', quoted: false })
  })

  it('one quoted empty field is a record with one empty quoted field', () => {
    const recs = parseCsv('""\n')
    expect(recs).toHaveLength(1)
    expect(recs[0].fields[0]).toEqual({ text: '', quoted: true })
  })

  it('supports other single-character delimiters', () => {
    expect(texts(parseCsv('a\tb\n1\t2', { delimiter: '\t' }))).toEqual([['a', 'b'], ['1', '2']])
    expect(texts(parseCsv('a;b\n"x;y";2', { delimiter: ';' }))).toEqual([['a', 'b'], ['x;y', '2']])
  })

  it('rejects delimiters that cannot work', () => {
    expect(() => new CsvParser({ delimiter: '""' })).toThrow()
    expect(() => new CsvParser({ delimiter: '"' })).toThrow()
    expect(() => new CsvParser({ delimiter: '\n' })).toThrow()
    expect(() => new CsvParser({ delimiter: '' })).toThrow()
  })
})

describe('CsvParser refusals', () => {
  it('a character after a closing quote is an error naming the line', () => {
    expect(() => parseCsv('a\n"closed"x\n')).toThrow(CsvParseError)
    try {
      parseCsv('a\n"closed"x\n')
    } catch (err) {
      expect((err as CsvParseError).line).toBe(2)
      expect((err as Error).message).toContain('closing quote')
    }
  })

  it('an unterminated quoted field at end of input names the line it started on', () => {
    try {
      parseCsv('a,b\n1,"still open\n2,3\n')
      expect.unreachable()
    } catch (err) {
      expect(err).toBeInstanceOf(CsvParseError)
      expect((err as CsvParseError).line).toBe(2)
      expect((err as Error).message).toContain('unterminated')
    }
  })

  it('a record above the size cap is refused (runaway quote, giant row)', () => {
    expect(() => parseCsv('"aaaaaaaaaa', { maxRecordChars: 5 })).toThrow(/exceeds 5 characters/)
    expect(() => parseCsv('a,b\n' + 'x'.repeat(10) + ',y', { maxRecordChars: 5 })).toThrow(/exceeds 5 characters/)
  })
})

describe('CsvParser is incremental: records straddling chunk boundaries', () => {
  function chunks(text: string, size: number): string[] {
    const out: string[] = []
    for (let i = 0; i < text.length; i += size) out.push(text.slice(i, i + size))
    return out
  }
  const cases: Array<{ name: string; input: string }> = [
    { name: 'quoted field across chunks', input: 'a,b\n1,"multi chunk"\n' },
    { name: 'doubled quote across chunks', input: 'a\n"say ""hi"" now"\n' },
    { name: 'CRLF across chunks', input: 'a,b\r\n1,2\r\n' },
    { name: 'quoted CRLF data across chunks', input: '"r1\r\nr2",x\n' },
    { name: 'delimiter run across chunks', input: 'a,b,c\n1,,\n' },
    { name: 'BOM split from the header', input: '\ufeffa,b\n1,2\n' },
    { name: 'record ending exactly at a chunk edge', input: 'a,b\n1,2\n3,4\n' },
  ]
  for (const size of [1, 2, 3, 5, 7]) {
    for (const c of cases) {
      it(`${c.name} (chunk size ${size})`, () => {
        const whole = parseCsv(c.input)
        const p = new CsvParser()
        const out = []
        for (const ch of chunks(c.input, size)) out.push(...p.push(ch))
        out.push(...p.end())
        expect(out).toEqual(whole)
      })
    }
  }

  it('an error thrown mid-stream keeps its line number when fed in chunks', () => {
    const p = new CsvParser()
    for (const ch of chunks('a\n1\n"open\n', 2)) p.push(ch)
    expect(() => p.end()).toThrow(CsvParseError)
  })
})

describe('CSV dialect matches the server export (round-trip fixtures)', () => {
  // Exact bytes the Go export encoder (writeCSVField) produces for the
  // adversarial `wide` fixture rows of the e2e leg. The client must read
  // them back with quoted/NULL semantics intact.
  it('server-exported adversarial rows parse back exactly', () => {
    const exported = [
      'id,note,raw',
      '9223372036854775807,,\\x00ff10',
      '-9223372036854775808,"",',
      '1,"a,""b""\nc",\\xdeadbeef',
      '2," lead and trail ",\\x',
    ].join('\n') + '\n'
    const recs = parseCsv(exported)
    expect(texts(recs)).toEqual([
      ['id', 'note', 'raw'],
      ['9223372036854775807', '', '\\x00ff10'],
      ['-9223372036854775808', '', ''],
      ['1', 'a,"b"\nc', '\\xdeadbeef'],
      ['2', ' lead and trail ', '\\x'],
    ])
    // NULL (unquoted empty) vs empty string (quoted) is exactly the
    // distinction the next import stage maps to NULL vs ''.
    expect(recs[1].fields[1].quoted).toBe(false)
    expect(recs[2].fields[1].quoted).toBe(true)
    expect(recs[1].fields[2].quoted).toBe(false)
    expect(recs[2].fields[2].quoted).toBe(false)
  })
})
