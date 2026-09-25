// Lossless, streaming JSON record reading for the table importer (S06).
//
// JSON.parse turns every number into an IEEE double: 9007199254740993
// silently becomes 9007199254740992 and 40-digit decimals lose digits. The
// importer must never do that (the S01 wire contract: bigint/decimal digits
// never cross through a JavaScript number), so this module keeps every
// number as its literal source text and every nested object/array as its
// exact source slice.
//
// Two layers:
//   - JsonRecordSplitter: incremental. Accepts a top-level ARRAY of records
//     ([{...}, {...}]) or NEWLINE-DELIMITED / concatenated records
//     ({...}\n{...}), chosen by the first non-whitespace character, and
//     yields each record's exact source text. Memory is bounded by the
//     largest single record (maxRecordChars), never by the file.
//   - parseJsonRecord: strict recursive-descent parse of one record, which
//     must be an object. Members map to LosslessJson values; duplicate keys
//     are an error (which value was meant is ambiguous).

export type LosslessJson =
  | { kind: 'null' }
  | { kind: 'boolean'; value: boolean }
  /** raw: the number's exact literal text. */
  | { kind: 'number'; raw: string }
  | { kind: 'string'; value: string }
  /** raw: the exact source text of the nested object/array. */
  | { kind: 'object'; raw: string }
  | { kind: 'array'; raw: string }

export class JsonImportError extends Error {
  line: number
  constructor(message: string, line: number) {
    super(`line ${line}: ${message}`)
    this.name = 'JsonImportError'
    this.line = line
  }
}

export interface JsonRecordText {
  text: string
  /** 1-based line the record starts on. */
  line: number
}

const DEFAULT_MAX_RECORD_CHARS = 1 << 20

function isWs(c: number): boolean {
  return c === 0x20 || c === 0x09 || c === 0x0a || c === 0x0d
}

/**
 * Incremental record splitter. Tracks string/escape state and nesting depth
 * only — full validation happens per record in parseJsonRecord.
 */
export class JsonRecordSplitter {
  private mode: 'detect' | 'array' | 'stream' | 'done' = 'detect'
  private readonly maxChars: number
  private depth = 0
  private inString = false
  private escaped = false
  private buf = ''
  private recordLine = 1
  private line = 1
  private inRecord = false
  /** array mode: a ',' or '[' was seen and an element must follow. */
  private expectElement = false
  private sawElement = false
  private started = false
  private out: JsonRecordText[] = []

  constructor(options: { maxRecordChars?: number } = {}) {
    this.maxChars = options.maxRecordChars ?? DEFAULT_MAX_RECORD_CHARS
  }

  private grow(text: string) {
    this.buf += text
    if (this.buf.length > this.maxChars) {
      throw new JsonImportError(`record exceeds ${this.maxChars} characters`, this.recordLine)
    }
  }

  private emit() {
    this.out.push({ text: this.buf, line: this.recordLine })
    this.buf = ''
    this.inRecord = false
  }

  push(chunk: string): JsonRecordText[] {
    let i = 0
    if (!this.started && chunk.length > 0) {
      this.started = true
      if (chunk.charCodeAt(0) === 0xfeff) i = 1
    }
    let segStart = -1
    const flushSeg = (end: number) => {
      if (segStart >= 0) {
        this.grow(chunk.slice(segStart, end))
        segStart = -1
      }
    }
    for (; i < chunk.length; i++) {
      const c = chunk.charCodeAt(i)
      if (c === 0x0a) this.line++

      if (this.inRecord) {
        if (segStart < 0) segStart = i
        if (this.inString) {
          if (this.escaped) this.escaped = false
          else if (c === 0x5c) this.escaped = true
          else if (c === 0x22) this.inString = false
          continue
        }
        if (c === 0x22) { this.inString = true; continue }
        if (c === 0x7b || c === 0x5b) { this.depth++; continue }
        if (c === 0x7d || c === 0x5d) {
          if (this.depth > 0) {
            this.depth--
            if (this.depth === 0 && this.mode === 'stream') {
              flushSeg(i + 1)
              this.emit()
            }
            continue
          }
          // depth 0 closing bracket: only the array's own ']' is legal here
          if (this.mode === 'array' && c === 0x5d) {
            flushSeg(i)
            this.finishArrayElement()
            this.mode = 'done'
            continue
          }
          throw new JsonImportError(`unexpected ${JSON.stringify(chunk[i])}`, this.line)
        }
        if (this.mode === 'array' && this.depth === 0 && c === 0x2c) {
          flushSeg(i)
          this.finishArrayElement()
          this.expectElement = true
          continue
        }
        if (this.mode === 'stream' && this.depth === 0) {
          // a scalar at the top level of a record stream is not a record
          throw new JsonImportError('each record must be a JSON object', this.recordLine)
        }
        continue
      }

      // between records
      if (isWs(c)) continue
      switch (this.mode) {
        case 'detect':
          if (c === 0x5b) {
            this.mode = 'array'
            this.expectElement = true
            continue
          }
          if (c === 0x7b) {
            this.mode = 'stream'
            this.startRecord()
            this.depth = 1
            segStart = i
            continue
          }
          throw new JsonImportError('expected a JSON array of objects or newline-delimited JSON objects', this.line)
        case 'array':
          if (c === 0x5d) {
            if (this.expectElement && this.sawElement) {
              throw new JsonImportError('trailing comma before "]"', this.line)
            }
            this.mode = 'done'
            continue
          }
          if (c === 0x2c) {
            throw new JsonImportError('missing array element', this.line)
          }
          if (!this.expectElement) {
            throw new JsonImportError('expected "," or "]" between array elements', this.line)
          }
          this.startRecord()
          this.expectElement = false
          this.sawElement = true
          segStart = i
          // reprocess this character inside the record
          if (c === 0x22) this.inString = true
          else if (c === 0x7b || c === 0x5b) this.depth = 1
          continue
        case 'stream':
          if (c !== 0x7b) throw new JsonImportError('each record must be a JSON object', this.line)
          this.startRecord()
          this.depth = 1
          segStart = i
          continue
        case 'done':
          throw new JsonImportError('unexpected data after the closing "]"', this.line)
      }
    }
    flushSeg(chunk.length)
    const done = this.out
    this.out = []
    return done
  }

  private startRecord() {
    this.inRecord = true
    this.recordLine = this.line
    this.depth = 0
    this.inString = false
    this.escaped = false
  }

  private finishArrayElement() {
    const text = this.buf.trim()
    if (text === '') throw new JsonImportError('missing array element', this.line)
    this.buf = text
    this.emit()
  }

  end(): JsonRecordText[] {
    if (this.inRecord) {
      if (this.mode === 'array') throw new JsonImportError('unterminated array (missing "]")', this.recordLine)
      throw new JsonImportError('unterminated record at end of input', this.recordLine)
    }
    if (this.mode === 'array') throw new JsonImportError('unterminated array (missing "]")', this.line)
    const done = this.out
    this.out = []
    return done
  }
}

// --- strict single-record parser ---

class Cursor {
  pos = 0
  constructor(readonly text: string, readonly line: number) {}
  fail(message: string): never {
    throw new JsonImportError(`${message} (at character ${this.pos + 1} of the record)`, this.line)
  }
  ws() {
    while (this.pos < this.text.length && isWs(this.text.charCodeAt(this.pos))) this.pos++
  }
}

const NUMBER_RE = /-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/y

function parseString(cur: Cursor): string {
  const start = cur.pos
  cur.pos++ // opening quote
  const t = cur.text
  while (cur.pos < t.length) {
    const c = t.charCodeAt(cur.pos)
    if (c === 0x5c) { cur.pos += 2; continue }
    if (c === 0x22) {
      cur.pos++
      const raw = t.slice(start, cur.pos)
      try {
        // Strings (unlike numbers) decode exactly through JSON.parse.
        return JSON.parse(raw) as string
      } catch {
        cur.pos = start
        cur.fail('invalid string')
      }
    }
    if (c < 0x20) cur.fail('control character in string')
    cur.pos++
  }
  cur.pos = start
  return cur.fail('unterminated string')
}

function parseValue(cur: Cursor): LosslessJson {
  cur.ws()
  const t = cur.text
  const c = t.charCodeAt(cur.pos)
  if (c === 0x22) return { kind: 'string', value: parseString(cur) }
  if (c === 0x7b || c === 0x5b) {
    const start = cur.pos
    if (c === 0x7b) parseObjectMembers(cur, null)
    else parseArray(cur)
    return { kind: c === 0x7b ? 'object' : 'array', raw: t.slice(start, cur.pos) }
  }
  if (t.startsWith('true', cur.pos)) { cur.pos += 4; return { kind: 'boolean', value: true } }
  if (t.startsWith('false', cur.pos)) { cur.pos += 5; return { kind: 'boolean', value: false } }
  if (t.startsWith('null', cur.pos)) { cur.pos += 4; return { kind: 'null' } }
  NUMBER_RE.lastIndex = cur.pos
  const m = NUMBER_RE.exec(t)
  if (m) {
    cur.pos += m[0].length
    return { kind: 'number', raw: m[0] }
  }
  return cur.fail('expected a JSON value')
}

function parseArray(cur: Cursor) {
  cur.pos++ // [
  cur.ws()
  if (cur.text[cur.pos] === ']') { cur.pos++; return }
  for (;;) {
    parseValue(cur)
    cur.ws()
    const ch = cur.text[cur.pos]
    if (ch === ',') { cur.pos++; continue }
    if (ch === ']') { cur.pos++; return }
    cur.fail('expected "," or "]" in array')
  }
}

function parseObjectMembers(cur: Cursor, into: Map<string, LosslessJson> | null) {
  cur.pos++ // {
  cur.ws()
  if (cur.text[cur.pos] === '}') { cur.pos++; return }
  const seen = new Set<string>()
  for (;;) {
    cur.ws()
    if (cur.text[cur.pos] !== '"') cur.fail('expected a member name')
    const key = parseString(cur)
    if (into) {
      if (seen.has(key)) cur.fail(`duplicate member ${JSON.stringify(key)}`)
      seen.add(key)
    }
    cur.ws()
    if (cur.text[cur.pos] !== ':') cur.fail('expected ":" after a member name')
    cur.pos++
    const value = parseValue(cur)
    if (into) into.set(key, value)
    cur.ws()
    const ch = cur.text[cur.pos]
    if (ch === ',') { cur.pos++; continue }
    if (ch === '}') { cur.pos++; return }
    cur.fail('expected "," or "}" in object')
  }
}

/**
 * Parse one record's text strictly. It must be exactly one JSON object;
 * numbers keep their literal text, nested values their source slice.
 */
export function parseJsonRecord(text: string, line = 1): Map<string, LosslessJson> {
  const cur = new Cursor(text, line)
  cur.ws()
  if (cur.text[cur.pos] !== '{') cur.fail('each record must be a JSON object')
  const members = new Map<string, LosslessJson>()
  parseObjectMembers(cur, members)
  cur.ws()
  if (cur.pos !== text.length) cur.fail('unexpected data after the record')
  return members
}

/** Split and parse a whole JSON document (tests, small inputs). */
export function parseJsonRecords(text: string): Array<{ members: Map<string, LosslessJson>; line: number }> {
  const sp = new JsonRecordSplitter()
  return [...sp.push(text), ...sp.end()].map(r => ({ members: parseJsonRecord(r.text, r.line), line: r.line }))
}
