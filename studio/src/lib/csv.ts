// Streaming CSV parsing for the table importer (S06).
//
// RFC 4180 plus the one distinction PostgreSQL's COPY ... CSV makes and a
// plain string array loses: whether a field was QUOTED. An unquoted empty
// field and a quoted empty field ("") are different inputs — the importer
// maps the first to SQL NULL (by default) and the second to the empty
// string — so every field carries its `quoted` flag.
//
// The parser is incremental: push() takes arbitrary chunks (a record, a
// quoted field or a CRLF may straddle chunk boundaries) and returns the
// records completed so far; end() flushes the last record. Memory is bounded
// by the largest single record (maxRecordChars), never by the file.
//
// Rules:
//   - fields are separated by the delimiter (default ","); records end at
//     LF, CRLF or a lone CR;
//   - a field that starts with a quote is quoted: delimiters and line breaks
//     inside it are data, "" is one literal quote, and after the closing
//     quote only a delimiter, a line break or the end of input may follow
//     (anything else is a CsvParseError, never a guess);
//   - a quote inside an unquoted field is a literal character;
//   - a UTF-8 byte-order mark at the very start is dropped;
//   - completely blank lines are skipped (a record needs at least one
//     character or delimiter); write "" for a single empty-string column;
//   - an unterminated quoted field at end of input is an error naming the
//     line it started on.

export interface CsvField {
  text: string
  /** The field was written in quotes ("" is a quoted empty field). */
  quoted: boolean
}

export interface CsvRecord {
  fields: CsvField[]
  /** 1-based physical line the record starts on. */
  line: number
}

export class CsvParseError extends Error {
  line: number
  constructor(message: string, line: number) {
    super(`line ${line}: ${message}`)
    this.name = 'CsvParseError'
    this.line = line
  }
}

export interface CsvParserOptions {
  /** Single-character field delimiter (default ","). */
  delimiter?: string
  /** Largest accepted record in characters (default 1 MiB). */
  maxRecordChars?: number
}

const QUOTE = 0x22
const LF = 0x0a
const CR = 0x0d

const FIELD_START = 0
const UNQUOTED = 1
const QUOTED = 2
const QUOTE_IN_QUOTED = 3
const AFTER_CR = 4

export const DEFAULT_MAX_RECORD_CHARS = 1 << 20

export class CsvParser {
  private readonly delim: number
  private readonly maxChars: number
  private state = FIELD_START
  private buf = ''
  private quoted = false
  private fields: CsvField[] = []
  private recordChars = 0
  private inRecord = false
  private recordLine = 1
  private line = 1
  private started = false
  private quotedPrevCR = false
  private out: CsvRecord[] = []

  constructor(options: CsvParserOptions = {}) {
    const d = options.delimiter ?? ','
    if (d.length !== 1 || d === '"' || d === '\n' || d === '\r') {
      throw new Error('CSV delimiter must be one character other than a quote or line break')
    }
    this.delim = d.charCodeAt(0)
    this.maxChars = options.maxRecordChars ?? DEFAULT_MAX_RECORD_CHARS
  }

  private beginRecord() {
    if (!this.inRecord) {
      this.inRecord = true
      this.recordLine = this.line
    }
  }

  private append(text: string) {
    if (text.length === 0) return
    this.recordChars += text.length
    if (this.recordChars > this.maxChars) {
      throw new CsvParseError(
        `record exceeds ${this.maxChars} characters (an unterminated quote, or a row too large to import)`,
        this.recordLine)
    }
    this.buf += text
  }

  private pushField() {
    this.fields.push({ text: this.buf, quoted: this.quoted })
    this.buf = ''
    this.quoted = false
  }

  private endRecord() {
    this.out.push({ fields: this.fields, line: this.recordLine })
    this.fields = []
    this.recordChars = 0
    this.inRecord = false
  }

  /** Feed the next chunk; returns the records it completed. */
  push(chunk: string): CsvRecord[] {
    let i = 0
    const n = chunk.length
    if (!this.started && n > 0) {
      this.started = true
      if (chunk.charCodeAt(0) === 0xfeff) i = 1
    }
    const delim = this.delim
    while (i < n) {
      switch (this.state) {
        case AFTER_CR: {
          this.state = FIELD_START
          if (chunk.charCodeAt(i) === LF) i++
          break
        }
        case FIELD_START: {
          const c = chunk.charCodeAt(i)
          if (c === QUOTE) {
            this.beginRecord()
            this.quoted = true
            this.quotedPrevCR = false
            this.state = QUOTED
            i++
          } else if (c === delim) {
            this.beginRecord()
            this.pushField()
            i++
          } else if (c === LF || c === CR) {
            if (this.inRecord) {
              // after a delimiter: the record ends with an empty field
              this.pushField()
              this.endRecord()
            }
            this.line++
            this.state = c === CR ? AFTER_CR : FIELD_START
            i++
          } else {
            this.beginRecord()
            this.state = UNQUOTED
          }
          break
        }
        case UNQUOTED: {
          let j = i
          while (j < n) {
            const c = chunk.charCodeAt(j)
            if (c === delim || c === LF || c === CR) break
            j++
          }
          this.append(chunk.slice(i, j))
          i = j
          if (j === n) break
          const c = chunk.charCodeAt(j)
          this.pushField()
          i++
          if (c === delim) {
            this.state = FIELD_START
          } else {
            this.endRecord()
            this.line++
            this.state = c === CR ? AFTER_CR : FIELD_START
          }
          break
        }
        case QUOTED: {
          let j = i
          while (j < n) {
            const c = chunk.charCodeAt(j)
            if (c === QUOTE) break
            // CR, LF and CRLF each count as one line break
            if (c === CR || (c === LF && !this.quotedPrevCR)) this.line++
            this.quotedPrevCR = c === CR
            j++
          }
          this.append(chunk.slice(i, j))
          i = j
          if (j < n) {
            this.state = QUOTE_IN_QUOTED
            i++
          }
          break
        }
        case QUOTE_IN_QUOTED: {
          const c = chunk.charCodeAt(i)
          if (c === QUOTE) {
            this.append('"')
            this.quotedPrevCR = false
            this.state = QUOTED
            i++
          } else if (c === delim) {
            this.pushField()
            this.state = FIELD_START
            i++
          } else if (c === LF || c === CR) {
            this.pushField()
            this.endRecord()
            this.line++
            this.state = c === CR ? AFTER_CR : FIELD_START
            i++
          } else {
            throw new CsvParseError(
              `unexpected character ${JSON.stringify(chunk[i])} after a closing quote (quote the whole field and double inner quotes)`,
              this.line)
          }
          break
        }
      }
    }
    const done = this.out
    this.out = []
    return done
  }

  /** Signal end of input; returns the final record, if any. */
  end(): CsvRecord[] {
    if (this.state === QUOTED) {
      throw new CsvParseError('unterminated quoted field at end of input', this.recordLine)
    }
    if (this.inRecord) {
      this.pushField()
      this.endRecord()
    }
    this.state = FIELD_START
    const done = this.out
    this.out = []
    return done
  }
}

/** Parse a whole CSV string (tests, previews of small inputs). */
export function parseCsv(text: string, options?: CsvParserOptions): CsvRecord[] {
  const p = new CsvParser(options)
  return [...p.push(text), ...p.end()]
}
