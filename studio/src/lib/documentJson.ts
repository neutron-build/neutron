import { parseJsonRecord, type LosslessJson } from './losslessJson'

/** Numeric nodes carry source lexemes; they never become JS numbers. */
export class JsonNumber {
  constructor(readonly raw: string) {}
}
export type DocumentValue = null | boolean | string | JsonNumber | DocumentValue[] | { [key: string]: DocumentValue }
export type DocumentPath = (string | number)[]

// Reuse the strict importer parser, wrapping any JSON value as a record.
// parseJsonRecord validates recursively while retaining every numeric token.
export function parseDocument(text: string): DocumentValue {
  const members = parseJsonRecord(`{"value":${text}}`)
  if (members.size !== 1) throw new Error('Expected exactly one JSON value')
  return fromValue(members.get('value')!)
}
function fromValue(value: LosslessJson): DocumentValue {
  switch (value.kind) {
    case 'null': return null
    case 'boolean': case 'string': return value.value
    case 'number': return new JsonNumber(value.raw)
    case 'object': {
      const out: { [key: string]: DocumentValue } = Object.create(null)
      for (const [key, child] of parseJsonRecord(value.raw)) out[key] = fromValue(child)
      return out
    }
    case 'array': {
      // Locate top-level values without parsing their digits into doubles.
      const raw = value.raw
      const out: DocumentValue[] = []
      let start = 1, depth = 0, quoted = false, escaped = false
      for (let i = 1; i < raw.length; i++) {
        const c = raw[i]
        if (quoted) {
          if (escaped) escaped = false
          else if (c === '\\') escaped = true
          else if (c === '"') quoted = false
          continue
        }
        if (c === '"') { quoted = true; continue }
        if (c === '[' || c === '{') depth++
        else if (depth > 0 && (c === ']' || c === '}')) depth--
        else if (depth === 0 && (c === ',' || c === ']')) {
          const child = raw.slice(start, i).trim()
          if (child) out.push(parseDocument(child))
          start = i + 1
        }
      }
      return out
    }
  }
}
export function serializeDocument(value: DocumentValue): string {
  if (value instanceof JsonNumber) return value.raw
  if (Array.isArray(value)) return `[${value.map(serializeDocument).join(',')}]`
  if (value !== null && typeof value === 'object') {
    return `{${Object.entries(value).map(([k, v]) => `${JSON.stringify(k)}:${serializeDocument(v)}`).join(',')}}`
  }
  return JSON.stringify(value)
}
/** Immutable replacement, resolving own keys only. Empty path replaces the root. */
export function replaceDocument(value: DocumentValue, path: DocumentPath, replacement: DocumentValue): DocumentValue {
  if (path.length === 0) return replacement
  const [key, ...rest] = path
  if (!value || typeof value !== 'object' || value instanceof JsonNumber || !Object.hasOwn(value, key) ||
      (Array.isArray(value) ? typeof key !== 'number' : typeof key !== 'string')) throw new Error('Document path no longer exists')
  if (Array.isArray(value)) {
    const copy = [...value]
    copy[key as number] = replaceDocument(value[key as number], rest, replacement)
    return copy
  }
  const copy = Object.assign(Object.create(null), value)
  copy[key] = replaceDocument(value[key as string], rest, replacement)
  return copy
}
