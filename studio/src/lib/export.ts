import type { QueryResult } from './types'
import { formatCell } from './wire'

/**
 * Shared export utilities: exportCSV/exportJSON serve the model modules
 * (KV, documents, FTS, pub/sub, blobs) from their in-memory lists; their
 * output format is pinned by compatibility tests (export.test.ts).
 */

function download(content: string, filename: string, mime: string): void {
  const blob = new Blob([content], { type: mime })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  a.click()
  URL.revokeObjectURL(url)
}

export function exportCSV(data: Record<string, unknown>[], filename: string): void {
  if (data.length === 0) return
  const escape = (v: unknown) => {
    const s = v === null || v === undefined ? '' : String(v)
    return s.includes(',') || s.includes('"') || s.includes('\n')
      ? `"${s.replace(/"/g, '""')}"` : s
  }
  const keys = Object.keys(data[0])
  const header = keys.map(escape).join(',')
  const rows = data.map(row => keys.map(k => escape(row[k])).join(','))
  download([header, ...rows].join('\n'), filename, 'text/csv')
}

export function exportJSON(data: unknown, filename: string): void {
  download(JSON.stringify(data, null, 2), filename, 'application/json')
}

// --- Query-result export (S06) ---
//
// Exports the rows a grid already holds (the SQL editor's bounded result).
// Whole tables are exported by the server as a stream (api.tableExport);
// these never re-fetch. Formats match the server export's conventions:
// CSV keeps SQL NULL (unquoted empty field) and the empty string ("")
// distinct; JSON writes bigint digits exactly (number literals, never
// through a double), bytea as its \x-hex text, json values as JSON.

function csvCell(v: unknown): string {
  if (v === null || v === undefined) return ''
  const s = formatCell(v)
  const needs = s === '' || /[",\r\n]/.test(s) || /^[ \t]|[ \t]$/.test(s)
  return needs ? `"${s.replace(/"/g, '""')}"` : s
}

export function resultToCSV(result: QueryResult): string {
  const lines = [result.columns.map(c => csvCell(c)).join(',')]
  for (const row of result.rows) {
    lines.push((row as unknown[]).map(csvCell).join(','))
  }
  return lines.join('\n') + '\n'
}

function jsonCell(v: unknown): string {
  if (v === null || v === undefined) return 'null'
  if (typeof v === 'bigint') return v.toString()
  if (v instanceof Uint8Array) return JSON.stringify(formatCell(v))
  return JSON.stringify(v)
}

export function resultToJSON(result: QueryResult): string {
  const keys = result.columns.map(c => JSON.stringify(c))
  const rows = result.rows.map(row =>
    '{' + keys.map((k, i) => `${k}:${jsonCell((row as unknown[])[i])}`).join(',') + '}')
  return rows.length === 0 ? '[]\n' : '[\n' + rows.join(',\n') + '\n]\n'
}

export function exportResultCSV(result: QueryResult, filename: string): void {
  download(resultToCSV(result), filename, 'text/csv')
}

export function exportResultJSON(result: QueryResult, filename: string): void {
  download(resultToJSON(result), filename, 'application/json')
}
