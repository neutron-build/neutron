/**
 * Shared export utilities for modules that don't use DataGrid
 * (which has its own built-in CSV/JSON export).
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
