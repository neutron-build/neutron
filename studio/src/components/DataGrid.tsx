import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  createColumnHelper,
} from '@tanstack/react-table'
import { useMemo } from 'preact/hooks'
import type { QueryResult } from '../lib/types'
import s from './DataGrid.module.css'

interface DataGridProps {
  result: QueryResult
}

export function DataGrid({ result }: DataGridProps) {
  const columnHelper = useMemo(() => createColumnHelper<unknown[]>(), [])

  const columns = useMemo(
    () =>
      result.columns.map((col, i) =>
        columnHelper.accessor((row) => (row as unknown[])[i], {
          id: col,
          header: col,
          cell: (info) => {
            const val = info.getValue()
            if (val === null) return <span class={s.null}>NULL</span>
            if (val === undefined) return <span class={s.null}>—</span>
            return String(val)
          },
        })
      ),
    [result.columns]
  )

  const table = useReactTable({
    data: result.rows as unknown[][],
    columns,
    getCoreRowModel: getCoreRowModel(),
  })

  if (result.error) {
    return <div class={s.error}>{result.error}</div>
  }

  return (
    <div class={s.wrap}>
      <div class={s.scrollArea}>
        <table class={s.table}>
          <thead>
            {table.getHeaderGroups().map(hg => (
              <tr key={hg.id}>
                {hg.headers.map(header => (
                  <th key={header.id} class={s.th}>
                    {flexRender(header.column.columnDef.header, header.getContext())}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.map(row => (
              <tr key={row.id} class={s.tr}>
                {row.getVisibleCells().map(cell => (
                  <td key={cell.id} class={s.td}>
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div class={s.footer}>
        <span class={s.footerText}>
          {result.rowCount} row{result.rowCount === 1 ? '' : 's'}
          {result.duration != null ? ` · ${result.duration}ms` : ''}
        </span>
        <div class={s.exportBtns}>
          <button class={s.exportBtn} onClick={() => exportCSV(result)}>↓ CSV</button>
          <button class={s.exportBtn} onClick={() => exportJSON(result)}>↓ JSON</button>
        </div>
      </div>
    </div>
  )
}

// --- Export helpers ---

function download(content: string, filename: string, mime: string) {
  const blob = new Blob([content], { type: mime })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  a.click()
  URL.revokeObjectURL(url)
}

function exportCSV(result: QueryResult) {
  const escape = (v: unknown) => {
    const s = v === null || v === undefined ? '' : String(v)
    return s.includes(',') || s.includes('"') || s.includes('\n')
      ? `"${s.replace(/"/g, '""')}"` : s
  }
  const header = result.columns.map(escape).join(',')
  const rows = result.rows.map(row => (row as unknown[]).map(escape).join(','))
  download([header, ...rows].join('\n'), 'export.csv', 'text/csv')
}

function exportJSON(result: QueryResult) {
  const objects = result.rows.map(row =>
    Object.fromEntries(result.columns.map((col, i) => [col, (row as unknown[])[i]]))
  )
  download(JSON.stringify(objects, null, 2), 'export.json', 'application/json')
}
