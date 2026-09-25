import { describe, it, expect, vi } from 'vitest'
import {
  blobTextChunks, readSourceRecords, FieldRegistry, autoMap, mappingProblems,
  importTargets, isRequired, encodeRecord, encodeSourceValue, newImportJournal,
  runImport, resolvePending, retryPending, skipPending, skipFailedRow,
  markPendingCommitted, batchOperationId, journalKey, loadJournal, sameFile,
  describeJournal, MAX_BATCH_ROWS, MAX_BATCH_BYTES,
  type ColumnMapping, type ImportFormat, type ImportJournal, type ImportDeps,
  type CsvSourceOptions, type SourceField, type TableMetaCol,
} from './importer'
import { ApiError } from './api'
import type { TableMetaColumn } from './types'

// Importer logic tests (S06). The transport boundary (ImportDeps) is the
// only stub — the same seam the dialog injects; parsing/encoding/journal
// decisions run for real. Server-side exactness (tagged wire decoding,
// transactions, outcomes) is pinned by the Go e2e leg.

function col(name: string, o: Partial<TableMetaColumn> = {}): TableMetaColumn {
  return {
    name, type: o.type ?? 'text', tag: o.tag ?? null, nullable: o.nullable ?? true,
    isKey: o.isKey ?? false, generated: o.generated ?? false, identity: o.identity ?? false,
    hasDefault: o.hasDefault ?? false, autoAssigned: o.autoAssigned ?? false,
    editable: o.editable ?? true, readOnlyReason: o.readOnlyReason, insertable: o.insertable ?? true,
    ...o,
  } as TableMetaColumn
}

async function collect<T>(gen: AsyncIterable<T>): Promise<T[]> {
  const out: T[] = []
  for await (const x of gen) out.push(x)
  return out
}

function chunksOf(text: string): AsyncIterable<string> {
  return (async function* () { yield text })()
}

const CSV_OPTS: CsvSourceOptions = { delimiter: ',', header: true }

// --- source reading ---

describe('readSourceRecords: CSV', () => {
  it('header names the fields; data rows are numbered from 1', async () => {
    const reg = new FieldRegistry()
    const recs = await collect(readSourceRecords(chunksOf('id,NAME,note\n1,Alice,\n2,"Bob",x\n'), 'csv', CSV_OPTS, reg))
    expect(reg.fields.map(f => f.label)).toEqual(['id', 'NAME', 'note'])
    expect(recs.map(r => r.row)).toEqual([1, 2])
    expect(recs[1].values.get('c1')).toEqual({ kind: 'text', text: 'Bob', quoted: true })
    expect(recs[0].values.get('c2')).toEqual({ kind: 'text', text: '', quoted: false })
  })

  it('without a header the first record is data and fields are "column N"', async () => {
    const reg = new FieldRegistry()
    const recs = await collect(readSourceRecords(chunksOf('1,Alice\n2,Bob\n'), 'csv', { ...CSV_OPTS, header: false }, reg))
    expect(recs).toHaveLength(2)
    expect(reg.fields.map(f => f.label)).toEqual(['column 1', 'column 2'])
    expect(recs[0].values.get('c1')!.kind).toBe('text')
  })

  it('duplicate header names are disambiguated, empties named', async () => {
    const reg = new FieldRegistry()
    await collect(readSourceRecords(chunksOf('id,,id\n1,2,3\n'), 'csv', CSV_OPTS, reg))
    expect(reg.fields.map(f => f.label)).toEqual(['id', '(unnamed 2)', 'id (#3)'])
  })

  it('a wrong field count is a per-row shape error, not a file error', async () => {
    const reg = new FieldRegistry()
    const recs = await collect(readSourceRecords(chunksOf('a,b\n1,2\n3\n'), 'csv', CSV_OPTS, reg))
    expect(recs[1].shapeError).toContain('expected 2')
    expect(recs[0].shapeError).toBeUndefined()
  })

  it('a different delimiter flows through', async () => {
    const reg = new FieldRegistry()
    const recs = await collect(readSourceRecords(chunksOf('a\tb\n1\t2\n'), 'csv', { delimiter: '\t', header: true }, reg))
    expect(recs[0].values.get('c0')).toEqual({ kind: 'text', text: '1', quoted: false })
    expect(recs[0].values.get('c1')).toEqual({ kind: 'text', text: '2', quoted: false })
  })
})

describe('readSourceRecords: JSON', () => {
  it('an array of objects and NDJSON both stream, with first-seen field order', async () => {
    for (const text of ['[{"a":1,"b":"x"},{"a":2,"c":true}]', '{"a":1,"b":"x"}\n{"a":2,"c":true}']) {
      const reg = new FieldRegistry()
      const recs = await collect(readSourceRecords(chunksOf(text), 'json', CSV_OPTS, reg))
      expect(recs.map(r => r.row)).toEqual([1, 2])
      expect(reg.fields.map(f => f.label)).toEqual(['a', 'b', 'c'])
      expect(recs[0].values.get('k:a')).toEqual({ kind: 'json', value: { kind: 'number', raw: '1' } })
    }
  })

  it('numbers beyond 2^53 stay literal digits; nested values keep their slice', async () => {
    const reg = new FieldRegistry()
    const recs = await collect(readSourceRecords(chunksOf('{"big": 9007199254740993, "doc": {"v": [1, 2.50]}}'), 'json', CSV_OPTS, reg))
    expect(recs[0].values.get('k:big')).toEqual({ kind: 'json', value: { kind: 'number', raw: '9007199254740993' } })
    expect(recs[0].values.get('k:doc')).toEqual({ kind: 'json', value: { kind: 'object', raw: '{"v": [1, 2.50]}' } })
  })

  it('absent members are absent values (DEFAULT), not nulls', async () => {
    const reg = new FieldRegistry()
    const recs = await collect(readSourceRecords(chunksOf('{"a":1}\n{"a":null,"b":2}\n'), 'json', CSV_OPTS, reg))
    expect(recs[0].values.has('k:b')).toBe(false)
    expect(recs[1].values.get('k:a')).toEqual({ kind: 'json', value: { kind: 'null' } })
  })

  it('a malformed file fails the stream with its line', async () => {
    const reg = new FieldRegistry()
    await expect(collect(readSourceRecords(chunksOf('{"a":1}\nnot json\n'), 'json', CSV_OPTS, reg))).rejects.toThrow(/line 2/)
  })
})

describe('blobTextChunks', () => {
  it('streams a Blob as text and refuses invalid UTF-8 loudly', async () => {
    const good = new Blob(['{"a":1}\n{"a":2}\n'])
    expect(await collect(blobTextChunks(good))).toEqual(['{"a":1}\n{"a":2}\n'])
    const bad = new Blob([new Uint8Array([0x22, 0xff, 0xfe, 0x22])])
    await expect(collect(blobTextChunks(bad))).rejects.toThrow(/not valid UTF-8/)
  })
})

// --- mapping ---

const FIELDS: SourceField[] = [
  { id: 'c0', label: 'ID' },
  { id: 'c1', label: 'name' },
  { id: 'k:x', label: 'extra' },
]

describe('mapping', () => {
  const cols = [
    col('id', { type: 'int4', nullable: false, isKey: true, editable: false, insertable: true }),
    col('name', { nullable: true }),
    col('forced', { nullable: false, hasDefault: false }),
    col('def', { nullable: false, hasDefault: true }),
  ]
  it('targets are insertable columns only', () => {
    const targets = importTargets([...cols, col('gen', { generated: true, insertable: false })])
    expect(targets.map(c => c.name)).toEqual(['id', 'name', 'forced', 'def'])
  })
  it('auto-map matches case-insensitively; the rest are DEFAULT', () => {
    expect(autoMap(FIELDS, cols)).toEqual({
      id: { kind: 'field', field: 'c0' },
      name: { kind: 'field', field: 'c1' },
      forced: { kind: 'default' },
      def: { kind: 'default' },
    })
  })
  it('required = NOT NULL without default; problems name exactly those', () => {
    expect(isRequired(cols[2])).toBe(true)
    expect(isRequired(cols[0])).toBe(true) // a NOT NULL key without a default must be mapped too
    expect(isRequired(cols[3])).toBe(false) // NOT NULL with a default is fine omitted
    const problems = mappingProblems({ id: { kind: 'field', field: 'c0' }, forced: { kind: 'default' }, def: { kind: 'default' }, name: { kind: 'null' } }, cols)
    expect(problems).toEqual(['forced is NOT NULL without a default — map a source field to it'])
    expect(mappingProblems({ id: { kind: 'field', field: 'c0' }, forced: { kind: 'field', field: 'k:x' }, def: { kind: 'default' }, name: { kind: 'null' } }, cols)).toEqual([])
    expect(mappingProblems({ id: { kind: 'default' }, forced: { kind: 'default' }, def: { kind: 'default' }, name: { kind: 'default' } }, cols)).toContain('map at least one source field to a column')
  })
})

// --- encoding: the exactness core ---

const VOPTS = { emptyUnquoted: 'null' as const, nullMarker: null as string | null }

describe('encodeSourceValue: CSV (quoted flags and markers)', () => {
  it('unquoted empty is NULL by default; "" is the empty string; \\N marker honors config', () => {
    const c = col('note')
    expect(encodeSourceValue({ kind: 'text', text: '', quoted: false }, c, VOPTS)).toEqual({ kind: 'null' })
    expect(encodeSourceValue({ kind: 'text', text: '', quoted: true }, c, VOPTS)).toEqual({ kind: 'value', value: '' })
    expect(encodeSourceValue({ kind: 'text', text: '', quoted: false }, c, { emptyUnquoted: 'empty', nullMarker: null })).toEqual({ kind: 'value', value: '' })
    expect(encodeSourceValue({ kind: 'text', text: '\\N', quoted: false }, c, { emptyUnquoted: 'empty', nullMarker: '\\N' })).toEqual({ kind: 'null' })
    expect(encodeSourceValue({ kind: 'text', text: '\\N', quoted: true }, c, { emptyUnquoted: 'empty', nullMarker: '\\N' })).toEqual({ kind: 'value', value: '\\N' })
  })

  it('int8 keeps digits exact, validates range, and is never a Number', () => {
    const c = col('qty', { type: 'bigint', tag: 'int8' })
    expect(encodeSourceValue({ kind: 'text', text: '9007199254740993', quoted: false }, c, VOPTS)).toEqual({ kind: 'value', value: { t: 'int8', v: '9007199254740993' } })
    expect(encodeSourceValue({ kind: 'text', text: '9223372036854775807', quoted: false }, c, VOPTS)).toEqual({ kind: 'value', value: { t: 'int8', v: '9223372036854775807' } })
    expect(encodeSourceValue({ kind: 'text', text: '-9223372036854775808', quoted: false }, c, VOPTS)).toEqual({ kind: 'value', value: { t: 'int8', v: '-9223372036854775808' } })
    expect(() => encodeSourceValue({ kind: 'text', text: '9223372036854775808', quoted: false }, c, VOPTS)).toThrow(/out of range/)
    expect(() => encodeSourceValue({ kind: 'text', text: '12.5', quoted: false }, c, VOPTS)).toThrow(/not an integer/)
    expect(() => encodeSourceValue({ kind: 'text', text: '', quoted: false }, c, VOPTS)).not.toThrow() // empty -> NULL before tag encoding
  })

  it('numeric keeps 40 digits as a tagged decimal string; bytea takes \\x or bare hex', () => {
    const num = col('amount', { type: 'numeric', tag: 'numeric' })
    expect(encodeSourceValue({ kind: 'text', text: '1234567890123456789012345678901234567890.0123456789', quoted: false }, num, VOPTS))
      .toEqual({ kind: 'value', value: { t: 'numeric', v: '1234567890123456789012345678901234567890.0123456789' } })
    const bytes = col('raw', { type: 'bytea', tag: 'bytea' })
    expect(encodeSourceValue({ kind: 'text', text: '\\x00FF10', quoted: false }, bytes, VOPTS)).toEqual({ kind: 'value', value: { t: 'bytea', v: '00ff10' } })
    expect(encodeSourceValue({ kind: 'text', text: '00ff10', quoted: false }, bytes, VOPTS)).toEqual({ kind: 'value', value: { t: 'bytea', v: '00ff10' } })
    expect(() => encodeSourceValue({ kind: 'text', text: '\\x0g', quoted: false }, bytes, VOPTS)).toThrow(/hex/)
    expect(() => encodeSourceValue({ kind: 'text', text: '\\x0ab', quoted: false }, bytes, VOPTS)).toThrow(/hex/)
  })

  it('booleans accept words; vector/tsvector refuse; temporal text re-tags', () => {
    const b = col('flag', { type: 'boolean' })
    for (const [t, v] of [['t', true], ['TRUE', true], ['yes', true], ['1', true], ['f', false], ['NO', false], ['off', false], ['0', false]] as const) {
      expect(encodeSourceValue({ kind: 'text', text: t, quoted: false }, b, VOPTS)).toEqual({ kind: 'value', value: v })
    }
    expect(() => encodeSourceValue({ kind: 'text', text: 'maybe', quoted: false }, b, VOPTS)).toThrow(/not a boolean/)
    expect(() => encodeSourceValue({ kind: 'text', text: 'x', quoted: false }, col('v', { type: 'vector', tag: 'vector' }), VOPTS)).toThrow(/not importable/)
    const ts = col('seen_at', { type: 'timestamptz', tag: 'timestamptz' })
    expect(encodeSourceValue({ kind: 'text', text: '2026-02-03 04:05:06.123456+00', quoted: false }, ts, VOPTS))
      .toEqual({ kind: 'value', value: { t: 'timestamptz', v: '2026-02-03 04:05:06.123456+00' } })
  })

  it('JSON columns take raw JSON text; other tagged columns refuse junk empty', () => {
    const j = col('doc', { type: 'jsonb' })
    expect(encodeSourceValue({ kind: 'text', text: '{"a": [1, 2.50]}', quoted: true }, j, VOPTS)).toEqual({ kind: 'value', value: '{"a": [1, 2.50]}' })
    expect(() => encodeSourceValue({ kind: 'text', text: 'nope', quoted: true }, j, VOPTS)).toThrow(/not valid JSON/)
    expect(() => encodeSourceValue({ kind: 'text', text: ' ', quoted: false }, col('d', { type: 'date', tag: 'date' }), VOPTS)).toThrow(/empty text/)
  })
})

describe('encodeSourceValue: JSON sources', () => {
  const json = (v: import('./losslessJson').LosslessJson) => ({ kind: 'json' as const, value: v })
  it('JSON null is SQL NULL; an absent member is DEFAULT', () => {
    expect(encodeSourceValue(json({ kind: 'null' }), col('x'), VOPTS)).toEqual({ kind: 'null' })
    expect(encodeSourceValue(undefined, col('x'), VOPTS)).toEqual({ kind: 'omit' })
  })
  it('a JSON number keeps its literal digits into tagged columns', () => {
    expect(encodeSourceValue(json({ kind: 'number', raw: '9007199254740993' }), col('q', { type: 'bigint', tag: 'int8' }), VOPTS))
      .toEqual({ kind: 'value', value: { t: 'int8', v: '9007199254740993' } })
    expect(encodeSourceValue(json({ kind: 'number', raw: '1234567890123456789012345678901234567890.0123456789' }), col('n', { type: 'numeric', tag: 'numeric' }), VOPTS))
      .toEqual({ kind: 'value', value: { t: 'numeric', v: '1234567890123456789012345678901234567890.0123456789' } })
  })
  it('type mismatches refuse instead of coercing', () => {
    expect(() => encodeSourceValue(json({ kind: 'number', raw: '1' }), col('f', { type: 'boolean' }), VOPTS)).toThrow(/not a boolean/)
    expect(() => encodeSourceValue(json({ kind: 'object', raw: '{"a":1}' }), col('q', { type: 'bigint', tag: 'int8' }), VOPTS)).toThrow()
    expect(() => encodeSourceValue(json({ kind: 'array', raw: '[1]' }), col('t'), VOPTS)).not.toThrow() // untagged text column: raw JSON text
  })
  it('booleans cross only into boolean columns', () => {
    expect(encodeSourceValue(json({ kind: 'boolean', value: true }), col('f', { type: 'boolean' }), VOPTS)).toEqual({ kind: 'value', value: true })
    expect(encodeSourceValue(json({ kind: 'boolean', value: false }), col('t'), VOPTS)).toEqual({ kind: 'value', value: 'false' })
  })
})

describe('encodeRecord: the row contract', () => {
  const cols = [
    col('id', { type: 'int4', nullable: false, isKey: true, editable: false }),
    col('qty', { type: 'bigint', tag: 'int8', nullable: false }),
    col('note', { nullable: true }),
    col('def', { nullable: false, hasDefault: true }),
  ]
  const mapping: Record<string, ColumnMapping> = {
    id: { kind: 'field', field: 'c0' },
    qty: { kind: 'field', field: 'c1' },
    note: { kind: 'field', field: 'c2' },
    def: { kind: 'default' },
  }
  function csvRow(...cells: Array<[string, boolean]>): import('./importer').SourceRecord {
    const values = new Map<string, import('./importer').SourceValue>()
    cells.forEach(([text, quoted], i) => values.set(`c${i}`, { kind: 'text', text, quoted }))
    return { row: 1, line: 1, values }
  }

  it('NULL, empty string and DEFAULT stay three distinct things; digits exact', () => {
    const values = encodeRecord(csvRow(['7', false], ['9223372036854775807', false], ['', true]), cols, mapping, VOPTS)
    expect(values).toEqual({ id: '7', qty: { t: 'int8', v: '9223372036854775807' }, note: '' })
  })

  it('omitted NOT-NULL-without-default columns and NULL into NOT NULL refuse, naming the column', () => {
    expect(() => encodeRecord(csvRow(['7', false], ['', false], ['x', false]), cols, mapping, VOPTS)).toThrow(/qty.*NOT NULL/)
    const noId = { ...mapping, id: { kind: 'default' as const } }
    expect(() => encodeRecord(csvRow(['7', false], ['1', false], ['x', false]), cols, noId, VOPTS)).toThrow(/id is NOT NULL without a default/)
  })

  it('a NULL-mapped column sends SQL NULL only where nullable', () => {
    const nullNote = { ...mapping, note: { kind: 'null' as const } }
    expect(encodeRecord(csvRow(['1', false], ['2', false], ['ignored', false]), cols, nullNote, VOPTS)).toEqual({ id: '1', qty: { t: 'int8', v: '2' }, note: null })
    const nullQty = { ...mapping, qty: { kind: 'null' as const } }
    expect(() => encodeRecord(csvRow(['1', false], ['ignored', false], ['x', false]), cols, nullQty, VOPTS)).toThrow(/qty is NOT NULL/)
  })

  it('a row with a shape error refuses before any column encoding', () => {
    const rec = { ...csvRow(['1', false]), shapeError: 'has 1 field; expected 3' }
    expect(() => encodeRecord(rec, cols, mapping, VOPTS)).toThrow(/has 1 field/)
  })
})

// --- journal and runner ---

function plan(over: Partial<ImportJournal['plan']> = {}) {
  return {
    connectionId: 'c1', schema: 'public', table: 'imp', binding: 'e1:100',
    format: 'csv' as ImportFormat, csv: { delimiter: ',', header: true },
    values: { emptyUnquoted: 'null' as const, nullMarker: null },
    mapping: {} as Record<string, ColumnMapping>, batchSize: MAX_BATCH_ROWS, ...over,
  }
}

function deps(over: Partial<ImportDeps> = {}): ImportDeps & { sent: unknown[][]; saved: ImportJournal[] } {
  const sent: unknown[][] = []
  const saved: ImportJournal[] = []
  const d: ImportDeps & { sent: unknown[][]; saved: ImportJournal[] } = {
    sent, saved,
    sendBatch: async req => { sent.push([req.operationId, req.rows]); return { operationId: req.operationId, applied: req.rows.length } },
    outcome: async () => { throw new Error('not expected') },
    save: j => saved.push(JSON.parse(JSON.stringify(j))),
    sleep: async () => {},
    ...over,
  }
  return d
}

function csvStream(text: string): AsyncIterable<import('./importer').SourceRecord> {
  const reg = new FieldRegistry()
  return readSourceRecords(chunksOf(text), 'csv', { delimiter: ',', header: true }, reg)
}

describe('runImport: batching, atomicity reporting and recovery', () => {
  const cols = [col('id', { type: 'int4', nullable: false, isKey: true }), col('v', { nullable: true })]
  const mapping: Record<string, ColumnMapping> = { id: { kind: 'field', field: 'c0' }, v: { kind: 'field', field: 'c1' } }
  const planCfg = plan({ mapping, batchSize: 3 })

  it('commits batch by batch, each under its own operation ID', async () => {
    const csv = 'id,v\n' + [1, 2, 3, 4, 5, 6, 7].map(i => `${i},x${i}`).join('\n') + '\n'
    const d = deps()
    const j = await runImport(csvStream(csv), cols, newImportJournal(planCfg, { name: 'f', size: 1, lastModified: 0 }, 'imp1'), d)
    expect(j.status).toBe('done')
    expect(j.rowsCommitted).toBe(7)
    expect(j.batchesCommitted).toBe(3)
    expect(d.sent.map(s => s[1])).toEqual([
      [{ id: '1', v: 'x1' }, { id: '2', v: 'x2' }, { id: '3', v: 'x3' }],
      [{ id: '4', v: 'x4' }, { id: '5', v: 'x5' }, { id: '6', v: 'x6' }],
      [{ id: '7', v: 'x7' }],
    ])
    expect(d.sent.map(s => s[0])).toEqual([
      batchOperationId('imp1', 1, 1), batchOperationId('imp1', 2, 1), batchOperationId('imp1', 3, 1)])
  })

  it('a byte-budget cut keeps single requests under the server bound', async () => {
    const big = 'x'.repeat(Math.ceil(MAX_BATCH_BYTES / 2))
    const csv = 'id,v\n' + [1, 2, 3].map(i => `${i},${big}`).join('\n') + '\n'
    const d = deps()
    const j = await runImport(csvStream(csv), cols, newImportJournal(plan({ mapping, batchSize: 100 }), { name: 'f', size: 1, lastModified: 0 }, 'imp2'), d)
    expect(j.status).toBe('done')
    expect(d.sent.every(s => (s[1] as unknown[]).length === 1)).toBe(true)
    expect(j.batchesCommitted).toBe(3)
  })

  it('a definite refusal (constraint) fails only its batch, names the source row, earlier batches stay', async () => {
    const csv = 'id,v\n1,ok\n2,ok\n3,bad\n4,never\n'
    const d = deps({
      sendBatch: async req => {
        d.sent.push([req.operationId, req.rows])
        if (req.rows.some(r => r.v === 'bad')) {
          throw new ApiError(409, 'operations[0]: duplicate key', { state: 'constraint', body: { failedRow: 0, applied: 0 } })
        }
        return { operationId: req.operationId, applied: req.rows.length }
      },
    })
    const j = await runImport(csvStream(csv), cols, newImportJournal(plan({ mapping, batchSize: 2 }), { name: 'f', size: 1, lastModified: 0 }, 'imp3'), d)
    expect(j.status).toBe('needs-decision')
    expect(j.rowsCommitted).toBe(2) // batch 1 committed
    expect(j.pending!.batch).toBe(2)
    expect(j.pending!.state).toBe('failed')
    expect(j.pending!.failedRow).toBe(3) // source row 3, mapped through the batch window
    expect(j.pending!.rows).toEqual([3, 4])
    // user skips exactly the failed row and retries the rest under a NEW id
    skipFailedRow(j)
    expect(j.skipped).toEqual([{ fromRow: 3, toRow: 3, reason: expect.stringContaining('duplicate') }])
    const j2 = await runImport(csvStream(csv), cols, j, d)
    expect(j2.status).toBe('done')
    expect(j2.rowsCommitted).toBe(3)
    expect(d.sent.length).toBe(3) // batch 1, the refused attempt, the retry under a new ID
    expect(d.sent[1][1]).toEqual([{ id: '3', v: 'bad' }, { id: '4', v: 'never' }])
    expect(d.sent[2][1]).toEqual([{ id: '4', v: 'never' }]) // the skipped row is not re-sent
    expect(d.sent.map(s => s[0])).toEqual([batchOperationId('imp3', 1, 1), batchOperationId('imp3', 2, 1), batchOperationId('imp3', 2, 2)])
  })

  it('a dropped response resolves through the server outcome before anything else', async () => {
    const csv = 'id,v\n1,x\n'
    const d = deps({
      sendBatch: async () => { throw new TypeError('network dropped') },
      outcome: async (_c, opId) => ({
        operationId: opId, state: 'committed', response: { operationId: opId, applied: 1 },
      }),
    })
    const j = await runImport(csvStream(csv), cols, newImportJournal(plan({ mapping }), { name: 'f', size: 1, lastModified: 0 }, 'imp4'), d)
    expect(j.status).toBe('done') // resolved through the outcome, then the source was exhausted
    expect(j.rowsCommitted).toBe(1)
    expect(d.sent.length).toBe(0) // the only send threw; nothing was re-sent
    // resuming from the finished journal re-sends nothing
    const j2 = await runImport(csvStream(csv), cols, j, deps())
    expect(j2.status).toBe('done')
    expect(j2.rowsCommitted).toBe(1)
  })

  it('an unknown outcome needs a decision; mark-committed advances honestly', async () => {
    const csv = 'id,v\n1,x\n2,y\n'
    const d = deps({
      sendBatch: async () => { throw new TypeError('network dropped') },
      outcome: async (_c, opId) => ({ operationId: opId, state: 'unknown' as const }),
    })
    const j = await runImport(csvStream(csv), cols, newImportJournal(plan({ mapping, batchSize: 1 }), { name: 'f', size: 1, lastModified: 0 }, 'imp5'), d)
    expect(j.status).toBe('needs-decision')
    expect(j.pending!.state).toBe('unknown')
    markPendingCommitted(j)
    expect(j.rowsCommitted).toBe(1)
    const j2 = await runImport(csvStream(csv), cols, j, deps())
    expect(j2.status).toBe('done')
    expect(j2.rowsCommitted).toBe(2)
  })

  it('resolvePending polls an in-progress batch until it settles', async () => {
    let calls = 0
    const j = newImportJournal(plan({ mapping }), { name: 'f', size: 1, lastModified: 0 }, 'imp6')
    j.pending = { batch: 1, operationId: 'op', attempt: 1, fromRow: 1, toRow: 2, rows: [1, 2], state: 'sending' }
    const d = deps({
      outcome: async (_c, opId) => {
        calls++
        if (calls < 3) return { operationId: opId, state: 'in_progress' as const }
        return { operationId: opId, state: 'failed', response: { applied: 0, error: 'operations[1]: boom' } }
      },
    })
    const out = await resolvePending(j, d)
    expect(out.pending!.state).toBe('failed')
    expect(out.pending!.failedRow).toBe(2)
    expect(out.status).toBe('needs-decision')
  })

  it('the abort signal pauses between batches; the journal resumes from nextRow without duplicates', async () => {
    const csv = 'id,v\n' + [1, 2, 3, 4].map(i => `${i},x`).join('\n') + '\n'
    const ctl = new AbortController()
    const d = deps({
      signal: ctl.signal,
      sendBatch: async req => {
        ctl.abort() // stop after the first batch is sent
        return { operationId: req.operationId, applied: req.rows.length }
      },
    })
    const j = await runImport(csvStream(csv), cols, newImportJournal(plan({ mapping, batchSize: 2 }), { name: 'f', size: 1, lastModified: 0 }, 'imp7'), d)
    expect(j.status).toBe('paused')
    expect(j.nextRow).toBe(2)
    const j2 = await runImport(csvStream(csv), cols, j, deps())
    expect(j2.status).toBe('done')
    expect(j2.rowsCommitted).toBe(4)
  })

  it('a malformed file stops the import with an honest error; committed batches stay counted', async () => {
    const gen = (async function* () {
      yield { row: 1, line: 1, values: new Map([['c0', { kind: 'text' as const, text: '1', quoted: false }], ['c1', { kind: 'text' as const, text: 'x', quoted: false }]]) }
      yield { row: 2, line: 2, values: new Map([['c0', { kind: 'text' as const, text: '2', quoted: false }], ['c1', { kind: 'text' as const, text: 'y', quoted: false }]]) }
      throw new Error('line 3: unterminated quoted field at end of input')
    })()
    const j = await runImport(gen, cols, newImportJournal(plan({ mapping, batchSize: 1 }), { name: 'f', size: 1, lastModified: 0 }, 'imp8'), deps())
    expect(j.status).toBe('error')
    expect(j.error).toContain('unterminated')
    expect(j.rowsCommitted).toBe(2)
  })

  it('a user-decided retry sends directly: the fresh ID is never resolved through the outcome lookup', async () => {
    const csv = 'id,v\n1,ok\n2,ok\n'
    let thrown = false
    const outcomes: string[] = []
    const d = deps({
      sendBatch: async req => {
        d.sent.push([req.operationId, req.rows])
        if (!thrown) {
          thrown = true
          throw new ApiError(409, 'operations[0]: boom', { state: 'constraint', body: { applied: 0 } })
        }
        return { operationId: req.operationId, applied: req.rows.length }
      },
      outcome: async (_c, opId) => { outcomes.push(opId); return { operationId: opId, state: 'unknown' as const } },
    })
    let j = await runImport(csvStream(csv), cols, newImportJournal(plan({ mapping, batchSize: 2 }), { name: 'f', size: 1, lastModified: 0 }, 'impR'), d)
    expect(j.status).toBe('needs-decision') // definite failure, nothing applied
    retryPending(j)
    // resolving a never-sent retry ID must not happen: the batch goes out
    j = await runImport(csvStream(csv), cols, j, d)
    expect(j.status).toBe('done')
    expect(j.rowsCommitted).toBe(2)
    expect(outcomes).toEqual([]) // no outcome lookups for the fresh attempt
    expect(d.sent).toHaveLength(2)
  })

  it('an in-flight interruption is still resolved through the outcome before resending', async () => {
    const csv = 'id,v\n1,ok\n'
    const seen: string[] = []
    const d = deps({
      sendBatch: async () => { throw new TypeError('dropped') },
      outcome: async (_c, opId) => {
        seen.push(opId)
        return { operationId: opId, state: 'committed', response: { applied: 1 } }
      },
    })
    const j = await runImport(csvStream(csv), cols, newImportJournal(plan({ mapping }), { name: 'f', size: 1, lastModified: 0 }, 'impR2'), d)
    expect(j.status).toBe('done')
    expect(seen).toEqual([batchOperationId('impR2', 1, 1)])
  })

  it('an invalid row is separated from valid rows and becomes its own skip decision', async () => {
    const csv = 'id,v\n1,10\n2,bad\n3,30\n'
    const typed = [col('id', { type: 'int4', nullable: false, isKey: true }), col('v', { type: 'bigint', tag: 'int8' })]
    const d = deps()
    const j = await runImport(csvStream(csv), typed, newImportJournal(plan({ mapping, batchSize: 10 }), { name: 'f', size: 1, lastModified: 0 }, 'imp9'), d)
    expect(j.status).toBe('needs-decision')
    expect(j.pending!.state).toBe('invalid')
    expect(j.pending!.failedRow).toBe(2)
    expect(d.sent[0][1]).toEqual([{ id: '1', v: { t: 'int8', v: '10' } }]) // valid prefix sent first
    skipPending(j)
    const j2 = await runImport(csvStream(csv), typed, j, deps())
    expect(j2.status).toBe('done')
    expect(j2.rowsCommitted).toBe(2) // row 1 + row 3
  })
})

describe('journal persistence helpers', () => {
  it('loads round-trips, junk is null, keys are per connection+table', () => {
    const store = new Map<string, string>()
    const storage = {
      getItem: (k: string) => store.get(k) ?? null,
      setItem: (k: string, v: string) => void store.set(k, v),
      removeItem: (k: string) => void store.delete(k),
    }
    expect(journalKey('c1', 'public', 'imp')).toBe('neutron-studio:import:c1:public.imp')
    const j = newImportJournal(plan(), { name: 'f.csv', size: 10, lastModified: 5 }, 'z')
    storage.setItem(journalKey('c1', 'public', 'imp'), JSON.stringify(j))
    expect(loadJournal(storage, journalKey('c1', 'public', 'imp'))!.importId).toBe('z')
    storage.setItem('junk', '{nope')
    expect(loadJournal(storage, 'junk')).toBeNull()
    expect(loadJournal(storage, 'missing')).toBeNull()
  })
  it('sameFile compares name, size and mtime', () => {
    expect(sameFile({ name: 'a', size: 1, lastModified: 2 }, { name: 'a', size: 1, lastModified: 2 })).toBe(true)
    expect(sameFile({ name: 'a', size: 1, lastModified: 2 }, { name: 'a', size: 1, lastModified: 3 })).toBe(false)
  })
  it('describeJournal reports committed, skipped and handled rows', () => {
    const j = newImportJournal(plan(), { name: 'f', size: 1, lastModified: 0 }, 'z')
    j.rowsCommitted = 10
    j.batchesCommitted = 4
    j.skipped = [{ fromRow: 3, toRow: 3, reason: 'bad' }]
    j.nextRow = 11
    expect(describeJournal(j)).toBe('10 rows committed in 4 batches; 1 row skipped (3–3); source rows 1–11 handled')
  })
  it('retryPending mints a new operation ID per attempt', () => {
    const j = newImportJournal(plan(), { name: 'f', size: 1, lastModified: 0 }, 'z')
    j.pending = { batch: 2, operationId: batchOperationId('z', 2, 1), attempt: 1, fromRow: 1, toRow: 2, rows: [1, 2], state: 'failed' }
    retryPending(j)
    expect(j.pending!.operationId).toBe(batchOperationId('z', 2, 2))
    expect(j.pending!.attempt).toBe(2)
  })
})
