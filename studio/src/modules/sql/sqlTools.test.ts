import { describe, it, expect, beforeEach } from 'vitest'
import { EditorState } from '@codemirror/state'
import { CompletionContext, type CompletionResult, type CompletionSource } from '@codemirror/autocomplete'
import { sql, PostgreSQL } from '@codemirror/lang-sql'
import type { SqlTable } from '../../lib/types'
import {
  buildCompletionNamespace, completionSchemas, defaultCompletionSchema, flattenPlan,
  isWriteStatement, leadingKeyword, loadHistory, newRequestId, parameterCount,
  pushHistory, quoteIdentIfNeeded, HISTORY_MAX,
  mayChangeCatalog,
} from './sqlTools'

describe('parameterCount', () => {
  it('counts the highest positional parameter', () => {
    expect(parameterCount('SELECT 1')).toBe(0)
    expect(parameterCount('SELECT * FROM t WHERE a = $1 AND b = $2')).toBe(2)
    expect(parameterCount('SELECT $3')).toBe(3)
    expect(parameterCount('SELECT $1, $1, $10')).toBe(10)
  })

  it('ignores $n inside literals, quoted identifiers, dollar quotes and comments', () => {
    expect(parameterCount(`SELECT '$1', "col$2", E'it\\'s $3' -- $4\n /* $5 /* $6 */ */`)).toBe(0)
    expect(parameterCount('SELECT $$ $1 $$, $fn$ $2 $fn$, $1')).toBe(1)
    expect(parameterCount("SELECT 'it''s $2', $1")).toBe(1)
    expect(parameterCount('SELECT a$1 FROM t')).toBe(0)
  })
})

describe('statement classification (UI hints only)', () => {
  it('finds the leading keyword past comments', () => {
    expect(leadingKeyword('-- c\n/* x */ select 1')).toBe('SELECT')
    expect(leadingKeyword('(SELECT 1)')).toBe('(')
    expect(leadingKeyword('')).toBe('')
  })

  it('flags writes, data-modifying CTEs and FOR UPDATE, not reads', () => {
    expect(isWriteStatement('DELETE FROM t')).toBe(true)
    expect(isWriteStatement('insert into t values (1)')).toBe(true)
    expect(isWriteStatement('WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d')).toBe(true)
    expect(isWriteStatement('SELECT * FROM t FOR UPDATE')).toBe(true)
    expect(isWriteStatement('SELECT * FROM t')).toBe(false)
    expect(isWriteStatement("SELECT 'DELETE FROM t'")).toBe(false)
    expect(isWriteStatement('SELECT "update" FROM t')).toBe(false)
  })
})

const catalog: SqlTable[] = [
  { schema: 'public', name: 'users', columns: [
    { name: 'id', type: 'int4', nullable: false, isPrimaryKey: true },
    { name: 'Email', type: 'text', nullable: true, isPrimaryKey: false },
  ] },
  { schema: 'app', name: 'orders', columns: [
    { name: 'order_id', type: 'int8', nullable: false, isPrimaryKey: true },
  ] },
  { schema: 'app', name: 'Line Items', columns: [] },
]

/** Run the completion sources a real lang-sql configuration installs. */
async function completeAt(doc: string, defaultSchema: string): Promise<string[]> {
  const state = EditorState.create({
    doc,
    selection: { anchor: doc.length },
    extensions: [sql({ dialect: PostgreSQL, schema: buildCompletionNamespace(catalog), defaultSchema })],
  })
  const sources = state.languageDataAt<CompletionSource>('autocomplete', doc.length)
  const labels: string[] = []
  for (const source of sources) {
    const res = (await source(new CompletionContext(state, doc.length, true))) as CompletionResult | null
    if (res) labels.push(...res.options.map(o => o.label))
  }
  return labels
}

describe('catalog-aware completion', () => {
  it('lists schemas and defaults to public, keeping a still-valid choice', () => {
    expect(completionSchemas(catalog)).toEqual(['app', 'public'])
    expect(defaultCompletionSchema(catalog)).toBe('public')
    expect(defaultCompletionSchema(catalog, 'app')).toBe('app')
    expect(defaultCompletionSchema(catalog, 'gone')).toBe('public')
    expect(defaultCompletionSchema([catalog[1]])).toBe('app')
  })

  it('completes the selected schema unqualified and others schema-qualified', async () => {
    const publicDefault = await completeAt('SELECT * FROM ', 'public')
    expect(publicDefault).toContain('users')
    expect(publicDefault).not.toContain('orders')
    expect(publicDefault).toContain('app')

    const appDefault = await completeAt('SELECT * FROM ', 'app')
    expect(appDefault).toContain('orders')
    expect(appDefault).not.toContain('users')
  })

  it('completes columns after a table, with types as detail', async () => {
    expect(await completeAt('SELECT users.', 'public')).toEqual(expect.arrayContaining(['id', 'Email']))
    expect(await completeAt('SELECT app.orders.', 'public')).toContain('order_id')
    const ns = buildCompletionNamespace(catalog) as Record<string, Record<string, { label: string; detail?: string; apply?: string }[]>>
    expect(ns.public.users[0]).toMatchObject({ label: 'id', detail: 'int4' })
    // Mixed-case names need quotes in PostgreSQL.
    expect(ns.public.users[1].apply).toBe('"Email"')
  })

  it('quotes identifiers PostgreSQL would fold or reject', () => {
    expect(quoteIdentIfNeeded('users')).toBe('users')
    expect(quoteIdentIfNeeded('Users')).toBe('"Users"')
    expect(quoteIdentIfNeeded('line items')).toBe('"line items"')
    expect(quoteIdentIfNeeded('a"b')).toBe('"a""b"')
  })
})

describe('flattenPlan', () => {
  // Shape of PostgreSQL 17 EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) output.
  const analyzed = [{
    Plan: {
      'Node Type': 'Hash Join', 'Join Type': 'Inner', 'Startup Cost': 1.07, 'Total Cost': 2.2,
      'Plan Rows': 3, 'Actual Rows': 3, 'Actual Total Time': 0.041, 'Actual Loops': 1,
      'Hash Cond': '(o.user_id = u.id)',
      Plans: [
        { 'Node Type': 'Seq Scan', 'Relation Name': 'orders', Alias: 'o', 'Startup Cost': 0, 'Total Cost': 1.03, 'Plan Rows': 3, 'Actual Rows': 3, 'Actual Total Time': 0.006, 'Actual Loops': 1 },
        { 'Node Type': 'Hash', 'Startup Cost': 1.03, 'Total Cost': 1.03, 'Plan Rows': 3, 'Actual Rows': 3, 'Actual Loops': 1,
          Plans: [{ 'Node Type': 'Index Scan', 'Relation Name': 'users', Alias: 'users', 'Index Name': 'users_pkey', 'Index Cond': '(id = 1)', 'Startup Cost': 0, 'Total Cost': 1.03, 'Plan Rows': 1, 'Actual Rows': 1, 'Actual Loops': 1 }] },
      ],
    },
    'Planning Time': 0.21,
    'Execution Time': 0.08,
  }]

  it('walks nodes in plan order with depth, targets, details and stats', () => {
    const s = flattenPlan(analyzed)!
    expect(s.analyzed).toBe(true)
    expect(s.planningTime).toBe(0.21)
    expect(s.executionTime).toBe(0.08)
    expect(s.rows.map(r => [r.depth, r.nodeType])).toEqual([[0, 'Hash Join'], [1, 'Seq Scan'], [1, 'Hash'], [2, 'Index Scan']])
    expect(s.rows[0].detail).toBe('Inner join; hash: (o.user_id = u.id)')
    expect(s.rows[1].target).toBe('orders o')
    expect(s.rows[3]).toMatchObject({ target: 'users', detail: 'using users_pkey; index: (id = 1)', actualRows: 1 })
  })

  it('reports a plain plan as not analyzed', () => {
    const s = flattenPlan([{ Plan: { 'Node Type': 'ModifyTable', Operation: 'Delete', 'Total Cost': 1, 'Plan Rows': 0 } }])!
    expect(s.analyzed).toBe(false)
    expect(s.rows[0]).toMatchObject({ nodeType: 'ModifyTable', detail: 'Delete' })
    expect(s.rows[0].actualRows).toBeUndefined()
  })

  it('refuses documents that are not a PostgreSQL JSON plan', () => {
    expect(flattenPlan(null)).toBeNull()
    expect(flattenPlan('Seq Scan on t')).toBeNull()
    expect(flattenPlan([{ QUERY: 'x' }])).toBeNull()
  })
})

describe('request ids', () => {
  it('match the server request-id pattern and differ', () => {
    const a = newRequestId()
    const b = newRequestId()
    expect(a).toMatch(/^[A-Za-z0-9_-]{8,128}$/)
    expect(a).not.toBe(b)
  })
})

describe('history (extends the existing per-connection store)', () => {
  beforeEach(() => localStorage.clear())

  it('reads entries written before S04 (no params/status)', () => {
    localStorage.setItem('neutron:query-history:c1', JSON.stringify([{ sql: 'SELECT 1', executedAt: 'x', duration: 1, rowCount: 1 }]))
    expect(loadHistory('c1')).toEqual([{ sql: 'SELECT 1', executedAt: 'x', duration: 1, rowCount: 1 }])
  })

  it('keeps runs with different parameters apart and moves identical runs to the top', () => {
    const base = { executedAt: 'x', duration: 1, rowCount: 1 }
    pushHistory('c1', { ...base, sql: 'SELECT $1', params: ['a'] })
    pushHistory('c1', { ...base, sql: 'SELECT $1', params: [null] })
    pushHistory('c1', { ...base, sql: 'SELECT $1', params: ['a'], status: 'canceled' })
    const h = loadHistory('c1')
    expect(h).toHaveLength(2)
    expect(h[0]).toMatchObject({ params: ['a'], status: 'canceled' })
    expect(h[1].params).toEqual([null])
  })

  it('caps history and survives corrupt storage', () => {
    for (let i = 0; i < HISTORY_MAX + 5; i++) pushHistory('c2', { sql: `SELECT ${i}`, executedAt: '', duration: 0, rowCount: 0 })
    expect(loadHistory('c2')).toHaveLength(HISTORY_MAX)
    localStorage.setItem('neutron:query-history:c3', '{"not":"an array"}')
    expect(loadHistory('c3')).toEqual([])
  })
})

describe('mayChangeCatalog (S05 schema refresh trigger)', () => {
  it('matches DDL statements anywhere in a script', () => {
    expect(mayChangeCatalog('create table t (x int)')).toBe(true)
    expect(mayChangeCatalog('select 1; ALTER TABLE t ADD COLUMN y int')).toBe(true)
    expect(mayChangeCatalog('  drop view v')).toBe(true)
    expect(mayChangeCatalog('comment on table t is \'x\'')).toBe(true)
  })
  it('ignores reads, DML and commented-out DDL', () => {
    expect(mayChangeCatalog('select * from created_things')).toBe(false)
    expect(mayChangeCatalog('update t set dropped = true')).toBe(false)
    expect(mayChangeCatalog('-- drop table t\nselect 1')).toBe(false)
    expect(mayChangeCatalog('/* alter table t */ select 1')).toBe(false)
  })
})
