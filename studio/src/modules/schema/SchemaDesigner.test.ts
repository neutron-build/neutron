import { it, expect } from 'vitest'
import { colsFromDetail, indexesFromDetail } from './SchemaDesigner'
import type { SchemaObjectDetail } from '../../lib/types'
// Exercises production metadata adaptation; client DDL generation was retired
// in favor of the server's reviewed planner and actual component plan tests.
it('retains exact contract types and database-managed generation/default identity', () => {
  const detail = { table: { columns: [
    { name: '__proto__', type: 'numeric(36,18)', notNull: true, isPrimaryKey: true, default: { kind: 'identity' } },
    { name: 'calc', type: 'bigint', notNull: false, isPrimaryKey: false, generated: { expression: 'a + 1' } },
    { name: 'note', type: 'text', notNull: false, isPrimaryKey: false, default: { kind: 'literal', sql: "'quoted'" } },
  ] } } as SchemaObjectDetail
  const columns = colsFromDetail(detail)
  expect(columns[0]).toMatchObject({ name: '__proto__', originalName: '__proto__', dataType: 'numeric(36,18)', default: null, defaultKind: 'identity', isNullable: false, isPrimaryKey: true })
  expect(columns[1].generated).toBe('a + 1')
  expect(columns[2].default).toBe("'quoted'")
})
it('adapts expression index keys from actual catalog metadata', () => {
  expect(indexesFromDetail({ table: { indexes: [{ name: 'mixed', unique: true, key: [{ column: '__proto__' }, { expression: 'lower(note)' }] }] } } as SchemaObjectDetail)).toEqual([{ name: 'mixed', unique: true, key: '__proto__, lower(note)' }])
})
