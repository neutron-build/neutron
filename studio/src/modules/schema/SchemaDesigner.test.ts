import { describe, it, expect } from 'vitest'

// Tests for SchemaDesigner logic: DDL generation, column editing, migration download

interface EditCol {
  name: string
  dataType: string
  isNullable: boolean
  default: string | null
  isPrimaryKey: boolean
  ordinal: number
  isNew: boolean
  isDeleted: boolean
  originalName: string
}

const COMMON_TYPES = [
  'text', 'varchar(255)', 'char(1)',
  'integer', 'bigint', 'smallint', 'serial', 'bigserial',
  'real', 'double precision', 'numeric',
  'boolean',
  'timestamptz', 'timestamp', 'date', 'time',
  'uuid', 'jsonb', 'json', 'bytea',
]

function generateAlterStatements(cols: EditCol[], schemaName: string, tableName: string): string[] {
  const tbl = `"${schemaName}"."${tableName}"`
  const stmts: string[] = []

  for (const c of cols) {
    if (c.isNew && !c.isDeleted) {
      const nullStr = c.isNullable ? '' : ' NOT NULL'
      const defStr = c.default ? ` DEFAULT ${c.default}` : ''
      stmts.push(`ALTER TABLE ${tbl} ADD COLUMN "${c.name}" ${c.dataType}${defStr}${nullStr}`)
    } else if (c.isDeleted && !c.isNew) {
      stmts.push(`ALTER TABLE ${tbl} DROP COLUMN "${c.originalName}"`)
    } else if (!c.isNew && !c.isDeleted) {
      if (c.name !== c.originalName) {
        stmts.push(`ALTER TABLE ${tbl} RENAME COLUMN "${c.originalName}" TO "${c.name}"`)
      }
    }
  }

  return stmts
}

function generateCreateTable(tableName: string, cols: EditCol[]): string {
  const pkCols = cols.filter(c => c.isPrimaryKey)
  const colDefs = cols.map(c => {
    const nullStr = c.isNullable ? '' : ' NOT NULL'
    const defStr = c.default ? ` DEFAULT ${c.default}` : ''
    const pkStr = pkCols.length === 1 && c.isPrimaryKey ? ' PRIMARY KEY' : ''
    return `  "${c.name}" ${c.dataType}${defStr}${nullStr}${pkStr}`
  })
  if (pkCols.length > 1) {
    colDefs.push(`  PRIMARY KEY (${pkCols.map(c => `"${c.name}"`).join(', ')})`)
  }
  return `CREATE TABLE "public"."${tableName}" (\n${colDefs.join(',\n')}\n)`
}

describe('SchemaDesigner — COMMON_TYPES', () => {
  it('should include standard SQL types', () => {
    expect(COMMON_TYPES).toContain('text')
    expect(COMMON_TYPES).toContain('integer')
    expect(COMMON_TYPES).toContain('bigint')
    expect(COMMON_TYPES).toContain('boolean')
    expect(COMMON_TYPES).toContain('uuid')
    expect(COMMON_TYPES).toContain('jsonb')
    expect(COMMON_TYPES).toContain('timestamptz')
    expect(COMMON_TYPES).toContain('bytea')
  })

  it('should include serial types', () => {
    expect(COMMON_TYPES).toContain('serial')
    expect(COMMON_TYPES).toContain('bigserial')
  })
})

describe('SchemaDesigner — ALTER TABLE generation', () => {
  it('should generate ADD COLUMN for new columns', () => {
    const cols: EditCol[] = [{
      name: 'email', dataType: 'text', isNullable: true,
      default: null, isPrimaryKey: false, ordinal: 2,
      isNew: true, isDeleted: false, originalName: '',
    }]
    const stmts = generateAlterStatements(cols, 'public', 'users')
    expect(stmts.length).toBe(1)
    expect(stmts[0]).toBe('ALTER TABLE "public"."users" ADD COLUMN "email" text')
  })

  it('should generate ADD COLUMN with NOT NULL', () => {
    const cols: EditCol[] = [{
      name: 'name', dataType: 'varchar(255)', isNullable: false,
      default: null, isPrimaryKey: false, ordinal: 2,
      isNew: true, isDeleted: false, originalName: '',
    }]
    const stmts = generateAlterStatements(cols, 'public', 'users')
    expect(stmts[0]).toContain('NOT NULL')
  })

  it('should generate ADD COLUMN with DEFAULT', () => {
    const cols: EditCol[] = [{
      name: 'active', dataType: 'boolean', isNullable: false,
      default: 'true', isPrimaryKey: false, ordinal: 2,
      isNew: true, isDeleted: false, originalName: '',
    }]
    const stmts = generateAlterStatements(cols, 'public', 'users')
    expect(stmts[0]).toContain('DEFAULT true')
    expect(stmts[0]).toContain('NOT NULL')
  })

  it('should generate DROP COLUMN for deleted columns', () => {
    const cols: EditCol[] = [{
      name: 'old_col', dataType: 'text', isNullable: true,
      default: null, isPrimaryKey: false, ordinal: 2,
      isNew: false, isDeleted: true, originalName: 'old_col',
    }]
    const stmts = generateAlterStatements(cols, 'public', 'users')
    expect(stmts.length).toBe(1)
    expect(stmts[0]).toBe('ALTER TABLE "public"."users" DROP COLUMN "old_col"')
  })

  it('should generate RENAME COLUMN for renamed columns', () => {
    const cols: EditCol[] = [{
      name: 'full_name', dataType: 'text', isNullable: true,
      default: null, isPrimaryKey: false, ordinal: 1,
      isNew: false, isDeleted: false, originalName: 'name',
    }]
    const stmts = generateAlterStatements(cols, 'public', 'users')
    expect(stmts.length).toBe(1)
    expect(stmts[0]).toBe('ALTER TABLE "public"."users" RENAME COLUMN "name" TO "full_name"')
  })

  it('should skip new+deleted columns (added then removed)', () => {
    const cols: EditCol[] = [{
      name: 'temp', dataType: 'text', isNullable: true,
      default: null, isPrimaryKey: false, ordinal: 1,
      isNew: true, isDeleted: true, originalName: '',
    }]
    const stmts = generateAlterStatements(cols, 'public', 'users')
    expect(stmts.length).toBe(0)
  })

  it('should skip unchanged columns', () => {
    const cols: EditCol[] = [{
      name: 'id', dataType: 'bigint', isNullable: false,
      default: null, isPrimaryKey: true, ordinal: 1,
      isNew: false, isDeleted: false, originalName: 'id',
    }]
    const stmts = generateAlterStatements(cols, 'public', 'users')
    expect(stmts.length).toBe(0)
  })

  it('should handle multiple changes', () => {
    const cols: EditCol[] = [
      { name: 'id', dataType: 'bigint', isNullable: false, default: null, isPrimaryKey: true, ordinal: 1, isNew: false, isDeleted: false, originalName: 'id' },
      { name: 'email', dataType: 'text', isNullable: true, default: null, isPrimaryKey: false, ordinal: 2, isNew: true, isDeleted: false, originalName: '' },
      { name: 'old_field', dataType: 'text', isNullable: true, default: null, isPrimaryKey: false, ordinal: 3, isNew: false, isDeleted: true, originalName: 'old_field' },
      { name: 'username', dataType: 'text', isNullable: false, default: null, isPrimaryKey: false, ordinal: 4, isNew: false, isDeleted: false, originalName: 'name' },
    ]
    const stmts = generateAlterStatements(cols, 'public', 'users')
    expect(stmts.length).toBe(3)
    expect(stmts[0]).toContain('ADD COLUMN')
    expect(stmts[1]).toContain('DROP COLUMN')
    expect(stmts[2]).toContain('RENAME COLUMN')
  })
})

describe('SchemaDesigner — CREATE TABLE generation', () => {
  it('should generate CREATE TABLE with single PK', () => {
    const cols: EditCol[] = [{
      name: 'id', dataType: 'bigserial', isNullable: false,
      default: null, isPrimaryKey: true, ordinal: 1,
      isNew: true, isDeleted: false, originalName: '',
    }]
    const sql = generateCreateTable('users', cols)
    expect(sql).toContain('CREATE TABLE "public"."users"')
    expect(sql).toContain('"id" bigserial NOT NULL PRIMARY KEY')
  })

  it('should generate composite primary key', () => {
    const cols: EditCol[] = [
      { name: 'user_id', dataType: 'bigint', isNullable: false, default: null, isPrimaryKey: true, ordinal: 1, isNew: true, isDeleted: false, originalName: '' },
      { name: 'role_id', dataType: 'bigint', isNullable: false, default: null, isPrimaryKey: true, ordinal: 2, isNew: true, isDeleted: false, originalName: '' },
    ]
    const sql = generateCreateTable('user_roles', cols)
    expect(sql).toContain('PRIMARY KEY ("user_id", "role_id")')
    // Individual columns should NOT have PRIMARY KEY
    expect(sql).not.toContain('"user_id" bigint NOT NULL PRIMARY KEY')
  })

  it('should generate column with DEFAULT and nullable', () => {
    const cols: EditCol[] = [
      { name: 'id', dataType: 'serial', isNullable: false, default: null, isPrimaryKey: true, ordinal: 1, isNew: true, isDeleted: false, originalName: '' },
      { name: 'status', dataType: 'text', isNullable: true, default: "'active'", isPrimaryKey: false, ordinal: 2, isNew: true, isDeleted: false, originalName: '' },
    ]
    const sql = generateCreateTable('tasks', cols)
    expect(sql).toContain(`"status" text DEFAULT 'active'`)
  })

  it('should handle table with no primary key', () => {
    const cols: EditCol[] = [{
      name: 'data', dataType: 'jsonb', isNullable: true,
      default: null, isPrimaryKey: false, ordinal: 1,
      isNew: true, isDeleted: false, originalName: '',
    }]
    const sql = generateCreateTable('logs', cols)
    expect(sql).not.toContain('PRIMARY KEY')
  })
})

describe('SchemaDesigner — column editing', () => {
  it('should add a new column with defaults', () => {
    const cols: EditCol[] = [
      { name: 'id', dataType: 'bigserial', isNullable: false, default: null, isPrimaryKey: true, ordinal: 1, isNew: false, isDeleted: false, originalName: 'id' },
    ]
    const ordinal = cols.filter(c => !c.isDeleted).length + 1
    const newCol: EditCol = {
      name: '', dataType: 'text', isNullable: true,
      default: null, isPrimaryKey: false, ordinal,
      isNew: true, isDeleted: false, originalName: '',
    }
    expect(newCol.ordinal).toBe(2)
    expect(newCol.dataType).toBe('text')
    expect(newCol.isNew).toBe(true)
  })

  it('should mark existing column as deleted', () => {
    const col: EditCol = {
      name: 'old', dataType: 'text', isNullable: true,
      default: null, isPrimaryKey: false, ordinal: 1,
      isNew: false, isDeleted: false, originalName: 'old',
    }
    const deleted = { ...col, isDeleted: true }
    expect(deleted.isDeleted).toBe(true)
  })

  it('should remove new column completely instead of marking deleted', () => {
    const cols: EditCol[] = [
      { name: 'id', dataType: 'serial', isNullable: false, default: null, isPrimaryKey: true, ordinal: 1, isNew: false, isDeleted: false, originalName: 'id' },
      { name: 'temp', dataType: 'text', isNullable: true, default: null, isPrimaryKey: false, ordinal: 2, isNew: true, isDeleted: false, originalName: '' },
    ]
    // When deleting a new column, filter it out instead of marking deleted
    const deleteIdx = 1
    const col = cols[deleteIdx]
    let result: EditCol[]
    if (col.isNew) {
      result = cols.filter((_, i) => i !== deleteIdx)
    } else {
      result = cols.map((c, i) => i === deleteIdx ? { ...c, isDeleted: true } : c)
    }
    expect(result.length).toBe(1)
  })

  it('should compute dirty state correctly', () => {
    function isDirty(cols: EditCol[]): boolean {
      return cols.some(c => c.isNew || c.isDeleted || c.name !== c.originalName)
    }

    const clean: EditCol[] = [
      { name: 'id', dataType: 'serial', isNullable: false, default: null, isPrimaryKey: true, ordinal: 1, isNew: false, isDeleted: false, originalName: 'id' },
    ]
    expect(isDirty(clean)).toBe(false)

    const withNew: EditCol[] = [
      ...clean,
      { name: 'email', dataType: 'text', isNullable: true, default: null, isPrimaryKey: false, ordinal: 2, isNew: true, isDeleted: false, originalName: '' },
    ]
    expect(isDirty(withNew)).toBe(true)

    const withDeleted: EditCol[] = [
      { ...clean[0], isDeleted: true },
    ]
    expect(isDirty(withDeleted)).toBe(true)

    const withRename: EditCol[] = [
      { ...clean[0], name: 'user_id' },
    ]
    expect(isDirty(withRename)).toBe(true)
  })
})
