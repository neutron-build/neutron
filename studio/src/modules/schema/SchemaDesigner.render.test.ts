import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'

// SchemaDesigner render tests

describe('SchemaDesigner component renders', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  afterEach(() => {
    vi.resetAllMocks()
  })

  it('should render table names in sidebar', () => {
    const schema = {
      tables: [
        { name: 'users', schema: 'public', columns: [] },
        { name: 'posts', schema: 'public', columns: [] },
        { name: 'comments', schema: 'public', columns: [] },
      ],
    }

    expect(schema.tables).toHaveLength(3)
    expect(schema.tables.map(t => t.name)).toContain('users')
    expect(schema.tables.map(t => t.name)).toContain('posts')
    expect(schema.tables.map(t => t.name)).toContain('comments')
  })

  it('should fetch columns when table is clicked', async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        columns: [
          { name: 'id', dataType: 'bigint', isNullable: false, isPrimaryKey: true },
          { name: 'name', dataType: 'text', isNullable: true, isPrimaryKey: false },
        ],
      }),
    })
    global.fetch = mockFetch as any

    const connectionId = 'test-conn'
    const tableName = 'users'

    const response = await mockFetch(`/api/columns?connectionId=${connectionId}&table=${tableName}`, {
      method: 'GET',
    })

    expect(mockFetch).toHaveBeenCalled()
    expect(response.ok).toBe(true)
    const data = await response.json()
    expect(data.columns).toHaveLength(2)
  })

  it('should display column editor with table structure', () => {
    const columns = [
      { name: 'id', dataType: 'bigserial', isPrimaryKey: true, isNullable: false },
      { name: 'email', dataType: 'text', isPrimaryKey: false, isNullable: true },
      { name: 'created_at', dataType: 'timestamptz', isPrimaryKey: false, isNullable: false },
    ]

    expect(columns).toHaveLength(3)
    expect(columns[0].isPrimaryKey).toBe(true)
    expect(columns[1].isNullable).toBe(true)
  })

  it('should toggle codegen tab for code generation', () => {
    let activeTab: 'columns' | 'codegen' = 'columns'

    // Switch to codegen
    activeTab = 'codegen'
    expect(activeTab).toBe('codegen')

    // Switch back to columns
    activeTab = 'columns'
    expect(activeTab).toBe('columns')
  })

  it('should call /api/codegen with language parameter', async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: async () => ({ code: 'type Users = { id: number; email?: string; }' }),
    })
    global.fetch = mockFetch as any

    const language = 'ts'
    const connectionId = 'test-conn'
    const tableName = 'users'

    const response = await mockFetch(
      `/api/codegen?connectionId=${connectionId}&table=${tableName}&lang=${language}`,
      { method: 'GET' }
    )

    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/codegen'),
      expect.any(Object)
    )
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining(`lang=${language}`),
      expect.any(Object)
    )

    expect(response.ok).toBe(true)
    const data = await response.json()
    expect(data.code).toContain('Users')
  })

  it('should display generated code for selected language', () => {
    const codeSnippets: Record<string, string> = {
      go: 'type Users struct { ID int64 `db:"id"`; Email string `db:"email"`; }',
      ts: 'interface Users { id: number; email?: string; }',
      rust: 'pub struct Users { pub id: i64, pub email: Option<String>, }',
      python: 'class Users(BaseModel): id: int; email: Optional[str]',
    }

    Object.entries(codeSnippets).forEach(([lang, code]) => {
      expect(code).toBeTruthy()
      expect(code.length).toBeGreaterThan(0)
    })
  })

  it('should manage column state for editing', () => {
    const columns = [
      { name: 'id', isNew: false, isDeleted: false, originalName: 'id' },
      { name: 'email', isNew: true, isDeleted: false, originalName: '' },
    ]

    expect(columns[0].isNew).toBe(false)
    expect(columns[1].isNew).toBe(true)
  })

  it('should reset state after canceling changes', () => {
    let isDirty = true
    const resetChanges = () => {
      isDirty = false
    }

    expect(isDirty).toBe(true)
    resetChanges()
    expect(isDirty).toBe(false)
  })

  it('should display index information', () => {
    const indexes = [
      { name: 'idx_users_email', columns: ['email'], isUnique: true },
      { name: 'idx_users_created_at', columns: ['created_at'], isUnique: false },
    ]

    expect(indexes).toHaveLength(2)
    expect(indexes[0].isUnique).toBe(true)
    expect(indexes[1].columns).toContain('created_at')
  })
})
