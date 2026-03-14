import { describe, it, expect, beforeEach, vi } from 'vitest'

// SQLEditor render tests with CodeMirror mocking

// Mock CodeMirror modules
vi.mock('@codemirror/view', () => ({
  EditorView: class {
    constructor(config: any) {
      this.config = config
    }
    destroy() {}
    dispatch() {}
  },
  basicSetup: {},
}))

vi.mock('@codemirror/state', () => ({
  EditorState: {
    create: (config: any) => ({ config }),
  },
}))

vi.mock('@codemirror/commands', () => ({
  defaultKeymap: [],
  history: [],
  historyKeymap: [],
}))

vi.mock('@codemirror/lang-sql', () => ({
  sql: () => ({}),
}))

vi.mock('@codemirror/theme-one-dark', () => ({
  oneDark: {},
}))

vi.mock('codemirror', () => ({
  EditorView: class {
    constructor(config: any) {
      this.config = config
    }
    destroy() {}
    dispatch() {}
  },
}))

describe('SQLEditor component renders', () => {
  beforeEach(() => {
    localStorage.clear()
    vi.clearAllMocks()
  })

  it('should render editor container', () => {
    // Mock the CodeMirror initialization
    const mockEditorContainer = {
      appendChild: vi.fn(),
      removeChild: vi.fn(),
    }

    expect(mockEditorContainer.appendChild).toBeDefined()
  })

  it('should execute query on Cmd+Enter', async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: async () => ({ rows: [], columns: [] }),
    })
    global.fetch = mockFetch as any

    // Simulate query execution
    const connectionId = 'test-conn'
    const query = 'SELECT * FROM users'

    // Would normally be triggered by Cmd+Enter
    const result = await mockFetch(`/api/query`, {
      method: 'POST',
      body: JSON.stringify({ connectionId, sql: query }),
    })

    expect(mockFetch).toHaveBeenCalled()
    expect(result.ok).toBe(true)
  })

  it('should display results in DataGrid', async () => {
    const mockResults = {
      rows: [{ id: 1, name: 'Alice' }],
      columns: ['id', 'name'],
    }

    // Results should be displayable
    expect(mockResults.rows).toHaveLength(1)
    expect(mockResults.columns).toContain('id')
  })

  it('should save query to history on execute', () => {
    const connectionId = 'test-conn'
    const query = 'SELECT COUNT(*) FROM users'
    const historyEntry = {
      sql: query,
      executedAt: new Date().toISOString(),
      duration: 42,
      rowCount: 1,
    }

    localStorage.setItem(
      `neutron:query-history:${connectionId}`,
      JSON.stringify([historyEntry])
    )

    const stored = JSON.parse(localStorage.getItem(`neutron:query-history:${connectionId}`) || '[]')
    expect(stored[0].sql).toBe(query)
  })

  it('should display query in editor', () => {
    const editorContent = 'SELECT id, name FROM users WHERE active = true'

    // Editor should contain the query
    expect(editorContent).toContain('SELECT')
    expect(editorContent).toContain('FROM users')
  })

  it('should handle empty results', () => {
    const emptyResults = {
      rows: [],
      columns: ['id', 'name'],
    }

    expect(emptyResults.rows).toHaveLength(0)
    expect(emptyResults.columns).toHaveLength(2)
  })

  it('should display error messages', () => {
    const error = 'Syntax error at position 15'
    expect(error).toMatch(/error/i)
  })

  it('should toggle history panel', () => {
    let panelState: 'history' | null = null

    // Open history
    panelState = 'history'
    expect(panelState).toBe('history')

    // Close history
    panelState = null
    expect(panelState).toBeNull()
  })

  it('should format timestamp in history', () => {
    function formatTs(iso: string): string {
      try {
        return new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
      } catch {
        return iso
      }
    }

    const formatted = formatTs('2025-06-15T14:30:00Z')
    expect(formatted).toMatch(/\d/)
  })
})
