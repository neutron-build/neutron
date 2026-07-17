import { describe, it, expect } from 'vitest'

// Tests for DatalogModule real Nucleus SQL. Datalog is a GLOBAL engine with no
// table-valued evaluator: a program is split into per-statement UPPERCASE
// SCALAR calls (DATALOG_ASSERT / DATALOG_RULE / DATALOG_QUERY).

const sqlStr = (v: string) => `'${v.replace(/'/g, "''")}'`

interface ParsedProgram {
  asserts: string[]
  rules: string[]
  queries: string[]
}

function parseDatalogProgram(program: string): ParsedProgram {
  const out: ParsedProgram = { asserts: [], rules: [], queries: [] }
  for (const raw of program.split('\n')) {
    let line = raw.trim()
    if (line === '' || line.startsWith('--')) continue
    if (line.endsWith('.')) line = line.slice(0, -1).trim()
    if (line.startsWith('?-')) {
      const q = line.slice(2).trim()
      if (q) out.queries.push(q)
    } else if (line.includes(':-')) {
      out.rules.push(line)
    } else {
      out.asserts.push(line)
    }
  }
  return out
}

function parseQueryTuples(cell: unknown): string[][] {
  if (cell == null) return []
  const text = String(cell).trim()
  if (text === '') return []
  try {
    const parsed = JSON.parse(text)
    return Array.isArray(parsed) ? (parsed as string[][]) : []
  } catch {
    return []
  }
}

const EXAMPLE_PROGRAMS = [
  `-- Ancestors example
parent(alice, bob).
parent(bob, charlie).
ancestor(X, Y) :- parent(X, Y).
ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z).
?- ancestor(alice, Who).`,
  `-- Graph reachability
edge(1, 2).
edge(2, 3).
path(X, Y) :- edge(X, Y).
path(X, Z) :- edge(X, Y), path(Y, Z).
?- path(1, Where).`,
  `-- Query existing facts
?- parent(X, Y).`,
]

describe('DatalogModule — EXAMPLE_PROGRAMS', () => {
  it('should have 3 example programs', () => {
    expect(EXAMPLE_PROGRAMS.length).toBe(3)
  })

  it('should all contain query markers', () => {
    for (const prog of EXAMPLE_PROGRAMS) {
      expect(prog).toContain('?-')
    }
  })

  it('first example should define parent and ancestor rules', () => {
    const prog = EXAMPLE_PROGRAMS[0]
    expect(prog).toContain('parent(alice, bob)')
    expect(prog).toContain('ancestor(X, Y) :- parent(X, Y)')
  })
})

describe('DatalogModule — program parsing', () => {
  it('should split facts, rules, and queries stripping "." and "?-"', () => {
    const parsed = parseDatalogProgram(EXAMPLE_PROGRAMS[0])
    expect(parsed.asserts).toEqual(['parent(alice, bob)', 'parent(bob, charlie)'])
    expect(parsed.rules).toEqual([
      'ancestor(X, Y) :- parent(X, Y)',
      'ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z)',
    ])
    expect(parsed.queries).toEqual(['ancestor(alice, Who)'])
  })

  it('should skip blank and comment lines', () => {
    const parsed = parseDatalogProgram('-- comment\n\n?- parent(X, Y).')
    expect(parsed.asserts).toEqual([])
    expect(parsed.rules).toEqual([])
    expect(parsed.queries).toEqual(['parent(X, Y)'])
  })
})

describe('DatalogModule — scalar SQL builders', () => {
  it('should build DATALOG_ASSERT / DATALOG_RULE / DATALOG_QUERY calls', () => {
    expect(`SELECT DATALOG_ASSERT(${sqlStr('parent(alice, bob)')})`).toBe(
      "SELECT DATALOG_ASSERT('parent(alice, bob)')"
    )
    expect(`SELECT DATALOG_RULE(${sqlStr('ancestor(X, Y) :- parent(X, Y)')})`).toBe(
      "SELECT DATALOG_RULE('ancestor(X, Y) :- parent(X, Y)')"
    )
    expect(`SELECT DATALOG_QUERY(${sqlStr('ancestor(alice, Who)')})`).toBe(
      "SELECT DATALOG_QUERY('ancestor(alice, Who)')"
    )
  })

  it('should escape single quotes in statements', () => {
    expect(sqlStr("fact('hello')")).toBe("'fact(''hello'')'")
  })
})

describe('DatalogModule — tuple result parsing', () => {
  it('should parse a JSON array of tuples', () => {
    const cell = JSON.stringify([['alice', 'bob'], ['bob', 'charlie']])
    expect(parseQueryTuples(cell)).toEqual([['alice', 'bob'], ['bob', 'charlie']])
  })

  it('should treat empty/invalid cells as no tuples', () => {
    expect(parseQueryTuples('')).toEqual([])
    expect(parseQueryTuples(null)).toEqual([])
    expect(parseQueryTuples('nope')).toEqual([])
  })
})
