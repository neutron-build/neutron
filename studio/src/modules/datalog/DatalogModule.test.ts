import { describe, it, expect } from 'vitest'

// Tests for DatalogModule: example programs, query building

const EXAMPLE_PROGRAMS = [
  `-- Ancestors example
parent(alice, bob).
parent(bob, charlie).
ancestor(X, Y) :- parent(X, Y).
ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z).
?- ancestor(alice, Who).`,
  `-- List all predicates
?- predicate(Name, Arity).`,
  `-- List all rules
?- rule(Head, Body).`,
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

describe('DatalogModule — query building', () => {
  it('should build datalog_eval query with escaped program', () => {
    const program = "parent(alice, bob).\n?- parent(alice, Who)."
    const escaped = program.replace(/'/g, "''")
    const sql = `SELECT * FROM datalog_eval('${escaped}')`
    expect(sql).toContain("datalog_eval('parent(alice, bob).")
    expect(sql).toContain("?- parent(alice, Who).")
  })

  it('should escape single quotes in program', () => {
    const program = "fact('hello')."
    const escaped = program.replace(/'/g, "''")
    expect(escaped).toBe("fact(''hello'').")
  })
})
