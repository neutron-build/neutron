import { describe, it, expect } from 'vitest'
import { parseDatalogProgram, parseQueryTuples } from './DatalogModule'

// Production parser tests; operation ownership lives in the component suite.

describe('DatalogModule — program parsing', () => {
  it('should split facts, rules, and queries stripping "." and "?-"', () => {
    const parsed = parseDatalogProgram('parent(alice, bob).\nparent(bob, charlie).\nancestor(X, Y) :- parent(X, Y).\nancestor(X, Z) :- parent(X, Y), ancestor(Y, Z).\n?- ancestor(alice, Who).')
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
