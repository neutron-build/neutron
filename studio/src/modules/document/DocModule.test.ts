import { describe, it, expect } from 'vitest'

// Tests for DocModule utility functions: previewDoc, parsePath, setNestedValue

function previewDoc(data: unknown): string {
  if (!data || typeof data !== 'object') return String(data)
  const keys = Object.keys(data as object)
  return keys.slice(0, 3).join(', ') + (keys.length > 3 ? '...' : '')
}

function parsePath(path: string): (string | number)[] {
  const parts: (string | number)[] = []
  let p = path.startsWith('$') ? path.slice(1) : path
  const regex = /\.([^.[]+)|\[(\d+)\]/g
  let match: RegExpExecArray | null
  while ((match = regex.exec(p)) !== null) {
    if (match[1] !== undefined) {
      parts.push(match[1])
    } else if (match[2] !== undefined) {
      parts.push(parseInt(match[2], 10))
    }
  }
  return parts
}

function setNestedValue(obj: unknown, path: string, value: unknown): void {
  const parts = parsePath(path)
  if (parts.length === 0) return

  let current: unknown = obj
  for (let i = 0; i < parts.length - 1; i++) {
    const part = parts[i]
    if (typeof part === 'number' && Array.isArray(current)) {
      current = current[part]
    } else if (typeof part === 'string' && current && typeof current === 'object') {
      current = (current as Record<string, unknown>)[part]
    } else {
      return
    }
  }

  const last = parts[parts.length - 1]
  if (typeof last === 'number' && Array.isArray(current)) {
    current[last] = value
  } else if (typeof last === 'string' && current && typeof current === 'object') {
    (current as Record<string, unknown>)[last] = value
  }
}

describe('DocModule — previewDoc', () => {
  it('should return stringified non-object values', () => {
    expect(previewDoc(null)).toBe('null')
    expect(previewDoc(undefined)).toBe('undefined')
    expect(previewDoc(42)).toBe('42')
    expect(previewDoc('hello')).toBe('hello')
  })

  it('should show first 3 keys for objects', () => {
    const data = { name: 'Alice', age: 30, email: 'a@b.com' }
    expect(previewDoc(data)).toBe('name, age, email')
  })

  it('should append ... for objects with more than 3 keys', () => {
    const data = { a: 1, b: 2, c: 3, d: 4 }
    expect(previewDoc(data)).toBe('a, b, c...')
  })

  it('should handle empty object', () => {
    expect(previewDoc({})).toBe('')
  })

  it('should handle single-key object', () => {
    expect(previewDoc({ id: 1 })).toBe('id')
  })
})

describe('DocModule — parsePath', () => {
  it('should parse root-only path', () => {
    expect(parsePath('$')).toEqual([])
  })

  it('should parse simple key path', () => {
    expect(parsePath('$.name')).toEqual(['name'])
  })

  it('should parse nested path', () => {
    expect(parsePath('$.user.address.city')).toEqual(['user', 'address', 'city'])
  })

  it('should parse array index', () => {
    expect(parsePath('$.items[0]')).toEqual(['items', 0])
  })

  it('should parse mixed path', () => {
    expect(parsePath('$.users[2].name')).toEqual(['users', 2, 'name'])
  })

  it('should parse deeply nested path', () => {
    expect(parsePath('$.a.b[0].c[1].d')).toEqual(['a', 'b', 0, 'c', 1, 'd'])
  })

  it('should handle path without $ prefix', () => {
    expect(parsePath('.foo.bar')).toEqual(['foo', 'bar'])
  })
})

describe('DocModule — setNestedValue', () => {
  it('should set a top-level property', () => {
    const obj = { name: 'Alice' }
    setNestedValue(obj, '$.name', 'Bob')
    expect(obj.name).toBe('Bob')
  })

  it('should set a nested property', () => {
    const obj = { user: { address: { city: 'SF' } } }
    setNestedValue(obj, '$.user.address.city', 'NYC')
    expect(obj.user.address.city).toBe('NYC')
  })

  it('should set an array element', () => {
    const obj = { items: ['a', 'b', 'c'] }
    setNestedValue(obj, '$.items[1]', 'X')
    expect(obj.items[1]).toBe('X')
  })

  it('should set a nested array element property', () => {
    const obj = { users: [{ name: 'Alice' }, { name: 'Bob' }] }
    setNestedValue(obj, '$.users[0].name', 'Charlie')
    expect(obj.users[0].name).toBe('Charlie')
  })

  it('should do nothing for empty path', () => {
    const obj = { a: 1 }
    setNestedValue(obj, '$', 42)
    expect(obj).toEqual({ a: 1 })
  })

  it('should do nothing for invalid path', () => {
    const obj = { a: 1 }
    setNestedValue(obj, '$.nonexistent.deep.path', 42)
    expect(obj).toEqual({ a: 1 })
  })

  it('should handle setting null value', () => {
    const obj = { name: 'Alice' }
    setNestedValue(obj, '$.name', null)
    expect(obj.name).toBeNull()
  })

  it('should handle setting object value', () => {
    const obj: Record<string, unknown> = { data: 'old' }
    setNestedValue(obj, '$.data', { nested: true })
    expect(obj.data).toEqual({ nested: true })
  })
})
