import { describe, it, expect, vi } from 'vitest'
import { decodeRows, formatCell, encodeCell, type WireTag } from './wire'
import { parseDeepLink } from './router'
import { queryMutationOrThrow } from './api'
import { autoMap, encodeRecord } from './importer'
import { buildCompletionNamespace, parameterCount, MAX_PARAMETERS, loadHistory, historyKey } from '../modules/sql/sqlTools'
import { validatedVector, MAX_VECTOR_DIMENSIONS } from '../modules/vector/VectorModule'

describe('final wire and admission contracts', () => {
  it.each([
    ['int8', '9007199254740993'], ['numeric', '0.1234567890123456789'],
    ['date', '2026-10-07'], ['timestamp', '2026-10-07T12:00:00.123456'],
    ['timestamptz', '2026-10-07T12:00:00Z'], ['bytea', 'aabb'], ['json', '{"a":9007199254740993}'],
    ['jsonb', '{"a":9007199254740993}'], ['vector', '[1, 0.123456789]'], ['tsvector', "'hello':1"],
  ] satisfies [WireTag, string][])('round trips %s through row decode and grid formatting', (tag, raw) => {
    const decoded = decodeRows([[{ t: tag, v: raw }]])[0][0]
    expect(decoded).not.toBeUndefined()
    expect(encodeCell(decoded, tag)).toEqual({ t: tag, v: raw })
    expect(formatCell(decoded)).toBe(tag === 'bytea' ? '\\x' + raw : raw)
  })
  it.each(['%', '%2', '%C0%AF', '%E0%A4%A'])('handles malformed deep link %s', value => {
    expect(parseDeepLink(`#/c/${value}/sql`)).toBeNull()
  })
  it.each([{error:'denied'}, {canceled:true}, {error:'denied', canceled:true}])('refuses resolved mutation failure %o', async extra => {
    const query = vi.fn().mockResolvedValue({ columns: [], rows: [], rowCount: 0, duration: 0, ...extra })
    await expect(queryMutationOrThrow('SELECT write()', 'c', undefined, query)).rejects.toThrow(extra.error ?? 'canceled')
    expect(query).toHaveBeenCalledTimes(1)
  })
  it.each(['$0', '$1025', '$9007199254740993', '$' + '9'.repeat(400)])('rejects unsafe positional token %s', sql => {
    expect(() => parameterCount(sql)).toThrow('Parameter index')
    expect(parameterCount(`SELECT '${sql}', "${sql}", $$${sql}$$ /* ${sql} */ -- ${sql}`)).toBe(0)
  })
  it('admits the exact UI bound without sparse unbounded allocation', () => expect(parameterCount(`SELECT $${MAX_PARAMETERS}`)).toBe(MAX_PARAMETERS))
  it.each(['[]', '["1"]', '[null]', '[1e999]', "[1'); SELECT 1; --]", '[1,]', '{}'])('rejects vector payload %s', raw => {
    expect(() => validatedVector(raw)).toThrow()
  })
  it('enforces vector dimensions and finite numeric values', () => {
    expect(validatedVector('[1,2.5,-3e2]')).toBe('[1,2.5,-300]')
    expect(() => validatedVector(JSON.stringify(Array(MAX_VECTOR_DIMENSIONS+1).fill(0)))).toThrow()
    expect(validatedVector(JSON.stringify(Array(MAX_VECTOR_DIMENSIONS).fill(0)))).toBeTruthy()
  })
  it('rejects malformed loaded history entries', () => {
    localStorage.setItem(historyKey('final'), JSON.stringify([null, {}, {sql: 4}, {sql: 'a', params: [4]}]))
    expect(loadHistory('final')).toEqual([])
  })
  it('keeps prototype names as exact own identifier keys', () => {
    const names = ['__proto__','constructor','toString']
    const columns = names.map(name => ({ name, type:'text', nullable:true, tag:null, default:null })) as never
    const fields = names.map((name,i) => ({id:String(i),label:name}))
    const mapping = autoMap(fields, columns)
    expect(Object.keys(mapping)).toEqual(names)
    const record = {row:1,line:1,values:new Map(fields.map(f => [f.id,{kind:'text',text:f.label,quoted:false}]))} as never
    const encoded = encodeRecord(record, columns, mapping, {emptyUnquoted:'null',nullMarker:null})
    expect(Object.keys(JSON.parse(JSON.stringify(encoded)))).toEqual(names)
    const ns = buildCompletionNamespace(names.map(name => ({schema:name,name,columns:[]}))) as Record<string,unknown>
    expect(Object.keys(ns)).toEqual(names)
    expect(Object.getPrototypeOf(ns)).toBeNull()
    expect(Object.prototype).not.toHaveProperty('polluted')
  })
})

it('prototype column names retain tagged, JSON, and null values through import JSON output',()=>{
 const columns=[{name:'__proto__',type:'bigint',tag:'int8',nullable:true},{name:'constructor',type:'jsonb',tag:'jsonb',nullable:true},{name:'toString',type:'text',tag:null,nullable:true}] as never
 const fields=['__proto__','constructor','toString'].map(name=>({id:name,label:name}))
 const mapping=autoMap(fields,columns)
 const record={row:1,line:1,values:new Map([
  ['__proto__',{kind:'text',text:'9007199254740993',quoted:false}],
  ['constructor',{kind:'text',text:'{"n":9007199254740993}',quoted:false}],
 ])} as never
 mapping.toString={kind:'null'}
 const encoded=encodeRecord(record,columns,mapping,{emptyUnquoted:'null',nullMarker:null})
 expect(JSON.parse(JSON.stringify(encoded))).toEqual(JSON.parse('{"__proto__":{"t":"int8","v":"9007199254740993"},"constructor":"{\\"n\\":9007199254740993}","toString":null}'))
})
