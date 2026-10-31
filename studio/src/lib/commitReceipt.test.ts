import { describe, it, expect } from 'vitest'
import { validateCommitReceipt, validateCommitOutcome } from './commitReceipt'
import { commitReceiptFor } from './commitFixture'
const operations = [{ op: 'insert', schema: 'public', table: 't', binding: 'e:1', values: { a: 'b' } }] as const
const good = () => commitReceiptFor('requested', operations)
describe('runtime authoritative receipt boundary', () => {
  it.each([null, {}, [], { ...good(), operationId: 'other' }, { ...good(), reversible: 'yes' }, { ...good(), rowsAffected: -1 }, { ...good(), rowsAffected: 9007199254740992 }, { ...good(), operations: [] }, { ...good(), operations: [{ index: 1, op: 'insert', rowsAffected: 1 }] }, { ...good(), operations: [{ index: 0, op: 'unknown', rowsAffected: 1 }] }, { ...good(), operations: [{ index: 0, op: 'insert', rowsAffected: 1, key: [{ column: 'id', value: {} }] }] }])('refuses invalid direct response %j', body => {
    expect(() => validateCommitReceipt(body, 'requested', operations)).toThrow('receipt')
  })
  it('admits a matching exact tagged key and safe result', () => {
    const response = good(); response.operations[0].key = [{ column: 'id', value: { t: 'int8', v: '9007199254740993' } }]
    expect(validateCommitReceipt(response, 'requested', operations)).toBe(response)
  })
  it.each([{}, null, { operationId: 'other', state: 'failed', status: 409, response: { operationId: 'other', error: 'no' } }, { operationId: 'requested', state: 'failed', status: 409, response: { error: 'no' } }, { operationId: 'requested', state: 'failed', status: 409, response: { operationId: 'other', error: 'no' } }, { operationId: 'requested', state: 'committed', response: { ...good(), operationId: 'other' } }])('refuses invalid outcome %j', body => {
    expect(() => validateCommitOutcome(body, 'requested', operations)).toThrow('receipt')
  })
  it('validates matching terminal refusal before releasing identity', () => {
    expect(validateCommitOutcome({ operationId: 'requested', state: 'failed', status: 409, response: { operationId: 'requested', error: 'conflict' } }, 'requested').error).toBe('conflict')
  })
})
