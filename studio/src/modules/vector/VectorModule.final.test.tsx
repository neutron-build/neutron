import { beforeEach, afterEach, describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import { VectorModule } from './VectorModule'
import { activeConnection } from '../../lib/store'
vi.mock('../../lib/api', async original=>{const m=await original<typeof import('../../lib/api')>();return {...m,api:{...m.api,query:vi.fn(),cancelQuery:vi.fn()}}})
import { api } from '../../lib/api'
const query=vi.mocked(api.query)
beforeEach(()=>{ activeConnection.value={id:'c',name:'test',url:'pg://test',isNucleus:true};query.mockReset();vi.mocked(api.cancelQuery).mockReset();vi.mocked(api.cancelQuery).mockResolvedValue({} as never);query.mockResolvedValue({columns:[],rows:[],rowCount:0,duration:0}) })
afterEach(cleanup)
describe('vector actual search handler',()=>{
  it.each(["[1'); SELECT 1; --]",'[]','["1"]','[1e999]'])('rejects malformed vector %s before query transport',async raw=>{
    render(<VectorModule name="vectors" />)
    await waitFor(()=>expect(query).toHaveBeenCalledTimes(1));query.mockClear()
    await fireEvent.input(screen.getByPlaceholderText('[0.1, 0.2, 0.3, ...]'),{target:{value:raw}})
    await fireEvent.click(screen.getByText('Search'))
    expect(query).not.toHaveBeenCalled()
  })
  it('binds canonical data, clamps k, and shows resolved query errors instead of samples',async()=>{
    render(<VectorModule name="vectors" />)
    await waitFor(()=>expect(query).toHaveBeenCalled());query.mockClear()
    query.mockResolvedValue({columns:[],rows:[],rowCount:0,duration:0,error:'dimension mismatch'})
    await fireEvent.input(screen.getByPlaceholderText('[0.1, 0.2, 0.3, ...]'),{target:{value:'[1.0, 2e0]'}})
    await fireEvent.input(document.querySelector('input[type="number"]')!,{target:{value:'999999'}})
    await fireEvent.click(screen.getByText('Search'))
    await screen.findByText('dimension mismatch')
    expect(query.mock.calls[0][0]).toContain('VECTOR($1)')
    expect(query.mock.calls[0][0]).toContain('LIMIT 1000')
    expect(query.mock.calls[0][2]).toEqual(['[1,2]'])
    expect(screen.queryByText('Stored vectors (sample)')).toBeNull()
  })
})

it('cancels the original request and ignores its late result after target changes', async () => {
  let finish!: (v: never) => void
  query.mockImplementation(async sql => sql.includes('VECTOR_DISTANCE') ? new Promise(resolve => { finish = resolve }) : { columns: [], rows: [], rowCount: 0, duration: 0 })
  render(<VectorModule name="vectors" />)
  await waitFor(() => expect(query).toHaveBeenCalled())
  await fireEvent.input(screen.getByPlaceholderText('[0.1, 0.2, 0.3, ...]'), { target: { value: '[1,2]' } })
  await fireEvent.click(screen.getByText('Search'))
  await waitFor(() => expect(finish).toBeTypeOf('function'))
  const call = query.mock.calls.find(([sql]) => sql.includes('VECTOR_DISTANCE'))!
  await fireEvent.click(screen.getByText('Cancel search'))
  expect(api.cancelQuery).toHaveBeenCalledWith('c', call[3])
  await fireEvent.input(document.querySelector('input[class*="kInput"]')!, { target: { value: 'new_target' } })
  finish({ columns: [], rows: [], rowCount: 0, duration: 0, error: 'old search error' } as never)
  await new Promise(resolve => setTimeout(resolve, 20))
  expect(screen.queryByText('old search error')).toBeNull()
  expect(screen.getByText('Search')).toBeTruthy()
})
it('old sample cannot replace the new target sample', async () => {
  let finish!: (v: never) => void
  query.mockImplementationOnce(() => new Promise(resolve => { finish = resolve }))
  query.mockResolvedValue({ columns: [], rows: [], rowCount: 0, duration: 0, error: 'new sample error' })
  render(<VectorModule name="vectors" />)
  await waitFor(() => expect(finish).toBeTypeOf('function'))
  await fireEvent.input(document.querySelector('input[class*="kInput"]')!, { target: { value: 'new_target' } })
  await screen.findByText('new sample error')
  finish({ columns: [], rows: [], rowCount: 0, duration: 0, error: 'old sample error' } as never)
  await new Promise(resolve => setTimeout(resolve, 20))
  expect(screen.queryByText('old sample error')).toBeNull()
  expect(screen.getByText('new sample error')).toBeTruthy()
})
