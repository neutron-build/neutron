import { beforeEach, afterEach, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import { activeConnection } from '../lib/store'
import { GraphModule } from './graph/GraphModule'
import { FTSModule } from './fts/FTSModule'
import { GeoModule } from './geo/GeoModule'
import { TableSearchPanel } from './sql/TableSearchPanel'
import { TSModule, chartValue } from './timeseries/TSModule'
vi.mock('../lib/api', async original => { const m = await original<typeof import('../lib/api')>(); return { ...m, api: { ...m.api, query: vi.fn(), tableSearch: vi.fn() } } })
import { api } from '../lib/api'
const ok = (cell: unknown) => ({ columns: ['v'], rows: [[cell]], rowCount: 1, duration: 0 })
beforeEach(() => { activeConnection.value = {id:'c1', name:'one', url:'pg://one', isNucleus:true}; vi.mocked(api.query).mockReset(); vi.mocked(api.tableSearch).mockReset() })
afterEach(cleanup)
it('FTS results cannot publish after a search draft A to B to A change', async () => {
  let finish!: (r: ReturnType<typeof ok>) => void
  vi.mocked(api.query).mockImplementation(async sql => sql.includes('FTS_DOC_COUNT') ? ok(1) : new Promise(resolve => { finish = resolve }))
  render(<FTSModule name="fts" />)
  const input = screen.getByPlaceholderText('Search documents…')
  fireEvent.input(input, {target:{value:'first'}}); fireEvent.keyDown(input, {key:'Enter'})
  await waitFor(() => expect(finish).toBeTypeOf('function'))
  fireEvent.input(input, {target:{value:'newer'}})
  fireEvent.input(input, {target:{value:'first'}})
  finish(ok(JSON.stringify([{doc_id:'old-hit',score:1}])))
  await new Promise(resolve => setTimeout(resolve, 0))
  expect(screen.queryByText('old-hit')).toBeNull()
  expect(screen.getByDisplayValue('first')).toBeTruthy()
})
it('Geo response cannot publish after a connection switch', async () => {
  let finish!: (r: ReturnType<typeof ok>) => void
  vi.mocked(api.query).mockImplementation(() => new Promise(resolve => { finish = resolve }))
  render(<GeoModule name="geo" />)
  fireEvent.click(screen.getByText('Compute', {selector:'button'}))
  await waitFor(() => expect(finish).toBeTypeOf('function'))
  activeConnection.value = { id:'c2', name:'two', url:'pg://two', isNucleus:true }
  finish(ok(1234567))
  await new Promise(resolve => setTimeout(resolve, 0))
  expect(document.body.textContent).not.toContain('1234567')
})
it('table search loses publication ownership when its input changes', async () => {
  let finish!: (r: ReturnType<typeof ok>) => void
  vi.mocked(api.tableSearch).mockImplementation(() => new Promise(resolve => { finish = resolve }))
  render(<TableSearchPanel schema="public" table="t" meta={{binding:'e:1', columns:[{name:'body',type:'text'}]} as never} />)
  const input = screen.getByPlaceholderText('search terms, e.g. "exact phrase" OR vector')
  fireEvent.input(input, {target:{value:'first'}}); fireEvent.keyDown(input, {key:'Enter'})
  await waitFor(() => expect(finish).toBeTypeOf('function'))
  fireEvent.input(input, {target:{value:'newer'}})
  finish(ok('obsolete-table-hit'))
  await new Promise(resolve => setTimeout(resolve, 0))
  expect(document.body.textContent).not.toContain('obsolete-table-hit')
})
it('time-series missing/invalid buckets stay nullable; zero is a real value', () => {
  expect([null, undefined, '', ' ', [], {}, NaN, Infinity, 0, '2'].map(chartValue)).toEqual([null,null,null,null,null,null,null,null,0,2])
})
// Rendered TS read/ingest scheduling is qualified with the shared owner and
// production helper above; live Plot gap rendering remains a browser gate.
it('time-series ingest does not clear a newer draft after acknowledged success', async () => {
  let finish!: (r: ReturnType<typeof ok>) => void
  vi.mocked(api.query).mockImplementation(async sql => sql.includes('TS_INSERT') ? new Promise(resolve => {finish=resolve}) : {...ok(0),rows:[[0,null]]})
  render(<TSModule name="series" />)
  const textarea = document.querySelector('textarea')!
  fireEvent.input(textarea, {target:{value:'1, 2'}})
  fireEvent.click(screen.getByText('Insert', {selector:'button'}))
  await waitFor(() => expect(finish).toBeTypeOf('function'))
  fireEvent.input(textarea, {target:{value:'3, 4'}})
  finish(ok(1))
  await new Promise(resolve => setTimeout(resolve, 0))
  expect(screen.getByDisplayValue('3, 4')).toBeTruthy()
})

it('graph results above the layout budget stay in the table', async () => {
  vi.mocked(api.query).mockImplementation(async sql => sql.includes('GRAPH_QUERY') ? ok(JSON.stringify({columns:['id'],rows:Array.from({length:201},(_,i)=>[String(i)])})) : {...ok(201),rows:[[201,0]]})
  const view = render(<GraphModule name="graph" />)
  fireEvent.click(screen.getByText('Run', {selector:'button'}))
  await screen.findByText(/Graph layout is limited to 200 nodes and 1000 edges/)
  expect(screen.queryByText('Graph', {selector:'button'})).toBeNull()
  expect(view.container.querySelector('svg[class*="graphSvg"]')).toBeNull()
})
