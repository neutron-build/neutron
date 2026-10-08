import { it, expect, vi, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup } from '@testing-library/preact'
import { api, _setSessionTokenForTests } from '../lib/api'
import { DataGrid } from './DataGrid'
import type { TableMetaColumn } from '../lib/types'
afterEach(()=>{cleanup();vi.unstubAllGlobals();_setSessionTokenForTests(null)})
it('API tagged vector/tsvector rows arrive unchanged at grid copy affordances',async()=>{
 _setSessionTokenForTests('test-token')
 const vector='[0.10000000000000001,2,3]',tsvector="'cat':1 'sat':2"
 const fetch=vi.fn().mockResolvedValue({ok:true,json:async()=>({columns:['embedding','keywords'],rows:[[{t:'vector',v:vector},{t:'tsvector',v:tsvector}]],rowCount:1,duration:0})})
 vi.stubGlobal('fetch',fetch)
 const result=await api.query('SELECT embedding, keywords','c')
 const columns=[{name:'embedding',type:'vector',tag:'vector',editable:false},{name:'keywords',type:'tsvector',tag:'tsvector',editable:false}] as TableMetaColumn[]
 const writeText=vi.fn().mockResolvedValue(undefined)
 Object.defineProperty(navigator,'clipboard',{value:{writeText},configurable:true})
 render(<DataGrid result={result} columns={columns}/>)
 await fireEvent.click(screen.getByTitle('Copy vector value'))
 await fireEvent.click(screen.getByTitle('Copy tsvector value'))
 expect(writeText.mock.calls).toEqual([[vector],[tsvector]])
 expect(fetch.mock.calls[0][1].headers['X-Studio-Session']).toBe('test-token')
})
