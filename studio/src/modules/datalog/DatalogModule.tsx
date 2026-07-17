import { useSignal } from '@preact/signals'
import { useEffect } from 'preact/hooks'
import { activeConnection, schema, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { DataGrid } from '../../components/DataGrid'
import type { QueryResult } from '../../lib/types'
import s from './DatalogModule.module.css'

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

const sqlStr = (v: string) => `'${v.replace(/'/g, "''")}'`

interface ParsedProgram {
  asserts: string[]
  rules: string[]
  queries: string[]
}

// Datalog is a GLOBAL engine. There is no table-valued evaluator; each line is
// a separate scalar call: facts → DATALOG_ASSERT, rules → DATALOG_RULE,
// `?-` goals → DATALOG_QUERY. Statement text carries no leading `?-` or
// trailing `.` when handed to the engine.
export function parseDatalogProgram(program: string): ParsedProgram {
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

// DATALOG_QUERY returns a JSON array of tuples (arrays of strings), e.g.
// [["alice","bob"],["bob","charlie"]].
export function parseQueryTuples(cell: unknown): string[][] {
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

export function tuplesToResult(tuples: string[][]): QueryResult {
  const maxArity = tuples.reduce((m, t) => Math.max(m, t.length), 0)
  const columns = Array.from({ length: maxArity }, (_, i) => `col${i}`)
  return { columns, rows: tuples, rowCount: tuples.length, duration: 0 }
}

export function DatalogModule() {
  const program = useSignal(EXAMPLE_PROGRAMS[0])
  const result = useSignal<QueryResult | null>(null)
  const running = useSignal(false)
  const stats = useSignal<{ predicates: number; rules: number } | null>(null)

  const conn = activeConnection.value!
  const dl = schema.value?.datalog

  useEffect(() => {
    if (dl) stats.value = { predicates: dl.predicateCount, rules: dl.ruleCount }
  }, [dl])

  async function evaluate() {
    const prog = program.value.trim()
    if (!prog) return
    running.value = true
    result.value = null
    const started = performance.now()
    try {
      const parsed = parseDatalogProgram(prog)
      for (const fact of parsed.asserts) {
        await api.query(`SELECT DATALOG_ASSERT(${sqlStr(fact)})`, conn.id)
      }
      for (const rule of parsed.rules) {
        await api.query(`SELECT DATALOG_RULE(${sqlStr(rule)})`, conn.id)
      }
      const tuples: string[][] = []
      for (const q of parsed.queries) {
        const r = await api.query(`SELECT DATALOG_QUERY(${sqlStr(q)})`, conn.id)
        if (r.error) {
          result.value = r
          return
        }
        const cell = r.rows.length > 0 ? r.rows[0][0] : null
        tuples.push(...parseQueryTuples(cell))
      }
      const res = tuplesToResult(tuples)
      res.duration = Math.round(performance.now() - started)
      result.value = res
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      running.value = false
    }
  }

  return (
    <div class={s.layout}>
      <div class={s.header}>
        <span class={s.title}>Datalog</span>
        {stats.value && (
          <div class={s.pills}>
            <span class={s.pill}>{stats.value.predicates} predicates</span>
            <span class={s.pill}>{stats.value.rules} rules</span>
          </div>
        )}
      </div>

      <div class={s.editorSection}>
        <div class={s.editorToolbar}>
          <div class={s.examples}>
            {EXAMPLE_PROGRAMS.map((p, i) => (
              <button key={i} class={s.exampleBtn} onClick={() => { program.value = p }}>
                eg{i + 1}
              </button>
            ))}
          </div>
          <span class={s.hint}>⌘↵ to evaluate</span>
          <button class={s.evalBtn} onClick={evaluate} disabled={running.value}>
            {running.value ? 'Evaluating…' : '▶ Evaluate'}
          </button>
        </div>
        <textarea
          class={s.editor}
          value={program.value}
          onInput={e => { program.value = (e.target as HTMLTextAreaElement).value }}
          spellcheck={false}
          onKeyDown={e => {
            if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { e.preventDefault(); evaluate() }
          }}
        />
      </div>

      <div class={s.results}>
        {result.value ? (
          result.value.error ? (
            <div class={s.error}>{result.value.error}</div>
          ) : (
            <>
              <div class={s.resultMeta}>{result.value.rowCount} tuples · {result.value.duration}ms</div>
              <div class={s.grid}><DataGrid result={result.value} /></div>
            </>
          )
        ) : !running.value && (
          <div class={s.hint2}>
            Write facts and rules, then query with <code>?-</code>
          </div>
        )}
      </div>
    </div>
  )
}
